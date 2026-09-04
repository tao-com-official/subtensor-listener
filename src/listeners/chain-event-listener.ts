import { Logger } from '@nestjs/common';
import type { ApiPromise } from '@polkadot/api';
import type { ListenerDefinition } from '../config/listener.definition';
import {
  DEFAULT_TEMPLATE,
  renderMessage,
  type TemplateVars,
} from '../notifications/message-template';
import type {
  DeliveryStatus,
  WebhookNotifier,
} from '../notifications/webhook.notifier';
import {
  BlockScanner,
  BlockUnavailableError,
  type MatchedEvent,
} from '../subtensor/block-scanner.service';
import type { ChainConnectionService } from '../subtensor/chain-connection.service';
import { DedupCache } from './dedup-cache';

/** Per-event summary returned by a replay (test) run. */
export interface ReplayedEvent {
  pallet: string;
  event: string;
  blockNumber: number;
  specVersionChange: string;
  timestamp: string;
  message: string;
  /**
   * Whether the notification was actually delivered (false on dry-run).
   * Derivable from {@link delivery}, kept as its own field because it is part
   * of the published `/test/replay` response shape.
   */
  sent: boolean;
  /**
   * How the delivery attempt ended; absent when none was made (dry-run). Tells
   * a replay operator whether a `sent: false` means "safe to replay again"
   * (`rejected`) or "it may already be in the channel" (`unknown`).
   */
  delivery?: DeliveryStatus;
}

export interface ReplayResult {
  network: string;
  blockNumber: number;
  blockHash: string;
  matched: number;
  events: ReplayedEvent[];
}

/** Progress snapshot for the health check. */
export interface ListenerLiveness {
  network: string;
  /** Best head this listener has observed (-1 before the first). */
  head: number;
  /** Wall-clock ms of the last head observed, or null if none yet. */
  lastHeadAtMs: number | null;
  /** Wall-clock ms when start() began, or null if never started. */
  startedAtMs: number | null;
  stopped: boolean;
}

/**
 * How long this listener has gone without observing a head. Measured from the
 * last head or, before the first one arrives, from when the listener started —
 * so a slow initial connect gets the same grace window rather than reading as
 * an instant stall. The readiness check and the reconnect watchdog share this
 * measurement so they can never disagree about how idle a listener is.
 */
export function idleMsOf(s: ListenerLiveness, nowMs: number): number {
  return Math.max(0, nowMs - (s.lastHeadAtMs ?? s.startedAtMs ?? nowMs));
}

/** How many blocks to scan concurrently while backfilling a range. */
const SCAN_CONCURRENCY = 5;

/** Result of scanning one block. */
type BlockOutcome = 'ok' | 'failed' | 'unavailable';
const BLOCK_OK: BlockOutcome = 'ok';
const BLOCK_FAILED: BlockOutcome = 'failed';
/** The node doesn't have the block yet — expected while it catches up. */
const BLOCK_UNAVAILABLE: BlockOutcome = 'unavailable';

/** Backoff before retrying a reattach that failed transiently. */
const REATTACH_RETRY_MS = 2500;

/**
 * Drives one listener definition: backfills a recent window on start, then
 * follows new (best) heads so alerts fire as soon as an event lands in a
 * block, without waiting for finalization. Backfill, live, and post-reconnect
 * gap-fill all go through the same drain loop, capped at the backfill window
 * so a long outage never triggers an unbounded catch-up.
 */
export class ChainEventListener {
  private readonly logger: Logger;
  private readonly eventLabel: string;
  private readonly dedup = new DedupCache();
  private api!: ApiPromise;
  private unsubscribe?: () => void;
  private detachReconnect?: () => void;

  /**
   * Blocks that are **fully handled** — scanned on a live connection, and any
   * matching alert actually delivered. This is the single source of truth for
   * "what is done".
   *
   * There is deliberately no cursor. A cursor conflates two facts that come
   * apart the moment a block fails to read — "everything up to N is handled"
   * and "we have reached N" — and every way of resolving that conflict is a
   * bug: advance it and the block is dropped, hold it back and every block
   * behind it stalls. A set of handled blocks keeps the two apart, so neither
   * failure mode exists to choose between.
   *
   * Bounded by construction: only blocks inside the current window are ever
   * kept (pruned to {@link floor} in {@link advanceFloor}), so it can hold at
   * most `backfillBlocks` entries.
   *
   * Deliberately NOT cleared on a runtime-upgrade recreate. A block that failed
   * to decode on the downgraded connection throws (see ChainConnectionService),
   * so it never entered `done` in the first place and is re-scanned on the clean
   * connection as ordinary work. Only blocks that decoded and delivered
   * correctly are here, and clearing them would both re-do delivered work and
   * erase the record that distinguishes a delivered block from a genuine miss.
   */
  private readonly done = new Set<number>();

  /**
   * Best block of the **current** socket — not a high-water mark across nodes.
   * A load-balanced endpoint can hand a reconnect a node that is behind the one
   * we were reading, and the work set is derived from this, so blocks that node
   * doesn't have are simply not asked for.
   */
  private head = -1;

  /** Highest head ever seen. Monotonic; drives {@link floor} and the miss log. */
  private frontier = -1;

  /**
   * Low edge of everything we still care about: `frontier - backfillBlocks + 1`.
   * **Monotonic**. {@link done} is pruned to it and work is floored at it, so a
   * head that dips onto a lagging node can never make us re-scan (or re-alert)
   * blocks that already aged out, and `done` can't grow past the window.
   */
  private floor = -1;

  /**
   * Bumped whenever the pooled api instance is replaced. A scan that started on
   * the old connection must not record its result — the upgrade-boundary block
   * is typically in flight on the *downgraded* connection at exactly the moment
   * the recreate fires, and its result is precisely what we must not trust.
   */
  private apiGen = 0;

  private draining = false;
  private stopped = false;

  /** When start() began, and when the last head was observed (for liveness). */
  private startedAtMs: number | null = null;
  private lastHeadAtMs: number | null = null;

  /** Serializes reattach() so racing reconnect signals can't leak head subs. */
  private reattaching = false;
  private reattachQueued = false;
  /** Pending reattach retry after a transient failure (cleared on stop). */
  private reattachRetry?: ReturnType<typeof setTimeout>;

  constructor(
    private readonly def: ListenerDefinition,
    private readonly connection: ChainConnectionService,
    private readonly scanner: BlockScanner,
    private readonly notifier: WebhookNotifier,
    private readonly backfillBlocks: number,
  ) {
    this.logger = new Logger(`Listener:${def.network}`);
    this.eventLabel = def.events
      .map((e) => `${e.pallet}.${e.event}`)
      .join(', ');
  }

  async start(): Promise<void> {
    this.startedAtMs = Date.now();
    // Registered FIRST, before anything that can reject. If subscribing to
    // heads throws, this handler is what revives the listener on the next
    // reconnect; registering it afterwards left a failed start permanently
    // dead — no head feed, no reconnect handler — behind a green readiness
    // probe, since the probe only watches the RPC connection.
    this.detachReconnect = this.connection.onReconnect(
      this.def.endpoints,
      () => void this.reattach(),
    );
    if (this.stopped) return this.teardown();

    this.api = await this.connection.getConnection(this.def.endpoints);
    if (this.stopped) return this.teardown();
    await this.api.isReady;
    if (this.stopped) return this.teardown();

    const best = await this.scanner.bestNumber(this.api);
    if (this.stopped) return this.teardown();

    this.logger.log(
      `Backfilling blocks ${Math.max(0, best - this.backfillBlocks + 1)}..${best}, ` +
        `then following new heads.`,
    );
    this.onHead(best);

    await this.subscribeHeads();
    // stop() may have run while start() was awaiting a slow RPC, finding
    // nothing to tear down. Undo our own work rather than leak a head feed.
    if (this.stopped) this.teardown();
  }

  /** Idempotent release of everything start() may have installed. */
  private teardown(): void {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    this.detachReconnect?.();
    this.detachReconnect = undefined;
  }

  /**
   * (Re-)subscribes to new (best) heads on the current {@link api}, replacing
   * any prior subscription. A reorg re-including the event in a nearby block is
   * absorbed by the dedup window (same event + arguments within
   * DEDUP_WINDOW_BLOCKS).
   */
  private async subscribeHeads(): Promise<void> {
    const next = await this.subscribeOn(this.api);
    // Tear down the old subscription only once the new one is live, so a
    // failed subscribe can't leave us with no head feed.
    const prev = this.unsubscribe;
    this.unsubscribe = next;
    prev?.();
  }

  /**
   * Opens a head subscription on a specific api and returns its unsubscribe,
   * committing nothing to `this`. Lets a reattach resolve the fallible subscribe
   * before swapping any listener state, so a rejection leaves the old
   * connection fully intact.
   */
  private subscribeOn(api: ApiPromise): Promise<() => void> {
    return api.rpc.chain.subscribeNewHeads((header) => {
      const n = header.number.toNumber();
      // Heartbeat: one line per block so the logs show the service is
      // alive and keeping up, even when nothing matches.
      this.logger.log(`Block #${n} — watching ${this.eventLabel}`);
      this.onHead(n);
    });
  }

  /**
   * Re-binds to the (possibly recreated) pooled connection. Serialized so
   * overlapping reconnect signals can't leak head subscriptions; a signal that
   * arrives mid-flight is coalesced into one trailing run.
   */
  private async reattach(): Promise<void> {
    if (this.reattaching) {
      this.reattachQueued = true;
      return;
    }
    this.reattaching = true;
    try {
      do {
        this.reattachQueued = false;
        try {
          await this.reattachOnce();
        } catch (err) {
          if (this.stopped) return;
          this.logger.error(
            `Re-attach failed: ${(err as Error).message}; retrying shortly.`,
          );
          this.scheduleReattachRetry();
        }
      } while (this.reattachQueued && !this.stopped);
    } finally {
      this.reattaching = false;
    }
  }

  /**
   * One reattach pass. If the pooled api is unchanged (a plain socket
   * reconnect) the head subscription still flows, so we only gap-fill. If it's
   * a fresh instance (post-upgrade recreation) we rebind: re-subscribe heads
   * and **rewind** so the upgrade-boundary block — which may have failed to
   * decode on the old downgraded connection — is re-scanned on the clean one.
   * Dedup suppresses any alert already delivered for the re-scanned blocks.
   */
  private async reattachOnce(): Promise<void> {
    if (this.stopped) return;
    const api = await this.connection.getConnection(this.def.endpoints);
    if (this.stopped) return;

    // Same api instance = a plain socket reconnect. The socket may now be
    // pinned to a different node in the pool, possibly behind the previous one
    // — which needs no special handling: the work set is derived from this
    // node's head, so blocks it lacks are never asked for.
    if (api === this.api) {
      // ...but if a start-time subscribe failed, we have no head feed at all.
      // This reconnect is our chance to install one; otherwise the listener is
      // blind on a connection that never gets replaced.
      if (!this.unsubscribe) {
        const unsub = await this.subscribeOn(api);
        if (this.stopped) return unsub();
        this.unsubscribe = unsub;
      }
      const head = await this.scanner.bestNumber(api);
      if (this.stopped) return;
      this.onHead(head);
      return;
    }

    // A fresh api (post-upgrade recreation). Resolve EVERYTHING that can reject
    // before touching listener state, then commit in one synchronous step, so a
    // transient failure here leaves the old connection fully intact (rather than
    // half-swapped: api reverted but apiGen bumped).
    const head = await this.scanner.bestNumber(api);
    if (this.stopped) return;
    const unsub = await this.subscribeOn(api);
    if (this.stopped) return unsub();

    // Commit atomically — no awaits between these lines.
    const prevUnsub = this.unsubscribe;
    this.api = api;
    this.apiGen++;
    this.unsubscribe = unsub;
    // `done` is deliberately NOT cleared. Blocks that failed on the downgraded
    // connection threw and never entered `done`, so they are already re-scanned
    // as ordinary work on the clean connection; blocks that are in `done`
    // decoded and delivered correctly and need no re-scan. Clearing it would
    // erase the record that tells a delivered block from a genuine miss (a
    // failed re-scan would then log a false "NOT alerted"), and re-scanning a
    // window whose low blocks got dropped by a far-ahead node's floor jump would
    // lose them silently.
    prevUnsub?.();
    this.onHead(head);
  }

  private scheduleReattachRetry(): void {
    if (this.reattachRetry || this.stopped) return;
    this.reattachRetry = setTimeout(() => {
      this.reattachRetry = undefined;
      if (!this.stopped) void this.reattach();
    }, REATTACH_RETRY_MS);
  }

  stop(): void {
    this.stopped = true;
    if (this.reattachRetry) clearTimeout(this.reattachRetry);
    this.teardown();
  }

  /**
   * Re-runs the live pipeline against a single block (by number or 0x-hash),
   * bypassing dedup. With `dryRun` it renders but does not deliver.
   */
  async replay(
    blockRef: number | string,
    dryRun: boolean,
  ): Promise<ReplayResult> {
    const api = await this.connection.getConnection(this.def.endpoints);
    await api.isReady;

    const blockNumber = await this.resolveBlockNumber(api, blockRef);
    let matches: MatchedEvent[];
    try {
      matches = await this.scanner.scanBlock(api, blockNumber, this.def.events);
    } catch (err) {
      // A load-balanced socket may currently be pinned to a node that lacks this
      // block; give the caller that instead of an opaque 500.
      if (err instanceof BlockUnavailableError) {
        throw new Error(
          `Block ${blockNumber} is not available on the connected node right now; retry.`,
        );
      }
      throw err;
    }

    const events: ReplayedEvent[] = [];
    for (const match of matches) {
      const summary = await this.handleMatch(match, { send: !dryRun });
      events.push(summary);
    }

    return {
      network: this.def.network,
      blockNumber,
      blockHash: matches[0]?.blockHash ?? (await this.hashOf(api, blockNumber)),
      matched: matches.length,
      events,
    };
  }

  /** Records the current socket's best block and kicks the drain. */
  private onHead(head: number): void {
    if (this.stopped || head < 0) return;
    // A fresh head means the feed is alive — the liveness signal the health
    // check reads to tell a keeping-up listener from a stalled one.
    this.lastHeadAtMs = Date.now();
    this.head = head;
    void this.drain();
  }

  /** Progress snapshot for the health check. */
  liveness(): ListenerLiveness {
    return {
      network: this.def.network,
      head: this.head,
      lastHeadAtMs: this.lastHeadAtMs,
      startedAtMs: this.startedAtMs,
      stopped: this.stopped,
    };
  }

  /**
   * Scans every block in the window that isn't handled yet, and keeps going
   * until the window is covered or nothing more can be read right now.
   *
   * The work is **derived** from `head`, {@link floor} and {@link done} on every
   * iteration rather than tracked incrementally, so a head that moves (forwards
   * or backwards, e.g. onto a lagging node) is picked up automatically instead
   * of racing.
   */
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (!this.stopped) {
        const head = this.head;
        const gen = this.apiGen;
        this.advanceFloor(head);

        const work = this.workFor(head);
        if (work.length === 0) break;

        const handled = await this.scanBatches(work, gen);
        if (this.stopped) break;
        // The head moved or the connection was replaced under us — recompute
        // rather than act on a stale view.
        if (this.head !== head || this.apiGen !== gen) continue;
        // Nothing in the window could be read on this node right now (it is
        // behind, or the blocks genuinely fail). Stop rather than spin; the
        // next head re-enters and retries whatever is still missing.
        //
        // Progress is head-driven: if the node keeps its socket open but stops
        // emitting heads entirely, a still-unread block is neither retried nor
        // (until a head finally advances the floor) reported as a miss. That is
        // a connection-liveness failure — the RPC health check owns detecting a
        // silent peer — not something the drain can resolve on its own.
        if (!handled) break;
      }
    } catch (err) {
      this.logger.error(`Drain error: ${(err as Error).message}`);
    } finally {
      this.draining = false;
    }
  }

  /** Blocks in `[floor, head]` that aren't handled yet, oldest first. */
  private workFor(head: number): number[] {
    const work: number[] = [];
    // Floored at `floor`, never at `head - backfillBlocks`: a head that dipped
    // onto a lagging node must not drag the low edge back down and re-scan
    // blocks that already aged out.
    for (let n = this.floor; n <= head; n++) {
      if (!this.done.has(n)) work.push(n);
    }
    return work;
  }

  /**
   * Scans `work` in batches of {@link SCAN_CONCURRENCY}, returning whether any
   * block was handled — i.e. whether this pass made progress at all. `gen` is
   * the apiGen the caller derived the work under; once the connection is
   * replaced we stop issuing *new* batches (the fresh connection's own drain
   * takes over), but results already in hand are still recorded — see below.
   */
  private async scanBatches(work: number[], gen: number): Promise<boolean> {
    let handled = false;
    for (let i = 0; i < work.length; i += SCAN_CONCURRENCY) {
      if (this.stopped || this.apiGen !== gen) return handled;
      const batch = work.slice(i, i + SCAN_CONCURRENCY);
      const results = await Promise.all(batch.map((n) => this.handleBlock(n)));

      // Record every fully-handled block, even if the connection was replaced
      // while this batch was in flight. A BLOCK_OK is trustworthy on its own:
      // handleBlock re-checks the generation between scan and delivery, so a
      // scan on a since-replaced connection returns BLOCK_FAILED, never OK.
      // Discarding OK results on a generation change instead dropped a block
      // whose alert had *already* been delivered (recreate during the send
      // await), which advanceFloor then mis-reported as a missed alert.
      //
      // A block that failed or isn't on this node yet simply stays out of
      // `done`, returns as work next pass, and if it ages out unhandled
      // advanceFloor derives the miss from its absence — no side record needed.
      batch.forEach((n, idx) => {
        if (results[idx] === BLOCK_OK) {
          this.done.add(n);
          handled = true;
        }
      });
      // No short-circuit on an unavailable block: work never runs above this
      // node's head (it is derived from `head`), and availability is not
      // monotone — a warp-synced node can lack an *old* block while serving
      // newer ones, so abandoning the rest of the window here would strand
      // blocks the node actually has.
    }
    return handled;
  }

  /**
   * Advances {@link frontier} and {@link floor}, pruning {@link done} and
   * accounting for every block that leaves the window. This is the one and only
   * place a block is abandoned, and the accounting is **derived from `done`**,
   * not from a side record: a block that was in a window we were responsible for
   * (at or below the *previous* frontier) but is not in `done` is a genuine miss
   * and logged loudly; blocks a head jump flew clean past (above the previous
   * frontier) were never in any window and get one bounded summary line.
   *
   * Deriving the miss from `done` is what makes it robust: it doesn't matter
   * whether the block was scanned-and-failed, never reached (an in-flight batch
   * discarded by a recreate), or anything else — if it aged out unhandled, it is
   * reported. The responsible span is at most `backfillBlocks` wide, so the loud
   * loop is bounded even when the head jumps millions ahead.
   */
  private advanceFloor(head: number): void {
    if (head < 0) return;
    const prevFrontier = this.frontier;
    if (head > this.frontier) this.frontier = head;

    const next = Math.max(0, this.frontier - this.backfillBlocks + 1);
    if (this.floor < 0) {
      this.floor = next; // seed on the first head; nothing below is expected
      return;
    }
    if (next <= this.floor) return; // head dipped or held — floor only rises

    // Blocks leaving the window that we were responsible for (reachable at or
    // below the previous frontier) and never handled: a genuine miss.
    const missTop = Math.min(prevFrontier, next - 1);
    for (let n = this.floor; n <= missTop; n++) {
      if (this.done.has(n)) continue;
      this.logger.error(
        `Gave up on block ${n}: never handled before it left the ` +
          `${this.backfillBlocks}-block window. Any ${this.eventLabel} in it was NOT alerted.`,
      );
    }
    // Blocks above the previous frontier that the head jumped clean over — never
    // in any window; one bounded summary rather than a per-block flood.
    if (prevFrontier + 1 <= next - 1) {
      this.logger.warn(
        `Skipping blocks ${prevFrontier + 1}..${next - 1} ` +
          `(never in the ${this.backfillBlocks}-block window after a head jump).`,
      );
    }
    for (const n of this.done) if (n < next) this.done.delete(n);
    this.floor = next;
  }

  /**
   * Scans one block and delivers any matches. Only reports {@link BLOCK_OK} —
   * i.e. "handled", never to be looked at again — once every alert in it has
   * been delivered, or has failed in a way a retry could only duplicate.
   */
  private async handleBlock(blockNumber: number): Promise<BlockOutcome> {
    const gen = this.apiGen;
    try {
      const matches = await this.scanner.scanBlock(
        this.api,
        blockNumber,
        this.def.events,
      );
      // The connection was replaced while this scan was in flight — the classic
      // case being the upgrade block scanned on the metadata-downgraded api at
      // the moment the recreate fires. Its decode cannot be trusted, so do NOT
      // deliver from it; the clean-connection re-scan will handle the block.
      //
      // This guards the *scan*, which is where the downgrade actually bites: a
      // downgraded connection fails to decode and throws (see
      // ChainConnectionService), landing in the catch below, not here. A recreate
      // that instead fires during the delivery await further down cannot be
      // un-sent; we accept that a decode which was valid at scan time is
      // delivered, rather than re-checking and risking a double-alert on every
      // routine reconnect.
      if (gen !== this.apiGen) return BLOCK_FAILED;
      for (const match of matches) {
        if (this.stopped) return BLOCK_FAILED;
        const key = dedupKey(this.def.network, match);
        // Reserve synchronously (check-and-record in one turn): two blocks in
        // the same concurrent batch carrying the same event must not both fire.
        if (!this.dedup.shouldAlert(key, blockNumber)) continue;
        this.logger.log(
          `Matched ${match.pallet}.${match.event} in block ${blockNumber}.`,
        );
        const { sent, delivery } = await this.handleMatch(match, {
          send: true,
        });
        if (!sent) {
          // A webhook that provably took nothing (refused connection, 4xx, 429)
          // must not cost us the alert: release the reservation and leave the
          // block unhandled so the next head retries it. A retry cannot
          // duplicate a message that was never posted.
          if (delivery === 'rejected') {
            this.dedup.forget(key, blockNumber);
            return BLOCK_FAILED;
          }
          // Ambiguous failure (5xx, read timeout, socket reset): the message may
          // already be in the channel. Releasing the reservation here is what
          // double-alerted on every runtime upgrade — the retry landed within a
          // second and Slack showed both. So keep the reservation and count the
          // block as handled, but say so loudly: this is also the one path where
          // an alert can be silently lost, and /test/replay can recover it.
          this.logger.error(
            `Delivery of ${match.pallet}.${match.event} in block ${blockNumber} ` +
              `may or may not have reached the webhook; not retrying, because a ` +
              `retry would double-alert. If the alert is missing, replay the block.`,
          );
        }
      }
      return BLOCK_OK;
    } catch (err) {
      // A block the node doesn't have yet is routine on a lagging node, not an
      // error — it's retried, not logged at error level.
      if (err instanceof BlockUnavailableError) return BLOCK_UNAVAILABLE;
      if (this.stopped) return BLOCK_FAILED;
      this.logger.error(
        `Failed to process block ${blockNumber}: ${(err as Error).message}`,
      );
      return BLOCK_FAILED;
    }
  }

  /** Builds vars, renders the message, and (optionally) delivers it. */
  private async handleMatch(
    match: MatchedEvent,
    opts: { send: boolean },
  ): Promise<ReplayedEvent> {
    const vars = this.buildVars(match);
    const template = this.def.messageTemplate ?? DEFAULT_TEMPLATE;
    const message = renderMessage(template, vars);

    let delivery: DeliveryStatus | undefined;
    if (opts.send) {
      const result = await this.notifier.send(
        { url: this.def.webhookUrl, field: this.def.webhookField },
        message,
      );
      delivery = result.status;
    }

    return {
      pallet: match.pallet,
      event: match.event,
      blockNumber: match.blockNumber,
      specVersionChange: vars.specVersionChange,
      timestamp: vars.timestamp,
      message,
      sent: delivery === 'delivered',
      delivery,
    };
  }

  private buildVars(match: MatchedEvent): TemplateVars {
    const vars: TemplateVars = {
      network: this.def.network,
      pallet: match.pallet,
      event: match.event,
      blockNumber: String(match.blockNumber),
      blockHash: match.blockHash,
      blockHashShort: shortHash(match.blockHash),
      specVersionFrom: match.specVersionFrom?.toString() ?? '',
      specVersionTo: match.specVersionTo?.toString() ?? '',
      specVersionChange: specVersionChange(match),
      timestamp: formatTimestamp(match.timestampMs),
      timestampUtc: formatTimestampUtc(match.timestampMs),
    };
    // Interpolate the explorer URL template (if configured) against the vars
    // above, so {{explorerUrl}} resolves to a clickable, raw block link.
    vars.explorerUrl = this.def.explorerUrl
      ? renderMessage(this.def.explorerUrl, vars)
      : '';
    return vars;
  }

  private async resolveBlockNumber(
    api: ApiPromise,
    blockRef: number | string,
  ): Promise<number> {
    if (typeof blockRef === 'number') return blockRef;
    const trimmed = blockRef.trim();
    if (/^0x[0-9a-fA-F]+$/.test(trimmed)) {
      const header = await api.rpc.chain.getHeader(trimmed);
      return header.number.toNumber();
    }
    const asNumber = Number(trimmed);
    if (!Number.isInteger(asNumber) || asNumber < 0) {
      throw new Error(`Invalid block reference: ${blockRef}`);
    }
    return asNumber;
  }

  private async hashOf(api: ApiPromise, blockNumber: number): Promise<string> {
    return (await api.rpc.chain.getBlockHash(blockNumber)).toString();
  }
}

/**
 * Dedup key: the event type plus its arguments, per listener. Deliberately
 * excludes the block number and event index — the {@link DedupCache} window
 * handles proximity, so a reorg that moves the event to a nearby block doesn't
 * re-alert, while the same event with different arguments alerts separately.
 */
export function dedupKey(network: string, match: MatchedEvent): string {
  return `${network}|${match.pallet}.${match.event}|${match.data}`;
}

/** Human-readable spec-version transition for the message. */
export function specVersionChange(match: MatchedEvent): string {
  const { specVersionFrom: from, specVersionTo: to } = match;
  if (from !== null && to !== null) {
    return from === to ? `unchanged (${to})` : `${from} → ${to}`;
  }
  if (to !== null) return `${to}`;
  return 'unknown';
}

function formatTimestamp(ms: number | null): string {
  if (ms === null) return 'unknown';
  return new Date(ms).toISOString();
}

const MONTHS = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
];

/** Human-readable UTC time, e.g. "28 May 2026 15:54 UTC". */
function formatTimestampUtc(ms: number | null): string {
  if (ms === null) return 'unknown';
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, '0');
  return (
    `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()} ` +
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())} UTC`
  );
}

/** Abbreviates a long 0x hash to `0x1234abcd…wxyz5678` for compact display. */
function shortHash(hash: string): string {
  return hash.length > 20 ? `${hash.slice(0, 10)}…${hash.slice(-8)}` : hash;
}
