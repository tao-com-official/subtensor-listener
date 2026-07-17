import { Logger } from '@nestjs/common';
import type { ApiPromise } from '@polkadot/api';
import type { ListenerDefinition } from '../config/listener.definition';
import {
  DEFAULT_TEMPLATE,
  renderMessage,
  type TemplateVars,
} from '../notifications/message-template';
import type { WebhookNotifier } from '../notifications/webhook.notifier';
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
  /** Whether the notification was actually delivered (false on dry-run). */
  sent: boolean;
}

export interface ReplayResult {
  network: string;
  blockNumber: number;
  blockHash: string;
  matched: number;
  events: ReplayedEvent[];
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
   * kept (see {@link forgetOutsideWindow}), so it can hold at most
   * `backfillBlocks` entries.
   */
  private readonly done = new Set<number>();

  /**
   * Best block of the **current** socket — not a high-water mark across nodes.
   * A load-balanced endpoint can hand a reconnect a node that is behind the one
   * we were reading, and the work set is derived from this, so blocks that node
   * doesn't have are simply not asked for.
   */
  private head = -1;

  /**
   * Low edge of the window we last covered. Blocks that drop below it without
   * ever landing in {@link done} are the one and only place work is abandoned,
   * and they are logged loudly there.
   */
  private windowFrom = -1;

  /**
   * Bumped whenever the pooled api instance is replaced. A scan that started on
   * the old connection must not record its result — the upgrade-boundary block
   * is typically in flight on the *downgraded* connection at exactly the moment
   * the recreate fires, and its result is precisely what we must not trust.
   */
  private apiGen = 0;

  private draining = false;
  private stopped = false;

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
    const next = await this.api.rpc.chain.subscribeNewHeads((header) => {
      const n = header.number.toNumber();
      // Heartbeat: one line per block so the logs show the service is
      // alive and keeping up, even when nothing matches.
      this.logger.log(`Block #${n} — watching ${this.eventLabel}`);
      this.onHead(n);
    });
    // Tear down the old subscription only once the new one is live, so a
    // failed subscribe can't leave us with no head feed.
    const prev = this.unsubscribe;
    this.unsubscribe = next;
    prev?.();
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
      const head = await this.scanner.bestNumber(api);
      if (this.stopped) return;
      this.onHead(head);
      return;
    }

    // A fresh api (post-upgrade recreation). Resolve everything that can reject
    // BEFORE committing to it: assigning this.api first meant a transient
    // failure here left `api === this.api` true forever after, so every later
    // reattach silently took the same-api branch and the boundary block was
    // never re-read on the clean connection.
    const head = await this.scanner.bestNumber(api);
    if (this.stopped) return;

    const previous = this.api;
    this.api = api;
    this.apiGen++;
    try {
      await this.subscribeHeads();
    } catch (err) {
      this.api = previous; // let the retry see a fresh api again
      throw err;
    }
    if (this.stopped) {
      this.unsubscribe?.();
      this.unsubscribe = undefined;
      return;
    }

    // Re-read the whole window on the clean-metadata connection: anything
    // scanned on the downgraded one may have mis-decoded. Dedup suppresses any
    // alert already delivered, so this is safe to repeat.
    this.done.clear();
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
    const matches = await this.scanner.scanBlock(
      api,
      blockNumber,
      this.def.events,
    );

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
    this.head = head;
    void this.drain();
  }

  /** The window we are responsible for: the last `backfillBlocks` up to `head`. */
  private windowOf(head: number): { from: number; to: number } {
    return { from: Math.max(0, head - this.backfillBlocks + 1), to: head };
  }

  /**
   * Scans every block in the window that isn't handled yet, and keeps going
   * until the window is covered or nothing more can be read right now.
   *
   * The work is **derived** from `head` and {@link done} on every iteration
   * rather than tracked incrementally, so a head that moves (forwards or
   * backwards, e.g. onto a lagging node) or a {@link done} cleared by an
   * upgrade recreate is picked up automatically instead of racing.
   */
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (!this.stopped) {
        const head = this.head;
        this.forgetOutsideWindow(head);

        const work = this.workFor(head);
        if (work.length === 0) break;

        const handled = await this.scanBatches(work);
        if (this.stopped) break;
        // The head moved or the connection was replaced under us — recompute
        // rather than act on a stale view.
        if (this.head !== head) continue;
        // Nothing in the window could be read on this node right now (it is
        // behind, or the blocks genuinely fail). Stop rather than spin; the
        // next head re-enters and retries whatever is still missing.
        if (!handled) break;
      }
    } catch (err) {
      this.logger.error(`Drain error: ${(err as Error).message}`);
    } finally {
      this.draining = false;
    }
  }

  /** Blocks inside the window that aren't handled yet, oldest first. */
  private workFor(head: number): number[] {
    const { from, to } = this.windowOf(head);
    const work: number[] = [];
    for (let n = from; n <= to; n++) if (!this.done.has(n)) work.push(n);
    return work;
  }

  /**
   * Scans `work` in batches of {@link SCAN_CONCURRENCY}, returning whether any
   * block was handled — i.e. whether this pass made progress at all.
   */
  private async scanBatches(work: number[]): Promise<boolean> {
    let handled = false;
    for (let i = 0; i < work.length; i += SCAN_CONCURRENCY) {
      if (this.stopped) return handled;
      const batch = work.slice(i, i + SCAN_CONCURRENCY);
      const gen = this.apiGen;
      const results = await Promise.all(batch.map((n) => this.handleBlock(n)));

      // Results from a connection that has since been replaced say nothing
      // about the current one — most importantly, a block scanned on the
      // metadata-downgraded connection must not count as handled.
      if (gen !== this.apiGen) return handled;

      batch.forEach((n, idx) => {
        if (results[idx] !== BLOCK_OK) return;
        this.done.add(n);
        handled = true;
      });
      // Every block above this node's head is unavailable too, so the rest of
      // the window is a waste of round-trips. Nothing is dropped by stopping:
      // whatever is missing simply stays out of `done` and returns as work.
      if (results.includes(BLOCK_UNAVAILABLE)) return handled;
    }
    return handled;
  }

  /**
   * Drops the window's low edge, logging loudly for any block that leaves it
   * without ever being handled. This is the one and only place work is
   * abandoned — everything else keeps retrying.
   */
  private forgetOutsideWindow(head: number): void {
    const { from } = this.windowOf(head);
    if (this.windowFrom < 0) {
      this.windowFrom = from;
      return;
    }
    for (let n = this.windowFrom; n < from; n++) {
      if (!this.done.delete(n)) {
        this.logger.error(
          `Gave up on block ${n}: never read after ${this.backfillBlocks} blocks. ` +
            `Any ${this.eventLabel} in it was NOT alerted.`,
        );
      }
    }
    this.windowFrom = Math.max(this.windowFrom, from);
  }

  /**
   * Scans one block and delivers any matches. Only reports {@link BLOCK_OK} —
   * i.e. "handled", never to be looked at again — once every alert in it has
   * actually been delivered.
   */
  private async handleBlock(blockNumber: number): Promise<BlockOutcome> {
    try {
      const matches = await this.scanner.scanBlock(
        this.api,
        blockNumber,
        this.def.events,
      );
      for (const match of matches) {
        if (this.stopped) return BLOCK_FAILED;
        const key = dedupKey(this.def.network, match);
        if (this.dedup.isDuplicate(key, blockNumber)) continue;
        this.logger.log(
          `Matched ${match.pallet}.${match.event} in block ${blockNumber}.`,
        );
        const { sent } = await this.handleMatch(match, { send: true });
        // A webhook that is briefly down must not cost us the alert: leave the
        // block unhandled so the next head retries it, and leave the dedup
        // marker unset so the retry isn't suppressed as a duplicate.
        if (!sent) return BLOCK_FAILED;
        this.dedup.remember(key, blockNumber);
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

    let sent = false;
    if (opts.send) {
      sent = await this.notifier.send(
        { url: this.def.webhookUrl, field: this.def.webhookField },
        message,
      );
    }

    return {
      pallet: match.pallet,
      event: match.event,
      blockNumber: match.blockNumber,
      specVersionChange: vars.specVersionChange,
      timestamp: vars.timestamp,
      message,
      sent,
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
