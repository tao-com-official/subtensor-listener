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

  private lastProcessed = -1;
  private targetHead = -1;
  private draining = false;
  private stopped = false;

  /**
   * Blocks that failed to scan and still owe us a look. Kept **separate from
   * the cursor**: `lastProcessed` always advances, so one unreadable block can
   * never stall alerting for the blocks behind it, while the block itself is
   * still retried on every head until it reads or falls out of the window.
   */
  private readonly pending = new Set<number>();

  /**
   * Bumped whenever {@link reattachOnce} rewinds the cursor. A drain pass that
   * started before the rewind must not write its stale `lastProcessed` back
   * over it — the rewind is what re-reads the upgrade-boundary block on the
   * fresh connection, and the upgrade block is typically the very block being
   * scanned when the recreate fires.
   */
  private generation = 0;

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
    this.api = await this.connection.getConnection(this.def.endpoints);
    if (this.stopped) return;
    await this.api.isReady;
    if (this.stopped) return;

    const best = await this.scanner.bestNumber(this.api);
    if (this.stopped) return;
    // Seed lastProcessed so the first drain covers exactly the backfill window.
    this.lastProcessed = Math.max(-1, best - this.backfillBlocks);
    this.logger.log(
      `Backfilling blocks ${this.lastProcessed + 1}..${best}, then following new heads.`,
    );
    this.bump(best);

    await this.subscribeHeads();

    // stop() may have run while start() was awaiting a slow RPC — it would have
    // found unsubscribe/detachReconnect still unset and torn down nothing. Undo
    // the subscription ourselves rather than leaking a live head feed.
    if (this.stopped) {
      this.unsubscribe?.();
      this.unsubscribe = undefined;
      return;
    }

    // On reconnect *or* connection recreation (after a runtime upgrade), the
    // pooled api may be a brand-new instance — re-acquire it, re-subscribe, and
    // gap-fill. Re-processing the boundary block on the fresh (clean-metadata)
    // connection is what makes the alert survive an upgrade; dedup prevents a
    // double-alert if the live pass already delivered it.
    this.detachReconnect = this.connection.onReconnect(
      this.def.endpoints,
      () => void this.reattach(),
    );
    if (this.stopped) {
      this.detachReconnect();
      this.detachReconnect = undefined;
    }
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
      this.bump(n);
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

    // Same api instance = a plain socket reconnect. The node behind it may
    // still have changed (the provider re-resolves the endpoint), so retarget
    // rather than bump — the new node can be behind the old one's head.
    //
    // With the current drain this is belt-and-braces rather than load-bearing:
    // the cursor always catches up to targetHead, so on a backwards node swap
    // lastProcessed already sits above the new head and the drain simply idles
    // until the node passes it. It is kept because the invariant it states —
    // targetHead is "how far the *current* socket reaches", not a high-water
    // mark across nodes — is what makes a lagging node safe, and a future drain
    // that can leave the cursor behind would depend on it. Deliberately not
    // pinned by a test: with this drain, bump and retarget are indistinguishable
    // here.
    if (api === this.api) {
      const head = await this.scanner.bestNumber(api);
      if (this.stopped) return;
      this.retarget(head);
      return;
    }

    this.api = api;
    const [, head] = await Promise.all([
      this.subscribeHeads(),
      this.scanner.bestNumber(api),
    ]);
    if (this.stopped) {
      this.unsubscribe?.();
      return;
    }
    // Rewind to re-cover the recent window (incl. the upgrade boundary), and
    // mark the state as moved on so a drain pass already in flight can't write
    // its stale cursor back over the rewind.
    this.generation++;
    this.lastProcessed = Math.min(
      this.lastProcessed,
      Math.max(-1, head - this.backfillBlocks),
    );
    this.retarget(head);
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
    this.unsubscribe?.();
    this.detachReconnect?.();
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

  /** Raises the drain target to `head` if it's ahead of what we already know. */
  private bump(head: number): void {
    if (this.stopped) return;
    if (head > this.targetHead) this.targetHead = head;
    void this.drain();
  }

  /**
   * Re-points the drain target at `head`, **lowering** it if necessary. Unlike
   * {@link bump}, this drops the previous high-water mark, because that mark
   * describes the node we *were* talking to. A reconnect re-resolves DNS, and a
   * load-balanced endpoint can hand us a different node in the pool whose head
   * is behind the previous one's. Keeping the old target would make us demand
   * blocks this node simply doesn't have yet — every one of them answered with
   * the zero hash. The blocks aren't lost: this node's own head subscription
   * walks up to them as it catches up.
   */
  private retarget(head: number): void {
    if (this.stopped) return;
    this.targetHead = head;
    void this.drain();
  }

  /**
   * Scans everything between the cursor and the target head, plus any blocks
   * still owed a retry. The cursor **always** advances to the target: a block
   * we couldn't read is parked in {@link pending} rather than holding the
   * cursor back, so one unreadable block never stops alerting for the blocks
   * behind it. Pending blocks are retried on each pass and only given up on
   * once they fall out of the backfill window.
   */
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (!this.stopped && this.lastProcessed < this.targetHead) {
        const to = this.targetHead;
        let from = this.lastProcessed + 1;
        const minFrom = to - this.backfillBlocks + 1;
        if (from < minFrom) {
          this.logger.warn(
            `Skipping blocks ${from}..${minFrom - 1} (beyond ${this.backfillBlocks}-block backfill window).`,
          );
          from = minFrom;
        }
        from = Math.max(from, 0);

        // Retries first: a pending block is older than `from`, and on a lagging
        // node it is the one most likely to have become readable.
        const numbers = [...this.pending]
          .filter((n) => n < from)
          .sort((a, b) => a - b);
        for (let n = from; n <= to; n++) numbers.push(n);

        const gen = this.generation;
        await this.processBlocks(numbers);

        // A reattach rewound the cursor while we were scanning (the upgrade
        // block is usually the very block in flight when the recreate fires).
        // That rewind is deliberate — re-read the boundary on the fresh
        // connection — so leave it alone and start over from the new state.
        if (gen !== this.generation) continue;
        if (this.stopped) return;

        this.lastProcessed = Math.max(this.lastProcessed, to);
        this.expirePending(to);
      }
    } catch (err) {
      this.logger.error(`Drain error: ${(err as Error).message}`);
    } finally {
      this.draining = false;
    }
  }

  /**
   * Scans `numbers` in batches of {@link SCAN_CONCURRENCY}. Blocks that fail
   * join {@link pending}; blocks that succeed leave it.
   */
  private async processBlocks(numbers: number[]): Promise<void> {
    for (let i = 0; i < numbers.length; i += SCAN_CONCURRENCY) {
      if (this.stopped) return;
      const batch = numbers.slice(i, i + SCAN_CONCURRENCY);
      const results = await Promise.all(batch.map((n) => this.processBlock(n)));
      batch.forEach((n, idx) => {
        if (results[idx] === BLOCK_OK) this.pending.delete(n);
        else this.pending.add(n);
      });
      // Every block above a node's head is unavailable too, so once one comes
      // back unavailable there is nothing to gain from the rest of this pass.
      if (results.includes(BLOCK_UNAVAILABLE)) {
        for (const n of batch.slice(results.indexOf(BLOCK_UNAVAILABLE))) {
          this.pending.add(n);
        }
        return;
      }
    }
  }

  /**
   * Drops pending blocks that have fallen outside the backfill window. This is
   * the one place a block is abandoned for good, so it is logged loudly — the
   * alternative is retrying it forever.
   */
  private expirePending(head: number): void {
    const oldest = head - this.backfillBlocks + 1;
    for (const n of this.pending) {
      if (n >= oldest) continue;
      this.pending.delete(n);
      this.logger.error(
        `Gave up on block ${n}: still unreadable after ${this.backfillBlocks} blocks. ` +
          `Any ${this.eventLabel} in it was NOT alerted.`,
      );
    }
  }

  /**
   * Scans one block and delivers any matches. Reports whether the block is
   * done, still owed a retry, or simply not on this node yet.
   */
  private async processBlock(blockNumber: number): Promise<BlockOutcome> {
    try {
      const matches = await this.scanner.scanBlock(
        this.api,
        blockNumber,
        this.def.events,
      );
      for (const match of matches) {
        if (this.stopped) return BLOCK_FAILED;
        const key = dedupKey(this.def.network, match);
        if (!this.dedup.shouldAlert(key, blockNumber)) continue;
        this.logger.log(
          `Matched ${match.pallet}.${match.event} in block ${blockNumber}.`,
        );
        await this.handleMatch(match, { send: true });
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
