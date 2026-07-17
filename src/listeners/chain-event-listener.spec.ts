import type { ListenerDefinition } from '../config/listener.definition';
import {
  BlockUnavailableError,
  type BlockScanner,
  type MatchedEvent,
} from '../subtensor/block-scanner.service';
import type { ChainConnectionService } from '../subtensor/chain-connection.service';
import type { WebhookNotifier } from '../notifications/webhook.notifier';
import {
  ChainEventListener,
  dedupKey,
  specVersionChange,
} from './chain-event-listener';

const base: MatchedEvent = {
  pallet: 'system',
  event: 'CodeUpdated',
  data: '[]',
  blockNumber: 100,
  blockHash: '0xabc',
  timestampMs: 0,
  eventIndex: 3,
  specVersionFrom: null,
  specVersionTo: null,
};

describe('dedupKey', () => {
  it('is unique per network/event/arguments', () => {
    expect(dedupKey('Finney', base)).toBe('Finney|system.CodeUpdated|[]');
  });

  it('ignores block number and position (a reorg must not re-alert)', () => {
    expect(
      dedupKey('Finney', { ...base, blockNumber: 101, eventIndex: 7 }),
    ).toBe(dedupKey('Finney', base));
  });

  it('differs when the event arguments differ', () => {
    expect(dedupKey('Finney', { ...base, data: '["0xdead"]' })).not.toBe(
      dedupKey('Finney', base),
    );
  });
});

describe('specVersionChange', () => {
  it('renders an upgrade transition', () => {
    expect(
      specVersionChange({ ...base, specVersionFrom: 180, specVersionTo: 181 }),
    ).toBe('180 → 181');
  });

  it('marks an unchanged version', () => {
    expect(
      specVersionChange({ ...base, specVersionFrom: 181, specVersionTo: 181 }),
    ).toBe('unchanged (181)');
  });

  it('falls back to the target version when the parent is unknown', () => {
    expect(
      specVersionChange({ ...base, specVersionFrom: null, specVersionTo: 181 }),
    ).toBe('181');
  });

  it('reports unknown when neither version could be read', () => {
    expect(specVersionChange(base)).toBe('unknown');
  });
});

/**
 * Regression tests for the 2026-07-16 mainnet incident: the `system.CodeUpdated`
 * alert for spec 424 → 432 (block 8636190) was never delivered.
 *
 * `lite.prod.tao.com` load-balances across nodes whose heads differ by ~16
 * blocks. The runtime upgrade made us recreate the connection, the new socket
 * landed on a node ~25 blocks behind, and the listener kept the old node's head
 * as its drain target — so it asked the lagging node for blocks it didn't have.
 * `chain_getBlockHash` answers those with the zero hash rather than an error,
 * every scan blew up, and the drain loop marked the whole range processed
 * anyway. Block 8636190 was never re-read.
 */
describe('ChainEventListener — lagging node / failed block handling', () => {
  const BACKFILL = 50;
  const UPGRADE_BLOCK = 8636190;
  /** Mirrors SCAN_CONCURRENCY in the listener. */
  const SCAN_CONCURRENCY = 5;

  const def: ListenerDefinition = {
    network: 'Finney Mainnet',
    endpoints: ['wss://lite.prod.tao.com'],
    events: [{ pallet: 'system', event: 'CodeUpdated' }],
    webhookUrl: 'https://example.invalid/hook',
  };

  const codeUpdated = (blockNumber: number): MatchedEvent => ({
    ...base,
    blockNumber,
    blockHash: `0x${blockNumber.toString(16)}`,
  });

  /**
   * A CodeUpdated whose *arguments* differ per block, so the dedup key differs
   * too. Needed whenever a test expects an alert from several nearby blocks —
   * with identical args the 5-block dedup window would (correctly) merge them,
   * and the test would pass for the wrong reason.
   */
  const distinctEvent = (blockNumber: number): MatchedEvent => ({
    ...codeUpdated(blockNumber),
    data: `["0x${blockNumber.toString(16)}"]`,
  });

  /** A fake node: `available` is the highest block it actually has. */
  class FakeScanner {
    head = 0;
    available = 0;
    readonly matches = new Map<number, MatchedEvent[]>();
    readonly scanned: number[] = [];
    /** Blocks that throw a transient error on their next scan only. */
    readonly failOnce = new Set<number>();
    /** Blocks that never scan successfully (e.g. an undecodable block). */
    readonly failAlways = new Set<number>();
    /** Blocks reported as "not on this node" on their next scan only. */
    readonly unavailableOnce = new Set<number>();
    /** Blocks this node never has (e.g. pruned on a warp-synced node). */
    readonly unavailableAlways = new Set<number>();
    /** Hook fired as a scan starts — lets a test interleave a reconnect/stop. */
    onScan?: (n: number) => void;
    /** Overrides the matches for a block based on which api scanned it. */
    matchesByApi?: (api: unknown, n: number) => MatchedEvent[] | undefined;
    /** Holds one block's scan (on one api) open until the test releases it. */
    gate?: { block: number; api: unknown; promise: Promise<void> };

    bestNumber = (): Promise<number> => Promise.resolve(this.head);

    scanBlock = async (api: unknown, n: number): Promise<MatchedEvent[]> => {
      this.scanned.push(n);
      this.onScan?.(n);
      if (this.gate && this.gate.block === n && this.gate.api === api) {
        await this.gate.promise;
      }
      if (this.unavailableAlways.has(n)) throw new BlockUnavailableError(n);
      if (n > this.available) throw new BlockUnavailableError(n);
      if (this.unavailableOnce.has(n)) {
        this.unavailableOnce.delete(n);
        throw new BlockUnavailableError(n);
      }
      if (this.failAlways.has(n)) {
        throw new Error('Unable to decode storage system.events');
      }
      if (this.failOnce.has(n)) {
        this.failOnce.delete(n);
        throw new Error('disconnected: 1000:: Normal Closure');
      }
      return this.matchesByApi?.(api, n) ?? this.matches.get(n) ?? [];
    };
  }

  class FakeConnection {
    private headCb?: (h: { number: { toNumber: () => number } }) => void;
    private readonly handlers = new Set<() => void>();
    /** Makes subscribeNewHeads reject, as a dropped socket really does. */
    failSubscribe = false;
    api = this.newApi();

    private newApi() {
      return {
        isReady: Promise.resolve(),
        rpc: {
          chain: {
            subscribeNewHeads: (
              cb: (h: { number: { toNumber: () => number } }) => void,
            ) => {
              if (this.failSubscribe) {
                return Promise.reject(new Error('disconnected'));
              }
              this.headCb = cb;
              return Promise.resolve(() => undefined);
            },
          },
        },
      };
    }

    getConnection = (): Promise<unknown> => Promise.resolve(this.api);

    onReconnect = (_e: string[], h: () => void): (() => void) => {
      this.handlers.add(h);
      return () => this.handlers.delete(h);
    };

    emitHead(n: number): void {
      this.headCb?.({ number: { toNumber: () => n } });
    }

    /**
     * Models ChainConnectionService recreating the connection on a runtime
     * upgrade: a brand-new ApiPromise, whose socket may land on a different
     * node in the pool than the one we were reading.
     */
    recreateApi(): void {
      this.api = this.newApi();
    }

    fireReconnect(): void {
      for (const h of this.handlers) h();
    }
  }

  let scanner: FakeScanner;
  let conn: FakeConnection;
  let sent: string[];
  /** Attempted deliveries, including ones the webhook rejected. */
  let attempted: string[];
  /** When true the webhook rejects every POST (returns false, as it really does). */
  let webhookDown: boolean;
  let errors: string[];
  let warns: string[];
  let listener: ChainEventListener;

  /** Let the drain loop's promise chain settle. */
  const flush = async () => {
    for (let i = 0; i < 60; i++) await new Promise((r) => setImmediate(r));
  };

  /** Advance the fake node one block and let the listener react. */
  const headTo = async (n: number) => {
    scanner.head = n;
    scanner.available = n;
    conn.emitHead(n);
    await flush();
  };

  beforeEach(() => {
    scanner = new FakeScanner();
    conn = new FakeConnection();
    sent = [];
    attempted = [];
    errors = [];
    warns = [];
    webhookDown = false;
    const notifier = {
      send: (_t: unknown, msg: string) => {
        attempted.push(msg);
        // WebhookNotifier never throws — it returns false on failure.
        if (webhookDown) return Promise.resolve(false);
        sent.push(msg);
        return Promise.resolve(true);
      },
    } as unknown as WebhookNotifier;

    listener = new ChainEventListener(
      def,
      conn as unknown as ChainConnectionService,
      scanner as unknown as BlockScanner,
      notifier,
      BACKFILL,
    );
    jest
      .spyOn(listener['logger'], 'warn')
      .mockImplementation((m: unknown) => void warns.push(String(m)));
    jest
      .spyOn(listener['logger'], 'error')
      .mockImplementation((m: unknown) => void errors.push(String(m)));
    jest.spyOn(listener['logger'], 'log').mockImplementation(() => undefined);
  });

  afterEach(() => listener.stop());

  it('retries a block that failed transiently instead of skipping it forever', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    // Mirrors the incident: the live scan of the upgrade block died when we tore
    // the socket down under it.
    scanner.failOnce.add(UPGRADE_BLOCK);

    await listener.start();
    await flush();

    // The block is not handed off as done on the failure: it stays in the work
    // set and is re-derived, so the drain retries it without waiting for a new
    // head (the work set is recomputed while the pass keeps making progress).
    expect(scanner.scanned.filter((n) => n === UPGRADE_BLOCK).length).toBe(2);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('CodeUpdated');
  });

  it('does not re-deliver a block already handled earlier in the same window', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    // One later block fails forever, forcing repeated drain passes over a window
    // that already contains the delivered upgrade block.
    scanner.failAlways.add(UPGRADE_BLOCK - 1);

    await listener.start();
    await flush();
    for (let n = UPGRADE_BLOCK + 1; n <= UPGRADE_BLOCK + 4; n++)
      await headTo(n);

    // Handled blocks are in `done`, so they are never re-scanned or re-sent.
    expect(sent).toHaveLength(1);
    expect(scanner.scanned.filter((n) => n === UPGRADE_BLOCK).length).toBe(1);
  });

  it('never asks a lagging node for blocks above its head, and alerts once it catches up', async () => {
    // Start on the node the LB gave us: 25 blocks behind, upgrade block ahead.
    const lagging = UPGRADE_BLOCK - 25;
    scanner.head = lagging;
    scanner.available = lagging;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);

    await listener.start();
    await flush();
    expect(scanner.scanned.length).toBeGreaterThan(0); // anti-vacuity
    expect(scanner.scanned.filter((n) => n > lagging)).toEqual([]);
    expect(sent).toHaveLength(0);

    // The node catches up block by block.
    for (let n = lagging + 1; n <= UPGRADE_BLOCK; n++) await headTo(n);

    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('CodeUpdated');
  });

  it('does not re-alert when an upgrade recreate re-scans an already-delivered block', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);

    await listener.start();
    await flush();
    expect(sent).toHaveLength(1);

    // recreateApi() is what makes this a re-scan: without it the same-api branch
    // is taken, nothing is re-read, and dedup is never exercised at all.
    scanner.scanned.length = 0;
    conn.recreateApi();
    conn.fireReconnect();
    await flush();

    expect(scanner.scanned).toContain(UPGRADE_BLOCK); // it really was re-scanned
    expect(sent).toHaveLength(1); // ...and dedup suppressed the repeat
  });

  it('keeps alerting on later blocks while one block stays unreadable', async () => {
    const STUCK = UPGRADE_BLOCK;
    scanner.head = STUCK;
    scanner.available = STUCK;
    scanner.failAlways.add(STUCK);
    await listener.start();
    await flush();

    for (let n = STUCK + 1; n <= STUCK + 6; n++) {
      scanner.matches.set(n, [distinctEvent(n)]);
      await headTo(n);
    }

    // Previously the cursor stalled behind STUCK and blacked out everything.
    expect(sent).toHaveLength(6);
    // ...and the stuck block is still retried, not forgotten.
    expect(scanner.scanned.filter((n) => n === STUCK).length).toBeGreaterThan(
      1,
    );
  });

  it('re-reads the upgrade block when the recreate fires mid-scan', async () => {
    // The production timing: the upgrade IS the block being scanned when
    // ChainConnectionService recreates the connection under us.
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    scanner.onScan = (n) => {
      if (n !== UPGRADE_BLOCK) return;
      scanner.onScan = undefined; // once
      conn.recreateApi();
      conn.fireReconnect();
    };

    await listener.start();
    await flush();

    // The scan that ran on the doomed connection must not count as handled.
    expect(
      scanner.scanned.filter((n) => n === UPGRADE_BLOCK).length,
    ).toBeGreaterThan(1);
    expect(sent).toHaveLength(1);
  });

  it('does not lose the blocks after a batch that hit an unavailable block', async () => {
    // The regression from the previous attempt: the batch short-circuit dropped
    // every block after the failing batch. Node has everything except a hole
    // starting mid-window, and the upgrade block sits beyond it.
    const HOLE = UPGRADE_BLOCK - 10;
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    // Make exactly one block report "not on this node yet", once.
    scanner.unavailableOnce.add(HOLE);

    await listener.start();
    await flush();
    // Next head re-derives the work set; nothing may have been skipped.
    await headTo(UPGRADE_BLOCK + 1);

    expect(scanner.scanned).toContain(HOLE);
    expect(scanner.scanned).toContain(UPGRADE_BLOCK);
    expect(sent).toHaveLength(1); // the alert beyond the hole still went out
  });

  it('retries the alert when the webhook is briefly down', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    webhookDown = true;

    await listener.start();
    await flush();
    expect(attempted.length).toBeGreaterThan(0); // anti-vacuity: it did try
    expect(sent).toHaveLength(0);

    // Webhook recovers; the block must be retried and the alert delivered.
    webhookDown = false;
    await headTo(UPGRADE_BLOCK + 1);

    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('CodeUpdated');
  });

  it('logs loudly when a block leaves the window unread', async () => {
    const STUCK = UPGRADE_BLOCK;
    scanner.head = STUCK;
    scanner.available = STUCK;
    scanner.failAlways.add(STUCK);
    await listener.start();
    await flush();
    expect(errors.some((e) => e.includes(`Gave up on block ${STUCK}`))).toBe(
      false,
    );

    // Walk the head past the backfill window so STUCK drops out of it.
    for (let n = STUCK + 1; n <= STUCK + BACKFILL + 1; n++) await headTo(n);

    expect(errors.some((e) => e.includes(`Gave up on block ${STUCK}`))).toBe(
      true,
    );
    expect(errors.some((e) => e.includes('NOT alerted'))).toBe(true);
  });

  it('stops scanning and delivering once stopped, even mid-drain', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    // Every block in the backfill window carries a distinct alert.
    for (let n = UPGRADE_BLOCK - BACKFILL + 1; n <= UPGRADE_BLOCK; n++) {
      scanner.matches.set(n, [distinctEvent(n)]);
    }
    scanner.onScan = () => {
      scanner.onScan = undefined;
      listener.stop();
    };

    await listener.start();
    await flush();

    // Without the stopped guards this delivers dozens of webhooks and scans the
    // whole window against a connection being torn down.
    expect(sent.length).toBeLessThanOrEqual(1);
    expect(scanner.scanned.length).toBeLessThanOrEqual(SCAN_CONCURRENCY);
  });

  it('keeps the reconnect handler when subscribing to heads fails at startup', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    conn.failSubscribe = true;

    await expect(listener.start()).rejects.toThrow();
    // Let the drain start() kicked off before subscribing run itself out, so
    // the assertion below can only be satisfied by the reconnect path.
    await flush();
    expect(sent).toHaveLength(0);

    // The listener must still be revivable: a later reconnect has to reach it.
    conn.failSubscribe = false;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    conn.recreateApi();
    conn.fireReconnect();
    await flush();

    expect(sent).toHaveLength(1);
  });

  it('revives the head feed on a plain (same-api) reconnect after a failed startup subscribe', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    conn.failSubscribe = true;
    await expect(listener.start()).rejects.toThrow();
    await flush();

    // A plain socket reconnect — SAME api, no recreate. This is the common case;
    // it must re-subscribe, otherwise the head feed never comes back.
    conn.failSubscribe = false;
    conn.fireReconnect();
    await flush();

    // The proof the feed is live: a NEW head now flows through on its own. If the
    // same-api branch had not re-subscribed, emitHead would reach no callback.
    scanner.matches.set(UPGRADE_BLOCK + 1, [codeUpdated(UPGRADE_BLOCK + 1)]);
    await headTo(UPGRADE_BLOCK + 1);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain(`#${UPGRADE_BLOCK + 1}`);
  });

  it('does not leak or re-scan aged blocks when the head dips onto a lagging node', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    await listener.start();
    await flush();
    const doneSet = listener['done'];
    expect(doneSet.size).toBe(BACKFILL); // anti-vacuity: the window is populated
    const scannedBefore = scanner.scanned.length;

    // Reconnect pins a node 16 blocks behind that still HAS its old blocks.
    scanner.head = UPGRADE_BLOCK - 16;
    scanner.available = UPGRADE_BLOCK - 16;
    conn.fireReconnect();
    await flush();

    // Nothing below the floor is re-scanned, and `done` never exceeds the window.
    expect(scanner.scanned.length).toBe(scannedBefore);
    expect(doneSet.size).toBeLessThanOrEqual(BACKFILL);

    // Repeated dips must not accumulate — the classic leak.
    for (let i = 0; i < 5; i++) {
      scanner.head = UPGRADE_BLOCK - 16;
      scanner.available = UPGRADE_BLOCK - 16;
      conn.fireReconnect();
      await flush();
    }
    expect(doneSet.size).toBeLessThanOrEqual(BACKFILL);
  });

  it('logs a bounded summary, not per-block, when the head jumps far forward', async () => {
    // Stuck on a stale node, then the LB moves us onto a healthy one millions
    // of blocks ahead.
    scanner.head = 1000;
    scanner.available = 1000;
    await listener.start();
    await flush();

    scanner.head = 1000 + 5_000_000;
    scanner.available = scanner.head;
    conn.fireReconnect();
    await flush();

    // Must NOT emit millions of error lines / block the loop.
    expect(errors.length).toBeLessThan(5);
    // A single summary of the skipped span is fine.
    expect(warns.some((w) => w.includes('Skipping blocks'))).toBe(true);
  });

  it('keeps alerting on recent blocks a warp-synced node has, despite an unavailable old block', async () => {
    // The node has the head and recent blocks but lacks an OLD one (pruned).
    const OLD = UPGRADE_BLOCK - 40;
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.unavailableAlways.add(OLD); // oldest-first, so this is in the first batch
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);

    await listener.start();
    await flush();

    // The recent block's alert must go out even though an older batch member
    // was unavailable — no full-window blackout.
    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('CodeUpdated');
  });

  it('does not fire two webhooks for the same event in one concurrent batch', async () => {
    // Two adjacent blocks in the last batch carry the SAME event (identical
    // dedup key). Scanned concurrently, they must collapse to one alert.
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);
    scanner.matches.set(UPGRADE_BLOCK - 1, [codeUpdated(UPGRADE_BLOCK - 1)]);

    await listener.start();
    await flush();

    expect(attempted.length).toBeGreaterThan(0); // anti-vacuity: it did try to send
    expect(sent).toHaveLength(1);
  });

  it('delivers the clean re-scan, not the stale decode, when a recreate lands mid-scan', async () => {
    // The upgrade block is scanned on the doomed (downgraded) connection when
    // the recreate fires; that decode must never reach the webhook.
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    const staleApi = conn.api;
    let release!: () => void;
    scanner.gate = {
      block: UPGRADE_BLOCK,
      api: staleApi,
      promise: new Promise<void>((r) => (release = r)),
    };
    // The downgraded connection mis-decodes the spec versions (nulls → "unknown");
    // the clean one reads the real 424 → 432. The message renders this, so it
    // tells us WHICH decode reached the webhook.
    scanner.matchesByApi = (api, n) => {
      if (n !== UPGRADE_BLOCK) return undefined;
      return api === staleApi
        ? [{ ...codeUpdated(n), specVersionFrom: null, specVersionTo: null }]
        : [{ ...codeUpdated(n), specVersionFrom: 424, specVersionTo: 432 }];
    };

    const startP = listener.start();
    await flush(); // the stale scan is now gated open on the old api

    // Recreate under it (upgrade path): new clean connection.
    conn.recreateApi();
    conn.fireReconnect();
    await flush();

    release(); // let the stale decode finally resolve
    await startP;
    await flush();

    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('424 → 432'); // the clean decode
    expect(sent[0]).not.toContain('unknown'); // never the downgraded one
  });
});
