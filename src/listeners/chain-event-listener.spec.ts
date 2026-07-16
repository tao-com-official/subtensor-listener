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
    /** Hook fired as a scan starts — lets a test interleave a reconnect/stop. */
    onScan?: (n: number) => void;

    bestNumber = (): Promise<number> => Promise.resolve(this.head);

    scanBlock = (_api: unknown, n: number): Promise<MatchedEvent[]> => {
      this.scanned.push(n);
      this.onScan?.(n);
      if (n > this.available)
        return Promise.reject(new BlockUnavailableError(n));
      if (this.failAlways.has(n)) {
        return Promise.reject(
          new Error('Unable to decode storage system.events'),
        );
      }
      if (this.failOnce.has(n)) {
        this.failOnce.delete(n);
        return Promise.reject(new Error('disconnected: 1000:: Normal Closure'));
      }
      return Promise.resolve(this.matches.get(n) ?? []);
    };
  }

  class FakeConnection {
    private headCb?: (h: { number: { toNumber: () => number } }) => void;
    private readonly handlers = new Set<() => void>();
    api = this.newApi();

    private newApi() {
      return {
        isReady: Promise.resolve(),
        rpc: {
          chain: {
            subscribeNewHeads: (
              cb: (h: { number: { toNumber: () => number } }) => void,
            ) => {
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
  let listener: ChainEventListener;

  /** Let the drain loop's promise chain settle. */
  const flush = async () => {
    for (let i = 0; i < 50; i++) await new Promise((r) => setImmediate(r));
  };

  beforeEach(() => {
    scanner = new FakeScanner();
    conn = new FakeConnection();
    sent = [];
    const notifier = {
      send: (_t: unknown, msg: string) => {
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
    jest.spyOn(listener['logger'], 'warn').mockImplementation(() => undefined);
    jest.spyOn(listener['logger'], 'error').mockImplementation(() => undefined);
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

    // The failed block must NOT be treated as delivered.
    expect(sent).toHaveLength(0);

    // The next head retries it — this is what the old code never did.
    scanner.head = UPGRADE_BLOCK + 1;
    scanner.available = UPGRADE_BLOCK + 1;
    conn.emitHead(UPGRADE_BLOCK + 1);
    await flush();

    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('CodeUpdated');
  });

  it('does not demand blocks the node lacks when an upgrade recreate lands on a lagging node', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    await listener.start();
    await flush();

    // The upgrade recreates the connection and the fresh socket lands on the
    // pool's other node, 25 blocks behind. The rewind re-covers the window on
    // that node — and must stop at ITS head, not the previous node's.
    const lagging = UPGRADE_BLOCK - 25;
    scanner.scanned.length = 0;
    scanner.head = lagging;
    scanner.available = lagging;
    conn.recreateApi();
    conn.fireReconnect();
    await flush();

    // The rewind really ran (otherwise the assertion below passes vacuously).
    expect(scanner.scanned.length).toBeGreaterThan(0);
    expect(scanner.scanned.filter((n) => n > lagging)).toEqual([]);
  });

  it('delivers the upgrade alert once the lagging node catches up to the block', async () => {
    // Start on the node that is behind; the upgrade block is ahead of it.
    scanner.head = UPGRADE_BLOCK - 25;
    scanner.available = UPGRADE_BLOCK - 25;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);

    await listener.start();
    await flush();
    expect(sent).toHaveLength(0);

    // The node catches up block by block.
    for (let n = UPGRADE_BLOCK - 24; n <= UPGRADE_BLOCK; n++) {
      scanner.head = n;
      scanner.available = n;
      conn.emitHead(n);
      await flush();
    }

    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('CodeUpdated');
  });

  it('does not re-alert when a rewind re-scans an already-delivered block', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    scanner.matches.set(UPGRADE_BLOCK, [codeUpdated(UPGRADE_BLOCK)]);

    await listener.start();
    await flush();
    expect(sent).toHaveLength(1);

    // recreateApi() is what makes this a *rewind*: without it reattachOnce
    // takes the same-api branch, nothing is re-scanned, and dedup is never
    // exercised at all.
    scanner.scanned.length = 0;
    conn.recreateApi();
    conn.fireReconnect();
    await flush();

    expect(scanner.scanned).toContain(UPGRADE_BLOCK); // the rewind really re-scanned it
    expect(sent).toHaveLength(1); // ...and dedup suppressed the repeat
  });

  it('keeps alerting on later blocks while one block stays unreadable', async () => {
    // The block that never reads — e.g. the documented "Unable to decode
    // storage system.events" after a metadata downgrade.
    const STUCK = UPGRADE_BLOCK;
    scanner.head = STUCK;
    scanner.available = STUCK;
    scanner.failAlways.add(STUCK);
    await listener.start();
    await flush();

    // Later blocks each carry a distinct alertable event.
    for (let n = STUCK + 1; n <= STUCK + 6; n++) {
      scanner.matches.set(n, [distinctEvent(n)]);
      scanner.head = n;
      scanner.available = n;
      conn.emitHead(n);
      await flush();
    }

    // Pre-rework this was a ~10-minute blackout: the cursor stalled at STUCK-1
    // and nothing after it was ever scanned.
    expect(sent).toHaveLength(6);
    // ...and the stuck block is still being retried, not forgotten.
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

    // The rewind must survive the in-flight drain writing its stale cursor back.
    const rescans = scanner.scanned.filter((n) => n === UPGRADE_BLOCK).length;
    expect(rescans).toBeGreaterThan(1);
    expect(sent).toHaveLength(1); // delivered once, dedup covers the re-read
  });

  it('does not scan past a lagging node head on a plain socket reconnect', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    await listener.start();
    await flush();

    // Same ApiPromise instance (no recreate) — but the provider re-resolved DNS
    // and the socket is now pinned to a node 25 blocks behind.
    const lagging = UPGRADE_BLOCK - 25;
    scanner.scanned.length = 0;
    scanner.head = lagging;
    scanner.available = lagging;
    conn.fireReconnect();
    await flush();

    expect(scanner.scanned.filter((n) => n > lagging)).toEqual([]);
  });

  it('stops delivering once stopped, even mid-drain', async () => {
    scanner.head = UPGRADE_BLOCK;
    scanner.available = UPGRADE_BLOCK;
    for (let n = UPGRADE_BLOCK - 40; n <= UPGRADE_BLOCK; n++) {
      scanner.matches.set(n, [distinctEvent(n)]);
    }
    // Stop as soon as the drain touches its first block.
    scanner.onScan = () => {
      scanner.onScan = undefined;
      listener.stop();
    };

    await listener.start();
    await flush();

    // A 41-block backfill would otherwise deliver dozens of webhooks.
    expect(sent.length).toBeLessThanOrEqual(1);
    // ...and the drain must abandon the remaining batches rather than scan all
    // 41 blocks against a connection that is being torn down under it.
    expect(scanner.scanned.length).toBeLessThanOrEqual(5);
  });
});
