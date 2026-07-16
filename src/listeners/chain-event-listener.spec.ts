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

  /** A fake node: `available` is the highest block it actually has. */
  class FakeScanner {
    head = 0;
    available = 0;
    readonly matches = new Map<number, MatchedEvent[]>();
    readonly scanned: number[] = [];
    /** Blocks that throw a transient error on their next scan only. */
    readonly failOnce = new Set<number>();

    bestNumber = (): Promise<number> => Promise.resolve(this.head);

    scanBlock = (_api: unknown, n: number): Promise<MatchedEvent[]> => {
      this.scanned.push(n);
      if (n > this.available)
        return Promise.reject(new BlockUnavailableError(n));
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

    // A reconnect rewinds over the same window; dedup must suppress the repeat.
    conn.fireReconnect();
    await flush();
    expect(sent).toHaveLength(1);
  });
});
