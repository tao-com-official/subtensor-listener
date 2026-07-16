import type { ApiPromise } from '@polkadot/api';
import {
  BlockScanner,
  BlockUnavailableError,
  isMatch,
} from './block-scanner.service';

describe('isMatch', () => {
  const filters = [
    { pallet: 'system', event: 'CodeUpdated' },
    { pallet: 'balances', event: 'Transfer' },
  ];

  it('matches a configured pallet+event', () => {
    expect(isMatch(filters, 'system', 'CodeUpdated')).toBe(true);
    expect(isMatch(filters, 'balances', 'Transfer')).toBe(true);
  });

  it('is case-sensitive and rejects partial matches', () => {
    expect(isMatch(filters, 'System', 'CodeUpdated')).toBe(false);
    expect(isMatch(filters, 'system', 'codeUpdated')).toBe(false);
    expect(isMatch(filters, 'system', 'ExtrinsicSuccess')).toBe(false);
  });
});

describe('BlockScanner.scanBlock', () => {
  const filters = [{ pallet: 'system', event: 'CodeUpdated' }];

  /** Minimal api stub whose getBlockHash returns a codec-like hash. */
  const apiWith = (hash: { isEmpty: boolean; toString: () => string }) => {
    const at = jest.fn().mockResolvedValue({
      query: {
        system: {
          events: () =>
            Promise.resolve(Object.assign([], { forEach: () => {} })),
        },
        timestamp: { now: () => Promise.resolve({ toNumber: () => 0 }) },
      },
    });
    return {
      api: {
        rpc: { chain: { getBlockHash: () => Promise.resolve(hash) } },
        at,
      } as unknown as ApiPromise,
      at,
    };
  };

  /**
   * A node that lags the head we recorded from another node in the pool answers
   * `chain_getBlockHash` with the zero hash instead of erroring. Feeding that to
   * `api.at()` produced the incident's opaque "Unable to retrieve header and
   * parent from supplied hash"; it must surface as a retryable, typed error.
   */
  it('reports a block the node does not have as unavailable, without calling api.at', async () => {
    const { api, at } = apiWith({
      isEmpty: true,
      toString: () => `0x${'00'.repeat(32)}`,
    });

    await expect(
      new BlockScanner().scanBlock(api, 8636190, filters),
    ).rejects.toThrow(BlockUnavailableError);
    expect(at).not.toHaveBeenCalled();
  });

  it('scans normally when the node has the block', async () => {
    const hash =
      '0x9a2350d84bfd0000000000000000000000000000000000000000000000000000';
    const { api, at } = apiWith({ isEmpty: false, toString: () => hash });

    await expect(
      new BlockScanner().scanBlock(api, 8636190, filters),
    ).resolves.toEqual([]);
    expect(at).toHaveBeenCalledWith(hash);
  });
});
