import type { ApiPromise } from '@polkadot/api';
import {
  BlockScanner,
  BlockUnavailableError,
  isEmptyHash,
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

describe('isEmptyHash', () => {
  it('recognises the zero hash a node returns for an unknown block', () => {
    expect(isEmptyHash(`0x${'00'.repeat(32)}`)).toBe(true);
    expect(isEmptyHash(`0X${'00'.repeat(32)}`)).toBe(true);
    expect(isEmptyHash('0x0')).toBe(true);
  });

  it('accepts a real block hash', () => {
    expect(
      isEmptyHash(
        '0x9a2350d84bfd0000000000000000000000000000000000000000000000000000',
      ),
    ).toBe(false);
  });
});

describe('BlockScanner.scanBlock', () => {
  /**
   * A node that lags the head we recorded from another node in the pool answers
   * `chain_getBlockHash` with the zero hash instead of erroring. Feeding that to
   * `api.at()` produced the incident's opaque "Unable to retrieve header and
   * parent from supplied hash"; it must surface as a retryable, typed error.
   */
  it('reports a block the node does not have as unavailable, without calling api.at', async () => {
    const at = jest.fn();
    const api = {
      rpc: {
        chain: {
          getBlockHash: () =>
            Promise.resolve({ toString: () => `0x${'00'.repeat(32)}` }),
        },
      },
      at,
    } as unknown as ApiPromise;

    await expect(
      new BlockScanner().scanBlock(api, 8636190, [
        { pallet: 'system', event: 'CodeUpdated' },
      ]),
    ).rejects.toThrow(BlockUnavailableError);
    expect(at).not.toHaveBeenCalled();
  });
});
