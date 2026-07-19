import type { AppConfig } from '../config/app.config';
import type { ListenersConfig } from '../config/listeners.config';
import type { ListenerDefinition } from '../config/listener.definition';
import type { WebhookNotifier } from '../notifications/webhook.notifier';
import type { BlockScanner } from '../subtensor/block-scanner.service';
import type { ChainConnectionService } from '../subtensor/chain-connection.service';
import { ListenersManager } from './listeners.manager';

/**
 * Regression: a listener whose endpoint is unreachable hangs in start()
 * (ApiPromise.create/isReady never resolves for a down RPC — it does NOT
 * reject), so awaiting the starts in onApplicationBootstrap stalled Nest's
 * bootstrap. app.listen() then never bound the HTTP port, the k8s liveness
 * probe on /health/live failed, and the pod restart-looped — taking the
 * healthy listeners down with it. Bootstrap must return promptly regardless.
 */
describe('ListenersManager — bootstrap resilience', () => {
  const def = (network: string): ListenerDefinition => ({
    network,
    endpoints: [`wss://${network}.example`],
    events: [{ pallet: 'system', event: 'CodeUpdated' }],
    webhookUrl: 'https://example.invalid/hook',
  });

  /** A connection whose getConnection never settles — models an unreachable RPC. */
  const hangingConnection = {
    // Never resolves and never rejects: exactly the down-endpoint behaviour.
    getConnection: () => new Promise<never>(() => {}),
    onReconnect: () => () => undefined,
  } as unknown as ChainConnectionService;

  const build = (networks: string[]) => {
    const config = {
      listeners: networks.map(def),
    } as unknown as ListenersConfig;
    const appConfig = { backfillBlocks: 50 } as unknown as AppConfig;
    return new ListenersManager(
      config,
      appConfig,
      hangingConnection,
      {} as unknown as BlockScanner,
      {} as unknown as WebhookNotifier,
    );
  };

  it('returns promptly even when every listener endpoint is unreachable', () => {
    const manager = build(['Finney', 'Testnet', 'Devnet']);
    const spy = jest
      .spyOn(manager['logger'], 'warn')
      .mockImplementation(() => undefined);

    // onApplicationBootstrap is now synchronous: a hung start() must not block
    // it. If it awaited the starts, this call would never return.
    const result = manager.onApplicationBootstrap();

    expect(result).toBeUndefined(); // not a pending Promise the caller must await
    expect(manager.networks()).toEqual(['Finney', 'Testnet', 'Devnet']);
    spy.mockRestore();
  });

  it('warns and does nothing when no listeners are configured', () => {
    const manager = build([]);
    const warn = jest
      .spyOn(manager['logger'], 'warn')
      .mockImplementation(() => undefined);
    manager.onApplicationBootstrap();
    expect(warn).toHaveBeenCalled();
  });
});
