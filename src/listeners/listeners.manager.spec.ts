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
const STALL_SECONDS = 96;

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
    const appConfig = {
      backfillBlocks: 50,
      listenerStallSeconds: STALL_SECONDS,
    } as unknown as AppConfig;
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
    manager.onApplicationShutdown();
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

/**
 * Regression: a WsProvider whose reconnect handshake never completes emits no
 * further open/close event, so it never schedules another attempt — the
 * listener then sat dead for days behind a live process (only a pod restart or
 * a runtime-upgrade recreate ever revived it). The manager must notice the
 * missing heads and rebuild the connection itself, and keep doing so.
 */
describe('ListenersManager — stalled-listener reconnect', () => {
  const stallMs = STALL_SECONDS * 1000;

  const def = (network: string): ListenerDefinition => ({
    network,
    endpoints: [`wss://${network}.example`],
    events: [{ pallet: 'system', event: 'CodeUpdated' }],
    webhookUrl: 'https://example.invalid/hook',
  });

  const build = (networks: string[]) => {
    const reconnect = jest.fn();
    const connection = {
      getConnection: () => new Promise<never>(() => {}),
      onReconnect: () => () => undefined,
      reconnect,
    } as unknown as ChainConnectionService;
    const manager = new ListenersManager(
      { listeners: networks.map(def) } as unknown as ListenersConfig,
      {
        backfillBlocks: 50,
        listenerStallSeconds: STALL_SECONDS,
      } as unknown as AppConfig,
      connection,
      {} as unknown as BlockScanner,
      {} as unknown as WebhookNotifier,
    );
    jest.spyOn(manager['logger'], 'warn').mockImplementation(() => undefined);
    return { manager, reconnect };
  };

  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  it('rebuilds the connection of a listener that stops seeing heads, and retries', () => {
    const { manager, reconnect } = build(['Finney', 'Testnet']);
    manager.onApplicationBootstrap();

    jest.advanceTimersByTime(stallMs + 1);
    expect(reconnect.mock.calls).toEqual([
      [['wss://Finney.example']],
      [['wss://Testnet.example']],
    ]);

    // Still no heads: the next pass must try again rather than give up.
    jest.advanceTimersByTime(stallMs);
    expect(reconnect).toHaveBeenCalledTimes(4);

    manager.onApplicationShutdown();
    jest.advanceTimersByTime(stallMs * 3);
    expect(reconnect).toHaveBeenCalledTimes(4); // watchdog stopped with the app
  });

  it('leaves a listener alone while heads keep arriving', () => {
    const { manager, reconnect } = build(['Finney', 'Testnet']);
    manager.onApplicationBootstrap();

    const healthy = manager['listeners'].get('Finney')!;
    jest.spyOn(healthy, 'liveness').mockImplementation(() => ({
      network: 'Finney',
      head: 1,
      lastHeadAtMs: Date.now(), // a head every time the watchdog looks
      startedAtMs: 0,
      stopped: false,
    }));

    jest.advanceTimersByTime(stallMs + 1);

    expect(reconnect).toHaveBeenCalledTimes(1);
    expect(reconnect).toHaveBeenCalledWith(['wss://Testnet.example']);

    manager.onApplicationShutdown();
  });
});
