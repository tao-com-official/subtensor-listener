import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { AppConfig } from '../config/app.config';
import { ListenersConfig } from '../config/listeners.config';
import { WebhookNotifier } from '../notifications/webhook.notifier';
import { BlockScanner } from '../subtensor/block-scanner.service';
import { ChainConnectionService } from '../subtensor/chain-connection.service';
import {
  ChainEventListener,
  idleMsOf,
  type ListenerLiveness,
  type ReplayResult,
} from './chain-event-listener';

/**
 * Builds one {@link ChainEventListener} per configured definition, starts them
 * on application bootstrap, and routes test replays to the right listener(s).
 */
@Injectable()
export class ListenersManager
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(ListenersManager.name);
  private readonly listeners = new Map<string, ChainEventListener>();
  private watchdog?: ReturnType<typeof setInterval>;

  constructor(
    private readonly config: ListenersConfig,
    private readonly appConfig: AppConfig,
    private readonly connection: ChainConnectionService,
    private readonly scanner: BlockScanner,
    private readonly notifier: WebhookNotifier,
  ) {}

  onApplicationBootstrap(): void {
    const defs = this.config.listeners;
    if (defs.length === 0) {
      this.logger.warn('No listeners configured — nothing to watch.');
      return;
    }
    for (const def of defs) {
      const listener = new ChainEventListener(
        def,
        this.connection,
        this.scanner,
        this.notifier,
        this.appConfig.backfillBlocks,
      );
      this.listeners.set(def.network, listener);
    }
    // Start each listener in the BACKGROUND — deliberately not awaited.
    //
    // start() blocks on the initial RPC connection (ApiPromise.create /
    // isReady), which for an unreachable endpoint never resolves *and never
    // rejects* — so it hangs rather than throwing, and the .catch below can't
    // save us. Awaiting it here would stall Nest's bootstrap, so app.listen()
    // never binds the HTTP port: /health/live becomes unreachable, the k8s
    // liveness probe fails, and the pod restart-loops — taking the healthy
    // listeners down with it every couple of minutes. One dead network must not
    // do that. Firing without awaiting lets the process come up and serve
    // /health immediately; each listener connects (and starts alerting) if and
    // when its endpoint does, retrying underneath via the WsProvider.
    for (const [network, listener] of this.listeners) {
      void listener.start().catch((err) => {
        this.logger.error(
          `Listener "${network}" failed to start: ${(err as Error).message}`,
        );
      });
    }

    // The poll period is the stall threshold itself: a listener always gets a
    // full threshold to produce a head before its connection is touched, and a
    // stall is noticed within two.
    const stallMs = this.appConfig.listenerStallSeconds * 1000;
    this.watchdog = setInterval(() => this.reconnectStalled(stallMs), stallMs);
  }

  onApplicationShutdown(): void {
    if (this.watchdog) clearInterval(this.watchdog);
    this.watchdog = undefined;
    for (const listener of this.listeners.values()) listener.stop();
    this.listeners.clear();
  }

  /**
   * Rebuilds the connection behind every listener that has stopped seeing
   * heads. The WsProvider cannot always do this itself — a reconnect whose
   * handshake never completes leaves the socket silent forever, and the
   * listener then sits dead behind a process that is otherwise healthy (only a
   * pod restart or a runtime-upgrade recreate ever brought it back). Runs on a
   * timer rather than once, so an endpoint that is still down simply gets
   * another attempt on the next pass.
   */
  private reconnectStalled(stallMs: number): void {
    const now = Date.now();
    for (const def of this.config.listeners) {
      const listener = this.listeners.get(def.network);
      if (!listener) continue;
      const state = listener.liveness();
      if (state.stopped) continue;
      const idleMs = idleMsOf(state, now);
      if (idleMs < stallMs) continue;
      this.logger.warn(
        `Listener "${def.network}" has seen no head for ` +
          `${Math.round(idleMs / 1000)}s — reconnecting.`,
      );
      this.connection.reconnect(def.endpoints);
    }
  }

  /** Lists configured listener names (for the test endpoint / diagnostics). */
  networks(): string[] {
    return this.config.listeners.map((l) => l.network);
  }

  /** How many listeners are configured (may exceed the number started yet). */
  expectedCount(): number {
    return this.config.listeners.length;
  }

  /** Progress snapshot per running listener, for the health check. */
  liveness(): ListenerLiveness[] {
    return Array.from(this.listeners.values()).map((l) => l.liveness());
  }

  /**
   * Replays a block through the live pipeline for one network (or all, if
   * `network` is omitted). Used by the secret-guarded test endpoint.
   */
  async replay(
    blockRef: number | string,
    dryRun: boolean,
    network?: string,
  ): Promise<ReplayResult[]> {
    const targets = this.resolveTargets(network);
    return Promise.all(targets.map((l) => l.replay(blockRef, dryRun)));
  }

  private resolveTargets(network?: string): ChainEventListener[] {
    if (this.listeners.size === 0) {
      throw new Error('No listeners are configured.');
    }
    if (!network) return Array.from(this.listeners.values());
    const listener = this.listeners.get(network);
    if (!listener) {
      throw new Error(
        `Unknown network "${network}". Configured: ${this.networks().join(', ')}.`,
      );
    }
    return [listener];
  }
}
