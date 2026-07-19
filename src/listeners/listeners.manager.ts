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
import { ChainEventListener, type ReplayResult } from './chain-event-listener';

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
  }

  onApplicationShutdown(): void {
    for (const listener of this.listeners.values()) listener.stop();
    this.listeners.clear();
  }

  /** Lists configured listener names (for the test endpoint / diagnostics). */
  networks(): string[] {
    return this.config.listeners.map((l) => l.network);
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
