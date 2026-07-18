import { Injectable } from '@nestjs/common';
import {
  HealthIndicatorResult,
  HealthIndicatorService,
} from '@nestjs/terminus';
import { AppConfig } from '../config/app.config';
import { ListenersManager } from '../listeners/listeners.manager';

/**
 * Readiness signal tied to listener *progress*, not just socket connectivity.
 * A listener whose RPC connection is up can still be stuck — a silent peer that
 * stops emitting heads, or a head feed that died without tripping a reconnect.
 * The RPC indicator would stay green through that; this one goes RED once any
 * listener has gone longer than `listenerStallSeconds` without observing a new
 * head, with a per-listener breakdown for diagnostics.
 *
 * "Idle" is measured from the last head OR, before the first head arrives, from
 * when the listener started — so a slow initial connect gets the same grace
 * window rather than flapping red on boot. A stopped listener is never stalled.
 */
@Injectable()
export class ListenerHealthIndicator {
  constructor(
    private readonly healthIndicatorService: HealthIndicatorService,
    private readonly manager: ListenersManager,
    private readonly appConfig: AppConfig,
  ) {}

  check(key: string): HealthIndicatorResult {
    const indicator = this.healthIndicatorService.check(key);
    const expected = this.manager.expectedCount();
    const thresholdSeconds = this.appConfig.listenerStallSeconds;
    const thresholdMs = thresholdSeconds * 1000;
    const now = Date.now();

    const listeners = this.manager.liveness().map((s) => {
      const since = s.lastHeadAtMs ?? s.startedAtMs ?? now;
      const idleMs = Math.max(0, now - since);
      const stalled = !s.stopped && idleMs > thresholdMs;
      return {
        network: s.network,
        head: s.head,
        idleSeconds: Math.round(idleMs / 1000),
        sawHead: s.lastHeadAtMs !== null,
        stalled,
      };
    });

    const details = { expected, thresholdSeconds, listeners };

    // Green when nothing is expected. Otherwise every configured listener must
    // be running (started and present) and none may be stalled.
    const allRunning = listeners.length === expected;
    const anyStalled = listeners.some((l) => l.stalled);
    const healthy = expected === 0 || (allRunning && !anyStalled);

    return healthy ? indicator.up(details) : indicator.down(details);
  }
}
