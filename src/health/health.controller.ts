import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { ListenerHealthIndicator } from './listener.health-indicator';
import { RpcHealthIndicator } from './rpc.health-indicator';

/**
 * Health probes (both public — no auth).
 *
 * - `/health/live`  — liveness. Dependency-free: if the process answers, it's
 *   alive. Maps to a k8s livenessProbe (decides whether to RESTART the pod).
 * - `/health/ready` — readiness. RED if any RPC connection is down OR any
 *   listener has stalled (stopped seeing new heads). Maps to a k8s
 *   readinessProbe (takes the pod out of rotation without restarting it) and is
 *   the signal to hang alerting on.
 */
@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly rpc: RpcHealthIndicator,
    private readonly listeners: ListenerHealthIndicator,
  ) {}

  @Get('live')
  live() {
    return { status: 'ok' };
  }

  @Get('ready')
  @HealthCheck()
  ready() {
    return this.health.check([
      () => this.rpc.check('rpc'),
      () => this.listeners.check('listeners'),
    ]);
  }
}
