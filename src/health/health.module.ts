import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { ListenersModule } from '../listeners/listeners.module';
import { SubtensorModule } from '../subtensor/subtensor.module';
import { HealthController } from './health.controller';
import { ListenerHealthIndicator } from './listener.health-indicator';
import { RpcHealthIndicator } from './rpc.health-indicator';

@Module({
  imports: [TerminusModule, SubtensorModule, ListenersModule],
  controllers: [HealthController],
  providers: [RpcHealthIndicator, ListenerHealthIndicator],
})
export class HealthModule {}
