import { ConsoleLogger, Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfig } from './config/app.config';

async function bootstrap() {
  // One JSON object per line (no ANSI colours) so New Relic parses level,
  // context and message into attributes.
  const app = await NestFactory.create(AppModule, {
    logger: new ConsoleLogger({ json: true }),
  });

  // Strip unknown properties and coerce DTOs across all routes.
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  // Run OnApplicationShutdown hooks on SIGTERM/SIGINT so websocket providers
  // disconnect cleanly and head subscriptions are torn down.
  app.enableShutdownHooks();

  const config = app.get(AppConfig);
  await app.listen(config.port);
  new Logger('Bootstrap').log(
    `subtensor-listener listening on port:${config.port}`,
  );
}

void bootstrap();
