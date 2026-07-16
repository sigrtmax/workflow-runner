import { Global, Module, Provider, ValidationPipe } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_GUARD, NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import helmet from 'helmet';
import type { Request, Response, NextFunction } from 'express';
import { RuntimeConfig } from './config';
import { Database } from './database';
import { ExecutionsModule } from './executions/executions.module';
import { ExecutionsRepository } from './executions/executions.repository';
import {
  ApiExceptionFilter,
  RawPayloadValidationPipe,
  requestId,
} from './http-boundary';
import { ApplicationLogger } from './logging';
import { Telemetry } from './telemetry';
import { WorkflowsModule } from './workflows/workflows.module';
import { WorkflowsRepository } from './workflows/workflows.repository';

@Global()
@Module({})
class RuntimeModule {
  static register(config: RuntimeConfig) {
    const providers: Provider[] = [
      {
        provide: Database,
        useFactory: () => new Database(config.DATABASE_URL),
      },
      {
        provide: Telemetry,
        useFactory: () =>
          new Telemetry({ endpoint: config.OTEL_EXPORTER_OTLP_ENDPOINT }),
      },
      {
        provide: WorkflowsRepository,
        useFactory: (db: Database) => new WorkflowsRepository(db),
        inject: [Database],
      },
      {
        provide: ExecutionsRepository,
        useFactory: (db: Database) =>
          new ExecutionsRepository(db, {
            leaseMs: config.LEASE_MS,
            concurrency: config.CONCURRENCY,
          }),
        inject: [Database],
      },
    ];
    return {
      module: RuntimeModule,
      providers,
      exports: [Database, Telemetry, WorkflowsRepository, ExecutionsRepository],
    };
  }
}

function rootModule(config: RuntimeConfig) {
  @Module({
    imports: [
      ConfigModule.forRoot({
        isGlobal: true,
        ignoreEnvFile: true,
        skipProcessEnv: true,
        validate: () => config,
      }),
      RuntimeModule.register(config),
      ThrottlerModule.forRoot([{ ttl: 60_000, limit: 120 }]),
      WorkflowsModule,
      ExecutionsModule,
    ],
    providers: [{ provide: APP_GUARD, useClass: ThrottlerGuard }],
  })
  class AppModule {}
  return AppModule;
}

export async function createApp(
  config: RuntimeConfig,
): Promise<NestExpressApplication> {
  const app = await NestFactory.create<NestExpressApplication>(
    rootModule(config),
    { bodyParser: false, logger: new ApplicationLogger() },
  );
  app.set('query parser', 'simple');
  app.disable('x-powered-by');
  app.use(helmet());
  app.use((request: Request, response: Response, next: NextFunction) => {
    response.setHeader('X-Request-Id', requestId(request));
    next();
  });
  app.useBodyParser('json', { limit: '256kb' });
  app.useGlobalPipes(
    new RawPayloadValidationPipe(),
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );
  app.useGlobalFilters(new ApiExceptionFilter());
  await app.init();
  return app;
}
