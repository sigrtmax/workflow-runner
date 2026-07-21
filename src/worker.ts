import { NestFactory } from '@nestjs/core';
import { validateConfig, type RuntimeConfig } from './config';
import { ApplicationLogger, logEvent } from './logging';
import { WorkerModule } from './worker/worker.module';

export async function bootstrapWorker(config: RuntimeConfig) {
  const application = await NestFactory.createApplicationContext(
    WorkerModule.register(config),
    { logger: new ApplicationLogger() },
  );
  process.send?.({ type: 'ready' });
  return application;
}

if (require.main === module) {
  void startFromEnvironment();
}

async function startFromEnvironment(): Promise<void> {
  try {
    const application = await bootstrapWorker(validateConfig(process.env));
    let closing = false;
    const close = (): void => {
      if (closing) return;
      closing = true;
      void application.close().then(
        () => {
          process.exitCode = 0;
        },
        () => {
          logEvent.fatal('worker.shutdown_failed');
          process.exitCode = 1;
        },
      );
    };
    process.once('SIGTERM', close);
    process.once('SIGINT', close);
  } catch {
    logEvent.fatal('worker.startup_failed');
    process.exitCode = 1;
  }
}
