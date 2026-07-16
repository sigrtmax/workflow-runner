import { validateConfig, type RuntimeConfig } from './config';
import { createApp } from './app.module';
import { logEvent } from './logging';

export async function bootstrap(config: RuntimeConfig) {
  const application = await createApp(config);
  try {
    await application.listen(config.PORT, '127.0.0.1');
  } catch (error) {
    await application.close();
    throw error;
  }
  return application;
}

if (require.main === module) {
  void startFromEnvironment();
}

async function startFromEnvironment(): Promise<void> {
  try {
    const application = await bootstrap(validateConfig(process.env));
    let closing = false;
    const close = (): void => {
      if (closing) return;
      closing = true;
      void application.close().then(
        () => {
          process.exitCode = 0;
        },
        () => {
          logEvent.fatal('application.shutdown_failed');
          process.exitCode = 1;
        },
      );
    };
    process.once('SIGTERM', close);
    process.once('SIGINT', close);
  } catch {
    logEvent.fatal('application.startup_failed');
    process.exitCode = 1;
  }
}
