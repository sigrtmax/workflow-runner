import { Module, type DynamicModule } from '@nestjs/common';
import { Queue, type ConnectionOptions } from 'bullmq';
import type { RuntimeConfig } from '../config';
import { Database } from '../database';
import { ExecutionsRepository } from '../executions/executions.repository';
import { logEvent } from '../logging';
import { Telemetry } from '../telemetry';
import { Scheduler } from './scheduler';
import { StepWorker, type StepJob } from './step-worker';

const QUEUE_NAME = 'workflow-steps';

export class WorkerRuntime {
  private readonly database: Database;
  private readonly telemetry: Telemetry;
  private readonly queue: Queue<StepJob>;
  private readonly stepWorker: StepWorker;
  private readonly scheduler: Scheduler;
  private closed = false;

  constructor(config: RuntimeConfig) {
    this.database = new Database(config.DATABASE_URL);
    this.telemetry = new Telemetry({
      endpoint: config.OTEL_EXPORTER_OTLP_ENDPOINT,
    });
    const executions = new ExecutionsRepository(this.database, {
      leaseMs: config.LEASE_MS,
      concurrency: config.CONCURRENCY,
    });
    const queueConnection = redisConnection(config.REDIS_URL, false);
    const workerConnection = redisConnection(config.REDIS_URL, true);
    this.queue = new Queue<StepJob>(QUEUE_NAME, {
      connection: queueConnection,
      prefix: config.QUEUE_PREFIX,
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: true,
        removeOnFail: true,
      },
    });
    this.queue.on('error', () => logEvent.error('worker.queue_error'));
    this.stepWorker = new StepWorker(
      executions,
      this.telemetry,
      config,
      QUEUE_NAME,
      workerConnection,
    );
    this.scheduler = new Scheduler(
      executions,
      this.queue,
      this.telemetry,
      config.POLL_MS,
    );
  }

  async onModuleInit(): Promise<void> {
    try {
      await this.stepWorker.start();
      await this.scheduler.start();
    } catch {
      await this.onApplicationShutdown();
      throw new Error('Worker startup failed');
    }
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    let firstFailure: unknown;
    const cleanup = async (operation: () => Promise<void>): Promise<void> => {
      try {
        await operation();
      } catch (error: unknown) {
        firstFailure ??= error;
      }
    };
    await cleanup(() => this.scheduler.close());
    await cleanup(() => this.stepWorker.pauseAdmissions());
    this.stepWorker.abortActive();
    await cleanup(() => this.stepWorker.settleActive());
    await cleanup(() => this.stepWorker.closeWorker());
    await cleanup(() => this.queue.close());
    await cleanup(() => this.telemetry.shutdown());
    await cleanup(() => this.database.close());
    if (firstFailure) throw firstFailure;
  }
}

@Module({})
export class WorkerModule {
  static register(config: RuntimeConfig): DynamicModule {
    return {
      module: WorkerModule,
      providers: [
        {
          provide: WorkerRuntime,
          useFactory: () => new WorkerRuntime(config),
        },
      ],
    };
  }
}

function redisConnection(raw: string, blocking: boolean): ConnectionOptions {
  const url = new URL(raw);
  const dbText = url.pathname.slice(1);
  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : 6379,
    ...(url.username ? { username: decodeURIComponent(url.username) } : {}),
    ...(url.password ? { password: decodeURIComponent(url.password) } : {}),
    ...(dbText ? { db: Number(dbText) } : {}),
    ...(url.protocol === 'rediss:' ? { tls: {} } : {}),
    maxRetriesPerRequest: blocking ? null : 1,
    ...(!blocking ? { enableOfflineQueue: false } : {}),
  };
}
