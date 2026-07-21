import { spawn, type ChildProcess } from 'node:child_process';
import type { RuntimeConfig } from '../../src/config';

export class WorkerProcess {
  private readonly exited: Promise<void>;
  private constructor(private readonly child: ChildProcess) {
    this.exited = new Promise((resolve) => child.once('exit', () => resolve()));
  }
  static async start(config: RuntimeConfig): Promise<WorkerProcess> {
    const child = spawn(
      process.execPath,
      ['-r', 'ts-node/register', 'src/worker.ts'],
      {
        cwd: process.cwd(),
        env: {
          PATH: process.env.PATH,
          NODE_ENV: 'test',
          TS_NODE_TRANSPILE_ONLY: 'true',
          DATABASE_URL: config.DATABASE_URL,
          REDIS_URL: config.REDIS_URL,
          QUEUE_PREFIX: config.QUEUE_PREFIX,
          CONCURRENCY: String(config.CONCURRENCY),
          LEASE_MS: String(config.LEASE_MS),
          POLL_MS: String(config.POLL_MS),
          INTEGRATIONS_JSON: JSON.stringify(config.INTEGRATIONS),
        },
        stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      },
    );
    const worker = new WorkerProcess(child);
    let output = '';
    child.stdout?.on('data', (chunk: Buffer) => {
      output = (output + chunk.toString()).slice(-4000);
    });
    child.stderr?.on('data', (chunk: Buffer) => {
      output = (output + chunk.toString()).slice(-4000);
    });
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`Worker did not become ready: ${output}`)),
          10000,
        );
        const failed = () => {
          clearTimeout(timer);
          reject(new Error(`Worker exited during startup: ${output}`));
        };
        child.once('error', failed);
        child.once('exit', failed);
        child.on('message', (message: unknown) => {
          if (
            typeof message === 'object' &&
            message !== null &&
            'type' in message &&
            message.type === 'ready'
          ) {
            clearTimeout(timer);
            child.off('error', failed);
            child.off('exit', failed);
            resolve();
          }
        });
      });
      return worker;
    } catch (error) {
      await worker.stop('SIGKILL');
      throw error;
    }
  }
  async stop(signal: 'SIGTERM' | 'SIGKILL' = 'SIGTERM'): Promise<void> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.kill(signal);
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 5000);
    try {
      await this.exited;
      if (
        signal === 'SIGTERM' &&
        (this.child.signalCode !== null || this.child.exitCode !== 0)
      )
        throw new Error('Worker failed to shut down gracefully');
    } finally {
      clearTimeout(timer);
    }
  }
}
