import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { Database } from '../../src/database';
import { validateConfig, type RuntimeConfig } from '../../src/config';

const composeArgs = [
  'compose',
  '--env-file',
  '/dev/null',
  '-f',
  'compose.yaml',
  '-p',
  'workflow-runner-test',
];
export function testConnectionUrls(): {
  databaseUrl: string;
  redisUrl: string;
} {
  const port = (service: string, target: string) =>
    execFileSync('docker', [...composeArgs, 'port', service, target], {
      encoding: 'utf8',
    }).trim();
  return {
    databaseUrl: `postgres://workflow:workflow_local@${port('postgres', '5432')}/workflow`,
    redisUrl: `redis://${port('redis', '6379')}`,
  };
}
export class TestInfrastructure {
  private constructor(
    readonly db: Database,
    readonly config: RuntimeConfig,
    private readonly admin: Pool,
    private readonly name: string,
  ) {}
  static async create(): Promise<TestInfrastructure> {
    const urls = testConnectionUrls();
    if (new URL(urls.databaseUrl).hostname !== '127.0.0.1')
      throw new Error('Test database must be local');
    const admin = new Pool({ connectionString: urls.databaseUrl });
    const name = `workflow_runner_test_${randomUUID().replaceAll('-', '')}`;
    await admin.query(`CREATE DATABASE "${name}"`);
    const url = new URL(urls.databaseUrl);
    url.pathname = `/${name}`;
    const config = validateConfig({
      DATABASE_URL: url.toString(),
      REDIS_URL: urls.redisUrl,
      PORT: 0,
      QUEUE_PREFIX: `workflow-runner-test-${randomUUID()}`,
      CONCURRENCY: 2,
      LEASE_MS: 600,
      POLL_MS: 30,
    });
    return new TestInfrastructure(
      new Database(config.DATABASE_URL),
      config,
      admin,
      name,
    );
  }
  async close(): Promise<void> {
    await this.db.close();
    try {
      // pg removes clients from its pool before their sockets finish closing.
      await eventually(
        async () => {
          const result = await this.admin.query<{ count: string }>(
            'SELECT count(*) FROM pg_stat_activity WHERE datname=$1',
            [this.name],
          );
          return Number(result.rows[0]!.count);
        },
        (count) => count === 0,
      );
      await this.admin.query(`DROP DATABASE "${this.name}"`);
    } finally {
      await this.admin.end();
    }
  }
}
export async function eventually<T>(
  read: () => Promise<T>,
  accept: (value: T) => boolean,
  timeoutMs = 10000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T;
  do {
    last = await read();
    if (accept(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, 20));
  } while (Date.now() < deadline);
  throw new Error(`Condition was not met: ${JSON.stringify(last!)}`);
}
