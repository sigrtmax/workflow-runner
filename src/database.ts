import {
  Pool,
  type PoolClient,
  type QueryResult,
  type QueryResultRow,
} from 'pg';

export class Database {
  readonly pool: Pool;
  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 10,
      connectionTimeoutMillis: 5000,
    });
  }
  query<R extends QueryResultRow = QueryResultRow>(
    sql: string,
    values?: unknown[],
  ): Promise<QueryResult<R>> {
    return this.pool.query<R>(sql, values);
  }
  async transaction<T>(
    operation: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    let released = false;
    try {
      await client.query('BEGIN');
      const result = await operation(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        client.release(true);
        released = true;
        throw error;
      }
      throw error;
    } finally {
      if (!released) client.release();
    }
  }
  async close(): Promise<void> {
    await this.pool.end();
  }
  async onModuleDestroy(): Promise<void> {
    await this.close();
  }
}
