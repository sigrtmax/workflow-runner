import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { Database } from '../src/database';

export async function migrate(db: Database): Promise<void> {
  await db.transaction(async (client) => {
    await client.query('SELECT pg_advisory_xact_lock(72849301)');
    await client.query(
      'CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, checksum text NOT NULL)',
    );
    const directory = resolve(__dirname, '../migrations');
    for (const name of (await readdir(directory))
      .filter((name) => name.endsWith('.sql'))
      .sort()) {
      const sql = await readFile(resolve(directory, name), 'utf8');
      const checksum = createHash('sha256').update(sql).digest('hex');
      const applied = await client.query<{ checksum: string }>(
        'SELECT checksum FROM schema_migrations WHERE name=$1',
        [name],
      );
      if (applied.rowCount) {
        if (applied.rows[0]!.checksum !== checksum)
          throw new Error(`Applied migration changed: ${name}`);
        continue;
      }
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations VALUES ($1,$2)', [
        name,
        checksum,
      ]);
    }
  });
}
if (require.main === module) {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error('DATABASE_URL is required');
  const db = new Database(url);
  void migrate(db)
    .catch(() => {
      process.stderr.write('Database migration failed\n');
      process.exitCode = 1;
    })
    .finally(() => db.close());
}
