export interface RuntimeConfig {
  DATABASE_URL: string;
  REDIS_URL: string;
  PORT: number;
  QUEUE_PREFIX: string;
  CONCURRENCY: number;
  LEASE_MS: number;
  POLL_MS: number;
  INTEGRATIONS: Record<string, { url: string }>;
  OTEL_EXPORTER_OTLP_ENDPOINT?: string;
}
export function validateConfig(raw: Record<string, unknown>): RuntimeConfig {
  const integer = (
    name: string,
    fallback: number,
    min: number,
    max: number,
  ): number => {
    const value = raw[name] ?? fallback;
    const parsed =
      typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
    if (
      typeof parsed !== 'number' ||
      !Number.isSafeInteger(parsed) ||
      parsed < min ||
      parsed > max
    ) {
      throw new Error(`Invalid ${name}`);
    }
    return parsed;
  };
  const databaseUrl = connection(raw.DATABASE_URL, 'DATABASE_URL', [
    'postgres:',
    'postgresql:',
  ]);
  const redisUrl = connection(raw.REDIS_URL, 'REDIS_URL', [
    'redis:',
    'rediss:',
  ]);
  const prefix = raw.QUEUE_PREFIX ?? 'workflow-runner';
  if (typeof prefix !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(prefix))
    throw new Error('Invalid QUEUE_PREFIX');
  let parsed: unknown;
  try {
    parsed = JSON.parse(
      typeof raw.INTEGRATIONS_JSON === 'string' ? raw.INTEGRATIONS_JSON : '{}',
    );
  } catch {
    throw new Error('Invalid INTEGRATIONS_JSON');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))
    throw new Error('Invalid INTEGRATIONS_JSON');
  const integrations: RuntimeConfig['INTEGRATIONS'] = Object.create(
    null,
  ) as RuntimeConfig['INTEGRATIONS'];
  for (const [name, value] of Object.entries(parsed)) {
    if (
      !/^[a-z][a-z0-9_]{0,63}$/.test(name) ||
      ['constructor', 'prototype', '__proto__'].includes(name) ||
      !value ||
      typeof value !== 'object' ||
      Array.isArray(value) ||
      Object.keys(value).join() !== 'url'
    )
      throw new Error('Invalid INTEGRATIONS_JSON');
    const url = connection((value as { url: unknown }).url, 'integration URL', [
      'http:',
      'https:',
    ]);
    const target = new URL(url);
    if (target.username || target.password || target.hash)
      throw new Error('Invalid integration URL');
    integrations[name] = { url };
  }
  const result: RuntimeConfig = {
    DATABASE_URL: databaseUrl,
    REDIS_URL: redisUrl,
    PORT: integer('PORT', 3000, 0, 65535),
    QUEUE_PREFIX: prefix,
    CONCURRENCY: integer('CONCURRENCY', 4, 1, 64),
    LEASE_MS: integer('LEASE_MS', 15000, 300, 300000),
    POLL_MS: integer('POLL_MS', 250, 10, 10000),
    INTEGRATIONS: integrations,
  };
  if (result.POLL_MS >= result.LEASE_MS)
    throw new Error('POLL_MS must be less than LEASE_MS');
  if (raw.OTEL_EXPORTER_OTLP_ENDPOINT)
    result.OTEL_EXPORTER_OTLP_ENDPOINT = connection(
      raw.OTEL_EXPORTER_OTLP_ENDPOINT,
      'OTEL_EXPORTER_OTLP_ENDPOINT',
      ['http:', 'https:'],
    );
  return result;
}
function connection(value: unknown, name: string, protocols: string[]): string {
  try {
    if (typeof value !== 'string') throw new Error();
    const url = new URL(value);
    if (!protocols.includes(url.protocol) || !url.hostname || url.hash)
      throw new Error();
    return value;
  } catch {
    throw new Error(`Invalid ${name}`);
  }
}
