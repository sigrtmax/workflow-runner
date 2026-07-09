import { validateConfig } from '../src/config';
const base = {
  DATABASE_URL: 'postgres://workflow:workflow_local@127.0.0.1/workflow',
  REDIS_URL: 'redis://127.0.0.1:6379',
};
describe('startup configuration', () => {
  test('validates the declared connections and parses bounded worker settings', () => {
    expect(
      validateConfig({
        ...base,
        CONCURRENCY: '2',
        INTEGRATIONS_JSON: '{"crm":{"url":"https://crm.example"}}',
      }),
    ).toMatchObject({
      CONCURRENCY: 2,
      LEASE_MS: 15000,
      INTEGRATIONS: { crm: { url: 'https://crm.example' } },
    });
  });
  test.each([
    { DATABASE_URL: undefined },
    { REDIS_URL: 'https://redis.example' },
    { CONCURRENCY: '0' },
    { LEASE_MS: '20' },
    { INTEGRATIONS_JSON: '{"crm":{"url":"file:///etc/passwd"}}' },
    { INTEGRATIONS_JSON: '{"crm":{"url":"https://user:secret@crm.example"}}' },
  ])('rejects invalid config without exposing values', (invalid) => {
    expect(() => validateConfig({ ...base, ...invalid })).toThrow();
    try {
      validateConfig({ ...base, ...invalid });
    } catch (error) {
      expect((error as Error).message).not.toContain('secret');
    }
  });
});
