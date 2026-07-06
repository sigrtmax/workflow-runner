import type { HttpStep, Step } from '../src/workflows/definition';
import { retryDelay, type StepFailure } from '../src/worker/retry-policy';

const http = (overrides: Partial<HttpStep> = {}): HttpStep => ({
  id: 'send',
  type: 'http',
  integration: 'crm',
  path: '/leads',
  method: 'POST',
  expectedStatuses: [201],
  timeoutMs: 1_000,
  retry: { maxAttempts: 4, backoffMs: 100, maxBackoffMs: 250 },
  safety: 'idempotency-key',
  ...overrides,
});

const failure = (overrides: Partial<StepFailure> = {}): StepFailure => ({
  kind: 'transient_http',
  message: 'temporary HTTP failure',
  ...overrides,
});

describe('retryDelay', () => {
  test('applies exponential backoff and caps it', () => {
    expect(retryDelay(http(), failure({ status: 503 }), 1)).toBe(100);
    expect(retryDelay(http(), failure({ status: 503 }), 2)).toBe(200);
    expect(retryDelay(http(), failure({ status: 503 }), 3)).toBe(250);
  });

  test('stops at the attempt budget', () => {
    expect(retryDelay(http(), failure({ status: 503 }), 4)).toBeNull();
  });

  test('uses an in-budget Retry-After delay and requires manual action beyond the cap', () => {
    expect(
      retryDelay(http(), failure({ status: 429, retryAfterMs: 220 }), 1),
    ).toBe(220);
    expect(
      retryDelay(http(), failure({ status: 429, retryAfterMs: 251 }), 1),
    ).toBeNull();
  });

  test('does not retry permanent, invalid, or internal failures', () => {
    expect(
      retryDelay(http(), failure({ kind: 'permanent_http' }), 1),
    ).toBeNull();
    expect(retryDelay(http(), failure({ kind: 'invalid_data' }), 1)).toBeNull();
    expect(retryDelay(http(), failure({ kind: 'internal' }), 1)).toBeNull();
  });

  test('does not retry unsafe or non-HTTP steps', () => {
    expect(retryDelay(http({ safety: 'unsafe' }), failure(), 1)).toBeNull();
    const delay: Step = { id: 'wait', type: 'delay', durationMs: 10 };
    expect(retryDelay(delay, failure(), 1)).toBeNull();
  });

  test('retries unknown outcomes only with read-only or idempotency-key safety', () => {
    const unknown = failure({ kind: 'unknown_outcome' });
    expect(retryDelay(http({ safety: 'idempotency-key' }), unknown, 1)).toBe(
      100,
    );
    expect(
      retryDelay(http({ method: 'GET', safety: 'read-only' }), unknown, 1),
    ).toBe(100);
  });

  test('rejects invalid attempt numbers', () => {
    for (const attempt of [0, -1, 1.5, Number.NaN]) {
      expect(() => retryDelay(http(), failure(), attempt)).toThrow('attempt');
    }
  });
});
