import type { HttpStep } from '../src/workflows/definition';
import { HttpStepError, httpStep } from '../src/worker/http-step';

const step = (overrides: Partial<HttpStep> = {}): HttpStep => ({
  id: 'send',
  type: 'http',
  integration: 'crm',
  path: '/leads',
  method: 'POST',
  body: { ref: 'input.lead' },
  expectedStatuses: [201],
  timeoutMs: 1_000,
  retry: { maxAttempts: 3, backoffMs: 100, maxBackoffMs: 1_000 },
  safety: 'idempotency-key',
  ...overrides,
});

const context = { input: { lead: { name: 'Ada' } }, steps: {} };
const integrations = { crm: { url: 'https://crm.example/base/' } };
const fetchMock = jest.fn<ReturnType<typeof fetch>, Parameters<typeof fetch>>();

function unexpectedSuccess(): never {
  throw new Error('Expected the HTTP step to reject');
}

function expectFailure(error: unknown, kind: string): void {
  expect(error).toBeInstanceOf(HttpStepError);
  expect((error as HttpStepError).failure.kind).toBe(kind);
}

describe('httpStep', () => {
  beforeEach(() => {
    fetchMock.mockReset();
    jest.spyOn(globalThis, 'fetch').mockImplementation(fetchMock);
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  test('sends a stable idempotency key and payload and accepts exact 201', async () => {
    fetchMock.mockImplementation(
      async () => new Response('{"id":7}', { status: 201 }),
    );

    await expect(
      httpStep(step(), context, integrations, 'exec:send', {}, undefined),
    ).resolves.toEqual({ status: 201, body: { id: 7 } });
    await expect(
      httpStep(step(), context, integrations, 'exec:send', {}, undefined),
    ).resolves.toEqual({ status: 201, body: { id: 7 } });

    for (const call of fetchMock.mock.calls) {
      expect(call[0]).toBe('https://crm.example/leads');
      expect(call[1]).toMatchObject({
        method: 'POST',
        body: '{"name":"Ada"}',
        redirect: 'manual',
      });
      const headers = new Headers(call[1]?.headers);
      expect(headers.get('Idempotency-Key')).toBe('exec:send');
      expect(headers.get('Content-Type')).toBe('application/json');
    }
  });

  test('forwards only supported trace context headers', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 201 }));
    await httpStep(step({ body: undefined }), context, integrations, 'key', {
      traceparent: '00-ab-cd-01',
      tracestate: 'vendor=value',
      authorization: 'secret',
      'x-extra': 'no',
    });

    const headers = new Headers(fetchMock.mock.calls[0]?.[1]?.headers);
    expect(headers.get('idempotency-key')).toBe('key');
    expect(headers.get('traceparent')).toBe('00-ab-cd-01');
    expect(headers.get('tracestate')).toBe('vendor=value');
    expect(headers.has('authorization')).toBe(false);
    expect(headers.has('x-extra')).toBe(false);
  });

  test('uses no body or content type for GET', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
    await httpStep(
      step({
        method: 'GET',
        safety: 'read-only',
        body: undefined,
        expectedStatuses: [200],
      }),
      context,
      integrations,
      'unused',
      {},
    );
    const init = fetchMock.mock.calls[0]?.[1];
    expect(init?.body).toBeUndefined();
    expect(new Headers(init?.headers).has('content-type')).toBe(false);
    expect(new Headers(init?.headers).has('idempotency-key')).toBe(false);
  });

  test('classifies ordinary 400 as permanent without exposing response data', async () => {
    fetchMock.mockResolvedValue(
      new Response('credential=secret', { status: 400 }),
    );
    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => {
        expectFailure(error, 'permanent_http');
        expect((error as Error).message).not.toContain('secret');
      },
    );
  });

  test('classifies 503 and 429 with Retry-After as transient for safe operations', async () => {
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 503 }));
    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => expectFailure(error, 'transient_http'),
    );

    fetchMock.mockResolvedValueOnce(
      new Response(null, { status: 429, headers: { 'Retry-After': '2' } }),
    );
    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => {
        expectFailure(error, 'transient_http');
        expect((error as HttpStepError).failure.retryAfterMs).toBe(2_000);
      },
    );
  });

  test('cancels a stalled response body before reporting a non-expected status', async () => {
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      start() {},
      cancel() {
        cancelled = true;
      },
    });
    fetchMock.mockResolvedValue(new Response(stream, { status: 503 }));

    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => expectFailure(error, 'transient_http'),
    );

    expect(cancelled).toBe(true);
  });

  test('preserves the status failure when response cancellation stalls until timeout', async () => {
    jest.useFakeTimers();
    const stream = new ReadableStream<Uint8Array>({
      start() {},
      cancel: () => new Promise<void>(() => undefined),
    });
    fetchMock.mockResolvedValue(new Response(stream, { status: 503 }));
    const result = httpStep(
      step({ timeoutMs: 50 }),
      context,
      integrations,
      'key',
      {},
    ).then(unexpectedSuccess, (error: unknown) =>
      expectFailure(error, 'transient_http'),
    );

    await jest.advanceTimersByTimeAsync(51);
    await result;
    jest.useRealTimers();
  });

  test('classifies an unsafe transport loss as unknown outcome', async () => {
    fetchMock.mockRejectedValue(new TypeError('socket closed'));
    await httpStep(
      step({ safety: 'unsafe' }),
      context,
      integrations,
      'unused',
      {},
    ).then(unexpectedSuccess, (error: unknown) =>
      expectFailure(error, 'unknown_outcome'),
    );
  });

  test('rejects unknown integrations and origin escapes before fetch', async () => {
    await expect(
      httpStep(step(), context, {}, 'key', {}),
    ).rejects.toMatchObject({
      failure: { kind: 'invalid_data' },
    });
    await expect(
      httpStep(
        step({ path: '//attacker.example/x' }),
        context,
        integrations,
        'key',
        {},
      ),
    ).rejects.toMatchObject({ failure: { kind: 'invalid_data' } });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('times out while reading a stalled streamed body', async () => {
    jest.useFakeTimers();
    const stream = new ReadableStream<Uint8Array>({ start() {} });
    fetchMock.mockResolvedValue(new Response(stream, { status: 201 }));
    const result = httpStep(
      step({ timeoutMs: 50 }),
      context,
      integrations,
      'key',
      {},
    ).then(unexpectedSuccess, (error: unknown) =>
      expectFailure(error, 'timeout'),
    );
    await jest.advanceTimersByTimeAsync(51);
    await result;
    jest.useRealTimers();
  });

  test('rejects oversized and malformed JSON responses', async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(new Uint8Array(1024 * 1024 + 1), { status: 201 }),
    );
    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => expectFailure(error, 'permanent_http'),
    );

    fetchMock.mockResolvedValueOnce(new Response('not json', { status: 201 }));
    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => expectFailure(error, 'permanent_http'),
    );
  });

  test('accepts deeply nested finite JSON within the response byte limit', async () => {
    const deeplyNested = `${'['.repeat(20_000)}0${']'.repeat(20_000)}`;
    fetchMock.mockResolvedValue(new Response(deeplyNested, { status: 201 }));

    const result = await httpStep(step(), context, integrations, 'key', {});

    expect(result).toHaveProperty('status', 201);
    expect(Array.isArray((result as { body: unknown }).body)).toBe(true);
  });

  test('rejects a parsed nonfinite JSON number as a permanent contract failure', async () => {
    fetchMock.mockResolvedValue(new Response('1e400', { status: 201 }));

    await httpStep(step(), context, integrations, 'key', {}).then(
      unexpectedSuccess,
      (error: unknown) => expectFailure(error, 'permanent_http'),
    );
  });
});
