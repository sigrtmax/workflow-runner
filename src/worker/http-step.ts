import type { HttpStep, Json } from '../workflows/definition';
import { evaluate, type DataContext } from './transform-step';
import type { StepFailure } from './retry-policy';

const MAX_RESPONSE_BYTES = 1024 * 1024;

export class HttpStepError extends Error {
  constructor(public readonly failure: StepFailure) {
    super(failure.message);
    this.name = 'HttpStepError';
  }
}

export interface Integration {
  url: string;
}

export async function httpStep(
  step: HttpStep,
  context: DataContext,
  integrations: Record<string, Integration>,
  idempotencyKey: string,
  traceHeaders: Record<string, string>,
  signal?: AbortSignal,
): Promise<Json> {
  const integration = integrations[step.integration];
  if (!integration) fail('invalid_data', 'Unknown HTTP integration');

  const url = resolveUrl(step.path, integration.url);
  const headers = new Headers();
  for (const name of ['traceparent', 'tracestate'] as const) {
    const value = traceHeaders[name];
    if (value !== undefined) headers.set(name, value);
  }

  let body: string | undefined;
  if (step.method !== 'GET' && step.body !== undefined) {
    body = JSON.stringify(evaluate(step.body, context));
    headers.set('Content-Type', 'application/json');
  }
  if (step.safety === 'idempotency-key')
    headers.set('Idempotency-Key', idempotencyKey);

  const controller = new AbortController();
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, step.timeoutMs);
  const abortFromCaller = (): void => controller.abort(signal?.reason);
  if (signal?.aborted) abortFromCaller();
  else signal?.addEventListener('abort', abortFromCaller, { once: true });

  try {
    const response = await fetch(url, {
      method: step.method,
      headers,
      body,
      redirect: 'manual',
      signal: controller.signal,
    });

    if (!step.expectedStatuses.includes(response.status)) {
      const failure = statusFailure(step, response);
      await cancelResponseBody(response.body, controller.signal);
      throw failure;
    }

    const bytes = await readBounded(response, controller.signal);
    if (bytes.byteLength === 0) return { status: response.status, body: null };
    let parsed: unknown;
    try {
      parsed = JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      fail(
        'permanent_http',
        'HTTP response was not valid JSON',
        response.status,
      );
    }
    if (!isJson(parsed)) {
      fail(
        'permanent_http',
        'HTTP response was not valid JSON',
        response.status,
      );
    }
    return { status: response.status, body: parsed };
  } catch (error: unknown) {
    if (error instanceof HttpStepError) throw error;
    if (timedOut)
      throw requestFailure(step, 'timeout', 'HTTP request timed out');
    throw requestFailure(step, 'transport', 'HTTP request failed');
  } finally {
    clearTimeout(timeout);
    signal?.removeEventListener('abort', abortFromCaller);
  }
}

function resolveUrl(path: string, configuredUrl: string): string {
  try {
    if (
      !path.startsWith('/') ||
      path.startsWith('//') ||
      /[\\\r\n]/.test(path)
    ) {
      fail('invalid_data', 'Invalid HTTP path');
    }
    const base = new URL(configuredUrl);
    if (
      !['http:', 'https:'].includes(base.protocol) ||
      base.username ||
      base.password ||
      base.hash
    ) {
      fail('invalid_data', 'Invalid HTTP integration URL');
    }
    const resolved = new URL(path, base);
    if (
      resolved.origin !== base.origin ||
      resolved.username ||
      resolved.password ||
      resolved.hash
    ) {
      fail('invalid_data', 'HTTP path cannot change integration origin');
    }
    return resolved.href;
  } catch (error: unknown) {
    if (error instanceof HttpStepError) throw error;
    fail('invalid_data', 'Invalid HTTP integration URL');
  }
}

function statusFailure(step: HttpStep, response: Response): HttpStepError {
  const status = response.status;
  if (step.safety === 'unsafe') {
    const kind =
      status === 408 || status >= 500 ? 'unknown_outcome' : 'permanent_http';
    return error(
      kind,
      'HTTP response did not satisfy the step contract',
      status,
    );
  }
  if (status === 408 || status === 429 || status >= 500) {
    return error(
      'transient_http',
      'HTTP service returned a temporary failure',
      status,
      status === 429
        ? parseRetryAfter(response.headers.get('Retry-After'))
        : undefined,
    );
  }
  return error(
    'permanent_http',
    'HTTP response did not satisfy the step contract',
    status,
  );
}

function parseRetryAfter(value: string | null): number | undefined {
  if (value === null) return undefined;
  const trimmed = value.trim();
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    const milliseconds = Number(trimmed) * 1000;
    return Number.isFinite(milliseconds) ? milliseconds : undefined;
  }
  const timestamp = Date.parse(trimmed);
  if (!Number.isFinite(timestamp)) return undefined;
  return Math.max(0, timestamp - Date.now());
}

async function readBounded(
  response: Response,
  signal: AbortSignal,
): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let cancellationRequested = false;
  const rejectAbort = (): void => abortRead();
  let abortRead: (reason?: unknown) => void = () => undefined;
  const aborted = new Promise<never>((_, reject) => {
    abortRead = reject;
    if (signal.aborted) rejectAbort();
    else signal.addEventListener('abort', rejectAbort, { once: true });
  });
  try {
    while (true) {
      const result = await Promise.race([reader.read(), aborted]);
      if (result.done) break;
      total += result.value.byteLength;
      if (total > MAX_RESPONSE_BYTES) {
        cancellationRequested = true;
        await settleCleanup(reader.cancel(), signal);
        fail(
          'permanent_http',
          'HTTP response exceeded the size limit',
          response.status,
        );
      }
      chunks.push(result.value);
    }
  } catch (caught: unknown) {
    if (!cancellationRequested) {
      await settleCleanup(reader.cancel(), signal);
    }
    throw caught;
  } finally {
    signal.removeEventListener('abort', rejectAbort);
    reader.releaseLock();
  }
  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return combined;
}

function isJson(value: unknown): value is Json {
  const pending: unknown[] = [value];
  while (pending.length > 0) {
    const item = pending.pop();
    if (
      item === null ||
      typeof item === 'boolean' ||
      typeof item === 'string'
    ) {
      continue;
    }
    if (typeof item === 'number') {
      if (!Number.isFinite(item)) return false;
      continue;
    }
    if (typeof item !== 'object') return false;
    for (const child of Object.values(item)) {
      pending.push(child);
    }
  }
  return true;
}

async function cancelResponseBody(
  body: ReadableStream<Uint8Array> | null,
  signal: AbortSignal,
): Promise<void> {
  if (!body) return;
  await settleCleanup(body.cancel(), signal);
}

async function settleCleanup(
  cleanup: Promise<unknown>,
  signal: AbortSignal,
): Promise<void> {
  const settledCleanup = cleanup.then(
    () => undefined,
    (_cleanupError: unknown) => {
      // Cleanup is best-effort and must not replace the classified HTTP failure.
    },
  );
  if (signal.aborted) return;

  let finishOnAbort = (): void => undefined;
  const aborted = new Promise<void>((resolve) => {
    finishOnAbort = resolve;
    signal.addEventListener('abort', finishOnAbort, { once: true });
    if (signal.aborted) resolve();
  });
  try {
    await Promise.race([settledCleanup, aborted]);
  } finally {
    signal.removeEventListener('abort', finishOnAbort);
  }
}

function requestFailure(
  step: HttpStep,
  safeKind: 'timeout' | 'transport',
  message: string,
): HttpStepError {
  return error(
    step.safety === 'unsafe' ? 'unknown_outcome' : safeKind,
    message,
  );
}

function fail(
  kind: StepFailure['kind'],
  message: string,
  status?: number,
  retryAfterMs?: number,
): never {
  throw error(kind, message, status, retryAfterMs);
}

function error(
  kind: StepFailure['kind'],
  message: string,
  status?: number,
  retryAfterMs?: number,
): HttpStepError {
  return new HttpStepError({ kind, message, status, retryAfterMs });
}
