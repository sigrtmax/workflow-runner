import type { Step } from '../workflows/definition';

export type FailureKind =
  | 'invalid_data'
  | 'permanent_http'
  | 'transient_http'
  | 'timeout'
  | 'transport'
  | 'unknown_outcome'
  | 'internal';

export interface StepFailure {
  kind: FailureKind;
  message: string;
  status?: number;
  retryAfterMs?: number;
}

export function retryDelay(
  step: Step,
  failure: StepFailure,
  attemptInCycle: number,
): number | null {
  if (!Number.isSafeInteger(attemptInCycle) || attemptInCycle < 1) {
    throw new RangeError('attempt must be a positive integer');
  }
  if (step.type !== 'http' || step.safety === 'unsafe') return null;
  if (attemptInCycle >= step.retry.maxAttempts) return null;

  const retryable =
    failure.kind === 'transient_http' ||
    failure.kind === 'timeout' ||
    failure.kind === 'transport' ||
    (failure.kind === 'unknown_outcome' &&
      (step.safety === 'read-only' || step.safety === 'idempotency-key'));
  if (!retryable) return null;

  const exponential = step.retry.backoffMs * 2 ** (attemptInCycle - 1);
  const backoff = Math.min(step.retry.maxBackoffMs, exponential);
  const retryAfter = failure.retryAfterMs;
  if (
    retryAfter === undefined ||
    !Number.isFinite(retryAfter) ||
    retryAfter < 0
  ) {
    return backoff;
  }
  if (retryAfter > step.retry.maxBackoffMs) return null;
  return Math.max(backoff, retryAfter);
}
