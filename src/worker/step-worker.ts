import { Worker, type Job, type ConnectionOptions } from 'bullmq';
import type { RuntimeConfig } from '../config';
import type {
  Claim,
  ExecutionsRepository,
} from '../executions/executions.repository';
import { logEvent } from '../logging';
import type { Telemetry, WorkflowSpan } from '../telemetry';
import {
  DefinitionError,
  validateJsonString,
  type Json,
} from '../workflows/definition';
import { condition } from './condition-step';
import { HttpStepError, httpStep } from './http-step';
import { DataError, transform } from './transform-step';

export interface StepJob {
  executionId: string;
  stepId: string;
  attemptCount: number;
}

export class StepWorker {
  private worker?: Worker<StepJob>;
  private stopping = false;
  private readonly active = new Set<Promise<void>>();
  private readonly controllers = new Set<AbortController>();

  constructor(
    private readonly executions: ExecutionsRepository,
    private readonly telemetry: Telemetry,
    private readonly config: RuntimeConfig,
    private readonly queueName: string,
    private readonly connection: ConnectionOptions,
  ) {}

  async start(): Promise<void> {
    const worker = new Worker<StepJob>(
      this.queueName,
      async (job) => this.track(job),
      {
        connection: this.connection,
        prefix: this.config.QUEUE_PREFIX,
        concurrency: this.config.CONCURRENCY,
      },
    );
    worker.on('error', () => logEvent.error('worker.queue_error'));
    this.worker = worker;
    await worker.waitUntilReady();
  }

  abortActive(): void {
    for (const controller of this.controllers) controller.abort();
  }

  async pauseAdmissions(): Promise<void> {
    this.stopping = true;
    await this.worker?.pause(true);
  }

  async settleActive(): Promise<void> {
    await Promise.allSettled([...this.active]);
  }

  async closeWorker(): Promise<void> {
    await this.worker?.close();
    this.worker = undefined;
  }

  private async track(job: Job<StepJob>): Promise<void> {
    const running = this.execute(job.data);
    this.active.add(running);
    try {
      await running;
    } finally {
      this.active.delete(running);
    }
  }

  private async execute(data: StepJob): Promise<void> {
    let claim: Claim | null;
    try {
      claim = await this.executions.claim(data.executionId, data.stepId);
    } catch {
      logEvent.error('worker.claim_failed', {
        executionId: data.executionId,
        stepId: data.stepId,
      });
      return;
    }
    if (!claim) return;

    const span = this.telemetry.startStep(claim.traceContext, {
      workflowName: claim.workflowName,
      version: claim.version,
      stepId: claim.step.id,
      stepType: claim.step.type,
      attempt: claim.attempt,
      executionId: claim.executionId,
    });
    const controller = new AbortController();
    this.controllers.add(controller);
    if (this.stopping) controller.abort();
    const lease = this.maintainLease(claim, controller);
    try {
      const output = await this.runStep(claim, span, controller.signal);
      await lease.stop();
      if (!lease.owned()) {
        span.end('lease_lost');
        return;
      }
      try {
        assertPersistable(output);
      } catch (error) {
        if (
          claim.step.type === 'http' &&
          (error instanceof DataError || error instanceof DefinitionError)
        ) {
          throw new HttpStepError({
            kind: 'permanent_http',
            status: (output as { status: number }).status,
            message: 'HTTP result cannot be persisted',
          });
        }
        throw error;
      }
      let persisted: boolean;
      try {
        persisted = await this.executions.succeed(claim, output);
      } catch {
        span.end('infrastructure_failure');
        logEvent.error('worker.persistence_failed', contextFor(claim));
        return;
      }
      span.end(persisted ? 'succeeded' : 'lease_lost');
      if (persisted) logEvent('step.succeeded', contextFor(claim));
    } catch (caught: unknown) {
      await lease.stop();
      if (!lease.owned()) {
        span.end('lease_lost');
        return;
      }
      const failure =
        caught instanceof HttpStepError
          ? caught.failure
          : caught instanceof DataError || caught instanceof DefinitionError
            ? { kind: 'invalid_data' as const, message: caught.message }
            : {
                kind: 'internal' as const,
                message: 'Step execution failed internally',
              };
      try {
        const persisted = await this.executions.fail(claim, failure);
        span.end(persisted ? failure.kind : 'lease_lost');
        if (persisted)
          logEvent.warn('step.failed', {
            ...contextFor(claim),
            outcome: failure.kind,
          });
      } catch {
        span.end('infrastructure_failure');
        logEvent.error('worker.persistence_failed', contextFor(claim));
      }
    } finally {
      await lease.stop();
      this.controllers.delete(controller);
    }
  }

  private async runStep(
    claim: Claim,
    span: WorkflowSpan,
    signal: AbortSignal,
  ): Promise<Json> {
    const context = { input: claim.input, steps: claim.outputs };
    if (claim.step.type === 'transform') return transform(claim.step, context);
    if (claim.step.type === 'condition') return condition(claim.step, context);
    if (claim.step.type === 'http') {
      return httpStep(
        claim.step,
        context,
        this.config.INTEGRATIONS,
        `${claim.executionId}-${claim.step.id}`,
        span.carrier,
        signal,
      );
    }
    throw new Error('Delay steps are completed by the scheduler');
  }

  private maintainLease(
    claim: Claim,
    controller: AbortController,
  ): { owned(): boolean; stop(): Promise<void> } {
    let ownsLease = true;
    let stopped = false;
    let renewing: Promise<void> | undefined;
    let timer: NodeJS.Timeout | undefined;
    const renew = async (): Promise<void> => {
      if (stopped || renewing) return;
      renewing = (async () => {
        try {
          if (!(await this.executions.heartbeat(claim))) {
            ownsLease = false;
            controller.abort();
          }
        } catch {
          ownsLease = false;
          controller.abort();
          logEvent.error('worker.heartbeat_failed', contextFor(claim));
        }
      })().finally(() => {
        renewing = undefined;
        if (!stopped && ownsLease) schedule();
      });
      await renewing;
    };
    const schedule = (): void => {
      timer = setTimeout(
        () => void renew(),
        Math.floor(this.config.LEASE_MS / 3),
      );
    };
    schedule();
    return {
      owned: () => ownsLease,
      stop: async () => {
        stopped = true;
        if (timer) clearTimeout(timer);
        await renewing;
      },
    };
  }
}

function contextFor(claim: Claim) {
  return {
    executionId: claim.executionId,
    stepId: claim.step.id,
    attempt: claim.attempt,
    traceId: claim.traceContext.traceparent?.split('-')[1],
  };
}

function assertPersistable(value: Json): void {
  const pending: Array<{ value: Json; depth: number }> = [{ value, depth: 0 }];
  while (pending.length > 0) {
    const item = pending.pop()!;
    if (item.depth > 32) {
      throw new DataError('Step output exceeds persistence nesting limit');
    }
    if (typeof item.value === 'string')
      validateJsonString(item.value, 'output');
    if (item.value !== null && typeof item.value === 'object') {
      for (const [key, child] of Object.entries(item.value)) {
        validateJsonString(key, 'output key');
        pending.push({ value: child, depth: item.depth + 1 });
      }
    }
  }
  if (Buffer.byteLength(JSON.stringify(value), 'utf8') > 1024 * 1024) {
    throw new DataError('Step output exceeds persistence size limit');
  }
}
