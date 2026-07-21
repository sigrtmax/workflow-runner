import { Queue } from 'bullmq';
import type { ExecutionsRepository } from '../executions/executions.repository';
import { logEvent } from '../logging';
import type { Telemetry } from '../telemetry';
import type { StepJob } from './step-worker';

export class Scheduler {
  private timer?: NodeJS.Timeout;
  private currentTick?: Promise<void>;
  private stopped = true;

  constructor(
    private readonly executions: ExecutionsRepository,
    private readonly queue: Queue<StepJob>,
    private readonly telemetry: Telemetry,
    private readonly pollMs: number,
  ) {}

  async start(): Promise<void> {
    this.stopped = false;
    await this.queue.waitUntilReady();
    await this.tick();
    this.schedule();
  }

  async close(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    await this.currentTick;
  }

  tick(): Promise<void> {
    if (!this.currentTick) {
      this.currentTick = this.poll().finally(() => {
        this.currentTick = undefined;
      });
    }
    return this.currentTick;
  }

  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      void this.tick().finally(() => this.schedule());
    }, this.pollMs);
  }

  private async poll(): Promise<void> {
    try {
      const delays = await this.executions.tick();
      for (const delay of delays) {
        const span = this.telemetry.startStep(
          delay.traceContext,
          {
            workflowName: delay.workflowName,
            version: delay.version,
            stepId: delay.stepId,
            stepType: 'delay',
            attempt: delay.attempt,
            executionId: delay.executionId,
          },
          delay.startedAt,
        );
        span.end('succeeded');
      }
      const ready = await this.executions.ready(100);
      for (const item of ready) {
        try {
          await this.queue.add('step', item, {
            jobId: `${item.executionId}-${item.stepId}-${item.attemptCount}`,
            attempts: 1,
            removeOnComplete: true,
            removeOnFail: true,
          });
        } catch {
          logEvent.error('scheduler.queue_add_failed', item);
        }
      }
    } catch {
      logEvent.error('scheduler.recovery_failed');
    }
  }
}
