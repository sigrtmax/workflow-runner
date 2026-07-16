import { BadRequestException, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Telemetry } from '../telemetry';
import { validateJsonData, type Json } from '../workflows/definition';
import { WorkflowsRepository } from '../workflows/workflows.repository';
import { ExecutionsRepository } from './executions.repository';

const IDEMPOTENCY_KEY = /^[\x21-\x7e]{1,128}$/;

@Injectable()
export class ExecutionsService {
  constructor(
    private readonly workflows: WorkflowsRepository,
    private readonly executions: ExecutionsRepository,
    private readonly telemetry: Telemetry,
  ) {}

  async launch(
    workflowName: string,
    version: number,
    input: unknown,
    key: string | undefined,
  ) {
    if (!key || !IDEMPOTENCY_KEY.test(key)) throw new BadRequestException();
    validateJsonData(input, 'input');
    const workflow = await this.workflows.get(workflowName, version);
    const id = randomUUID();
    const span = this.telemetry.startWorkflow(
      id,
      workflow.name,
      workflow.version,
    );
    try {
      const launched = await this.executions.launch(
        workflow,
        input as Json,
        key,
        span.carrier,
        id,
      );
      span.end(launched.created ? 'accepted' : 'duplicate');
      return launched;
    } catch (error) {
      span.end('rejected');
      throw error;
    }
  }

  get(id: string) {
    return this.executions.get(id);
  }
  async events(id: string, after?: string, limit?: number) {
    if (after && BigInt(after) > 9_223_372_036_854_775_807n)
      throw new BadRequestException();
    await this.executions.get(id);
    return this.executions.events(id, after, limit);
  }
  async pause(id: string) {
    await this.executions.pause(id);
    return this.executions.get(id);
  }
  async resume(id: string) {
    await this.executions.resume(id);
    return this.executions.get(id);
  }
  async retry(id: string, confirmUnknownOutcome?: boolean) {
    await this.executions.retry(id, confirmUnknownOutcome);
    return this.executions.get(id);
  }
}
