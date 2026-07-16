import { Injectable } from '@nestjs/common';
import { validateDefinition } from './definition';
import {
  WorkflowsRepository,
  type PublishedWorkflow,
} from './workflows.repository';

@Injectable()
export class WorkflowsService {
  constructor(private readonly workflows: WorkflowsRepository) {}

  async publish(
    name: string,
    version: number,
    definition: unknown,
  ): Promise<PublishedWorkflow> {
    return this.workflows.publish(
      name,
      version,
      validateDefinition(definition),
    );
  }

  async get(name: string, version: number): Promise<PublishedWorkflow> {
    return this.workflows.get(name, version);
  }
}
