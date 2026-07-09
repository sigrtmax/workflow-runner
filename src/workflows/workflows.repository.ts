import { randomUUID } from 'node:crypto';
import type { QueryResultRow } from 'pg';
import { Database } from '../database';
import type { Definition } from './definition';

export class WorkflowNotFound extends Error {
  constructor() {
    super('Workflow version was not found');
    this.name = 'WorkflowNotFound';
  }
}
export class VersionConflict extends Error {
  constructor() {
    super('Workflow version already exists');
    this.name = 'VersionConflict';
  }
}
export interface PublishedWorkflow {
  id: string;
  name: string;
  version: number;
  definition: Definition;
}
interface DefinitionRow extends QueryResultRow {
  id: string;
  name: string;
  version: number;
  definition: Definition;
}

export class WorkflowsRepository {
  constructor(private readonly db: Database) {}

  async publish(
    name: string,
    version: number,
    definition: Definition,
  ): Promise<PublishedWorkflow> {
    try {
      const result = await this.db.query<DefinitionRow>(
        'INSERT INTO workflow_definitions (id, name, version, definition) VALUES ($1, $2, $3, $4) RETURNING id, name, version, definition',
        [randomUUID(), name, version, JSON.stringify(definition)],
      );
      return result.rows[0]!;
    } catch (error: unknown) {
      if ((error as { code?: string }).code === '23505')
        throw new VersionConflict();
      throw error;
    }
  }

  async get(name: string, version: number): Promise<PublishedWorkflow> {
    const result = await this.db.query<DefinitionRow>(
      'SELECT id, name, version, definition FROM workflow_definitions WHERE name = $1 AND version = $2',
      [name, version],
    );
    if (!result.rowCount) throw new WorkflowNotFound();
    return result.rows[0]!;
  }
}
