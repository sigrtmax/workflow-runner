import { migrate } from '../scripts/migrate';
import {
  ExecutionConflict,
  ExecutionsRepository,
} from '../src/executions/executions.repository';
import {
  VersionConflict,
  WorkflowsRepository,
} from '../src/workflows/workflows.repository';
import { validateDefinition } from '../src/workflows/definition';
import { TestInfrastructure } from './support/infrastructure';

describe('workflow launch persistence', () => {
  let infra: TestInfrastructure;

  beforeAll(async () => {
    infra = await TestInfrastructure.create();
    await migrate(infra.db);
  });
  afterAll(async () => infra?.close());

  test('creates an execution once for concurrent equivalent idempotency keys', async () => {
    const workflows = new WorkflowsRepository(infra.db);
    const workflow = await workflows.publish(
      'leads',
      1,
      validateDefinition({
        steps: [{ id: 'wait', type: 'delay', durationMs: 1 }],
      }),
    );
    const executions = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 2,
    });

    const created = await Promise.all(
      Array.from({ length: 20 }, () =>
        executions.launch(workflow, { lead: 'a' }, 'one-key', {}),
      ),
    );

    expect(new Set(created.map((item) => item.id)).size).toBe(1);
    expect(created.filter((item) => item.created)).toHaveLength(1);
    expect((await executions.get(created[0]!.id)).steps).toEqual([
      expect.objectContaining({ id: 'wait', status: 'ready' }),
    ]);
  });

  test('rejects duplicate versions, SQL definition mutation, and changed idempotency content', async () => {
    const workflows = new WorkflowsRepository(infra.db);
    const definition = validateDefinition({
      steps: [{ id: 'wait', type: 'delay', durationMs: 1 }],
    });
    const workflow = await workflows.publish('immutable', 1, definition);
    await expect(
      workflows.publish('immutable', 1, definition),
    ).rejects.toBeInstanceOf(VersionConflict);
    await expect(
      infra.db.query('UPDATE workflow_definitions SET name=$2 WHERE id=$1', [
        workflow.id,
        'changed',
      ]),
    ).rejects.toMatchObject({ code: '55000' });
    const executions = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 2,
    });
    await executions.launch(workflow, { value: 1 }, 'conflict-key', {});
    await expect(
      executions.launch(workflow, { value: 2 }, 'conflict-key', {}),
    ).rejects.toBeInstanceOf(ExecutionConflict);
  });

  test.each([null, ['one', 2], 'a JSON string'])(
    'persists a %p JSON launch input without SQL coercion',
    async (input) => {
      const workflow = await new WorkflowsRepository(infra.db).publish(
        `json_input_${Math.random().toString().slice(2)}`,
        1,
        validateDefinition({
          steps: [{ id: 'work', type: 'transform', fields: {} }],
        }),
      );
      const executions = new ExecutionsRepository(infra.db, {
        leaseMs: 1_000,
        concurrency: 2,
      });
      const { id } = await executions.launch(
        workflow,
        input,
        `json-input-${Math.random()}`,
        {},
      );
      expect((await executions.get(id)).input).toEqual(input);
    },
  );

  test.each([null, ['one', 2], 'a JSON string'])(
    'persists a %p JSON success output without SQL coercion',
    async (output) => {
      const workflow = await new WorkflowsRepository(infra.db).publish(
        `json_output_${Math.random().toString().slice(2)}`,
        1,
        validateDefinition({
          steps: [{ id: 'work', type: 'transform', fields: {} }],
        }),
      );
      const executions = new ExecutionsRepository(infra.db, {
        leaseMs: 1_000,
        concurrency: 2,
      });
      const { id } = await executions.launch(
        workflow,
        {},
        `json-output-${Math.random()}`,
        {},
      );
      const claim = await executions.claim(id, 'work');
      await executions.succeed(claim!, output);
      expect((await executions.get(id)).steps[0]).toEqual(
        expect.objectContaining({ output }),
      );
    },
  );
});
