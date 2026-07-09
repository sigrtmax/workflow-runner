import { migrate } from '../scripts/migrate';
import {
  ExecutionConflict,
  ExecutionsRepository,
} from '../src/executions/executions.repository';
import { WorkflowsRepository } from '../src/workflows/workflows.repository';
import { validateDefinition } from '../src/workflows/definition';
import { TestInfrastructure } from './support/infrastructure';

describe('execution controls', () => {
  let infra: TestInfrastructure;
  let executions: ExecutionsRepository;
  beforeAll(async () => {
    infra = await TestInfrastructure.create();
    await migrate(infra.db);
    executions = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 2,
    });
  });
  afterAll(async () => infra?.close());

  test('pause prevents claims and resume restores readiness', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'controls',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'pause-key', {});
    await executions.pause(id);
    expect(await executions.claim(id, 'work')).toBeNull();
    await executions.resume(id);
    const claim = await executions.claim(id, 'work');
    expect(claim).not.toBeNull();
    await executions.succeed(claim!, {});
  });

  test('manual retry keeps a successful peer and requires confirmation for unsafe unknown outcome', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'retry',
      1,
      validateDefinition({
        steps: [
          { id: 'good', type: 'transform', fields: {} },
          {
            id: 'send',
            type: 'http',
            integration: 'crm',
            path: '/x',
            method: 'POST',
            expectedStatuses: [201],
            timeoutMs: 100,
            retry: { maxAttempts: 1, backoffMs: 1, maxBackoffMs: 1 },
            safety: 'unsafe',
          },
        ],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'retry-key', {});
    const good = await executions.claim(id, 'good');
    const send = await executions.claim(id, 'send');
    await executions.succeed(good!, { preserved: true });
    await executions.fail(send!, {
      kind: 'unknown_outcome',
      message: 'request outcome unknown',
    });
    await expect(executions.retry(id)).rejects.toBeInstanceOf(
      ExecutionConflict,
    );
    await executions.retry(id, true);
    expect((await executions.get(id)).steps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 'good',
          status: 'succeeded',
          output: { preserved: true },
        }),
        expect.objectContaining({ id: 'send', status: 'ready' }),
      ]),
    );
  });

  test('requires confirmation after an unsafe unexpected successful HTTP status', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'unsafe_unexpected_success',
      1,
      validateDefinition({
        steps: [
          {
            id: 'send',
            type: 'http',
            integration: 'crm',
            path: '/x',
            method: 'POST',
            expectedStatuses: [201],
            timeoutMs: 100,
            retry: { maxAttempts: 1, backoffMs: 1, maxBackoffMs: 1 },
            safety: 'unsafe',
          },
        ],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'unsafe-201-key', {});
    const claim = await executions.claim(id, 'send');
    await executions.fail(claim!, {
      kind: 'permanent_http',
      message: 'unexpected 200',
      status: 200,
    });
    await expect(executions.retry(id)).rejects.toBeInstanceOf(
      ExecutionConflict,
    );
    await expect(executions.retry(id, true)).resolves.toBeUndefined();
  });
});
