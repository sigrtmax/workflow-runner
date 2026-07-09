import { migrate } from '../scripts/migrate';
import { ExecutionsRepository } from '../src/executions/executions.repository';
import { WorkflowsRepository } from '../src/workflows/workflows.repository';
import { validateDefinition } from '../src/workflows/definition';
import { TestInfrastructure } from './support/infrastructure';

describe('persisted delays', () => {
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

  test('keeps a paused delay deadline and completes it only after resume', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'delays',
      1,
      validateDefinition({
        steps: [{ id: 'wait', type: 'delay', durationMs: 60_000 }],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'delay-key', {});
    await executions.tick();
    const due = (
      await infra.db.query<{ due_at: Date }>(
        'SELECT due_at FROM step_executions WHERE execution_id=$1',
        [id],
      )
    ).rows[0]!.due_at;
    await executions.pause(id);
    await infra.db.query(
      "UPDATE step_executions SET due_at=clock_timestamp()-interval '1 second' WHERE execution_id=$1",
      [id],
    );
    await executions.tick();
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({ status: 'waiting' }),
    );
    await executions.resume(id);
    const completed = await executions.tick();
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({ status: 'succeeded' }),
    );
    expect(due).toBeInstanceOf(Date);
    expect(completed).toEqual([
      expect.objectContaining({
        executionId: id,
        stepId: 'wait',
        workflowName: 'delays',
        version: 1,
        attempt: 1,
        startedAt: expect.any(Date),
      }),
    ]);
  });

  test('persists safe HTTP backoff then fails after its retry budget', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'http_backoff',
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
            retry: { maxAttempts: 2, backoffMs: 60_000, maxBackoffMs: 60_000 },
            safety: 'idempotency-key',
          },
        ],
      }),
    );
    const { id } = await executions.launch(
      workflow,
      {},
      'http-backoff-key',
      {},
    );
    const first = await executions.claim(id, 'send');
    await executions.fail(first!, {
      kind: 'transient_http',
      message: 'upstream returned 500',
      status: 500,
    });
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({ status: 'waiting' }),
    );
    await infra.db.query(
      "UPDATE step_executions SET due_at=clock_timestamp()-interval '1 second' WHERE execution_id=$1",
      [id],
    );
    await executions.tick();
    const second = await executions.claim(id, 'send');
    await executions.fail(second!, {
      kind: 'transient_http',
      message: 'upstream returned 500',
      status: 500,
    });
    expect((await executions.get(id)).status).toBe('failed');
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({ status: 'failed', attemptCount: 2 }),
    );
  });

  test('recovers an expired safe HTTP lease through persisted transport backoff', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'expired_safe_http',
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
            retry: { maxAttempts: 2, backoffMs: 60_000, maxBackoffMs: 60_000 },
            safety: 'idempotency-key',
          },
        ],
      }),
    );
    const { id } = await executions.launch(
      workflow,
      {},
      'expired-safe-http-key',
      {},
    );
    await executions.claim(id, 'send');
    await infra.db.query(
      "UPDATE step_executions SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id=$1",
      [id],
    );
    await executions.tick();
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({
        status: 'waiting',
        error: expect.objectContaining({ kind: 'transport' }),
      }),
    );
    await infra.db.query(
      "UPDATE step_executions SET due_at=clock_timestamp()-interval '1 second' WHERE execution_id=$1",
      [id],
    );
    await executions.tick();
    expect(await executions.claim(id, 'send')).not.toBeNull();
  });

  test('does not treat an application internal failure as a recoverable pure crash', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'pure_application_failure',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const { id } = await executions.launch(
      workflow,
      {},
      'pure-application-failure-key',
      {},
    );
    const claim = await executions.claim(id, 'work');
    await executions.fail(claim!, {
      kind: 'internal',
      message: 'bad transform',
    });
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({ status: 'failed' }),
    );
  });

  test('does not let more than one hundred sleeping delays hide ready work', async () => {
    const workflows = new WorkflowsRepository(infra.db);
    const delayWorkflow = await workflows.publish(
      'scan_sleeping',
      1,
      validateDefinition({
        steps: [{ id: 'wait', type: 'delay', durationMs: 60_000 }],
      }),
    );
    await Promise.all(
      Array.from({ length: 101 }, (_, index) =>
        executions.launch(delayWorkflow, {}, `scan-sleeper-${index}`, {}),
      ),
    );
    await executions.tick();
    const workWorkflow = await workflows.publish(
      'scan_ready',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const { id } = await executions.launch(
      workWorkflow,
      {},
      'scan-fairness-key',
      {},
    );
    await executions.tick();
    expect(await executions.claim(id, 'work')).not.toBeNull();
  });
});
