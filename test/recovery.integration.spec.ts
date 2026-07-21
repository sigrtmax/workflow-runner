import { readFile } from 'node:fs/promises';
import { Queue } from 'bullmq';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { createApp } from '../src/app.module';
import { migrate } from '../scripts/migrate';
import { ExecutionsRepository } from '../src/executions/executions.repository';
import type { HttpStep } from '../src/workflows/definition';
import { TestInfrastructure, eventually } from './support/infrastructure';
import { MockIntegrations } from './support/mock-integrations';
import { WorkerProcess } from './support/worker-process';

const http = (id: string, path = `/${id}`): HttpStep => ({
  id,
  type: 'http',
  integration: 'crm',
  path,
  method: 'POST',
  body: { ref: 'input' },
  expectedStatuses: [201],
  timeoutMs: 10000,
  retry: { maxAttempts: 3, backoffMs: 30, maxBackoffMs: 100 },
  safety: 'idempotency-key',
});

describe('worker recovery with PostgreSQL, Redis and real processes', () => {
  let infra: TestInfrastructure;
  let app: NestExpressApplication;
  let receiver: MockIntegrations;
  let repo: ExecutionsRepository;
  let workers: WorkerProcess[];
  let queue: Queue;

  beforeEach(async () => {
    infra = await TestInfrastructure.create();
    await migrate(infra.db);
    receiver = new MockIntegrations();
    const url = await receiver.listen();
    infra.config.INTEGRATIONS = { crm: { url }, erp: { url } };
    app = await createApp(infra.config);
    repo = app.get(ExecutionsRepository);
    workers = [];
    const redis = new URL(infra.config.REDIS_URL);
    queue = new Queue('workflow-steps', {
      prefix: infra.config.QUEUE_PREFIX,
      connection: { host: redis.hostname, port: Number(redis.port) },
    });
  });
  afterEach(async () => {
    await Promise.all(workers.map((worker) => worker.stop()));
    if (queue) {
      try {
        await queue.obliterate({ force: true });
      } finally {
        await queue.close();
      }
    }
    await app?.close();
    await receiver?.close();
    await infra?.close();
  });
  async function start() {
    const worker = await WorkerProcess.start(infra.config);
    workers.push(worker);
    return worker;
  }
  async function launch(steps: unknown[], input: unknown = {}) {
    const server = app.getHttpServer();
    await request(server)
      .post('/workflows/leads/versions')
      .send({ version: 1, definition: { steps } })
      .expect(201);
    const response = await request(server)
      .post('/executions')
      .set('Idempotency-Key', randomUUID())
      .send({ workflowName: 'leads', version: 1, input })
      .expect(201);
    return response.body.id as string;
  }

  test('recovers an accepted HTTP request after SIGKILL with the same key and trace', async () => {
    receiver.hold('/crm');
    const id = await launch(
      [
        {
          id: 'normalize',
          type: 'transform',
          fields: {
            email: {
              op: 'lowercase',
              value: { op: 'trim', value: { ref: 'input.email' } },
            },
          },
        },
        {
          ...http('crm'),
          needs: [{ step: 'normalize' }],
          body: { ref: 'steps.normalize' },
        },
        {
          id: 'accepted',
          type: 'condition',
          needs: [{ step: 'crm' }],
          operator: 'eq',
          left: { ref: 'steps.crm.status' },
          right: { literal: 201 },
        },
        {
          ...http('erp'),
          integration: 'erp',
          needs: [{ step: 'accepted', when: true }],
        },
        {
          id: 'discarded',
          type: 'transform',
          needs: [{ step: 'accepted', when: false }],
          fields: {},
        },
      ],
      { email: ' SAMPLE@EXAMPLE.TEST ' },
    );
    const first = await start();
    await eventually(
      async () => receiver.calls.filter((call) => call.path === '/crm').length,
      (count) => count === 1,
    );
    expect(receiver.effects.size).toBe(1);
    await first.stop('SIGKILL');
    receiver.release('/crm');
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(snapshot.steps.find((step) => step.id === 'crm')?.attemptCount).toBe(
      2,
    );
    expect(
      snapshot.steps.find((step) => step.id === 'normalize')?.attemptCount,
    ).toBe(1);
    expect(snapshot.steps.find((step) => step.id === 'discarded')?.status).toBe(
      'skipped',
    );
    const crmCalls = receiver.calls.filter((call) => call.path === '/crm');
    expect(crmCalls).toHaveLength(2);
    expect(crmCalls[0]!.key).toBe(crmCalls[1]!.key);
    expect(crmCalls[0]!.body).toEqual({ email: 'sample@example.test' });
    expect(crmCalls[1]!.body).toEqual(crmCalls[0]!.body);
    expect(receiver.effects.size).toBe(2); // CRM and ERP each accepted once.
    const traceIds = receiver.calls.map(
      (call) => call.traceparent.split('-')[1],
    );
    expect(traceIds[0]).toMatch(/^[0-9a-f]{32}$/);
    expect(new Set(traceIds).size).toBe(1);
    const stored = await infra.db.query<{
      trace_context: { traceparent: string };
    }>('SELECT trace_context FROM workflow_executions WHERE id=$1', [id]);
    expect(stored.rows[0]!.trace_context.traceparent.split('-')[1]).toBe(
      traceIds[0],
    );
    const attempts = await infra.db.query<{ status: string }>(
      'SELECT status FROM step_attempts WHERE execution_id=$1 AND step_id=$2 ORDER BY attempt',
      [id, 'crm'],
    );
    expect(attempts.rows.map((row) => row.status)).toEqual([
      'failed',
      'succeeded',
    ]);
  });

  test('pause accepts an in-flight result and blocks its successor until resume', async () => {
    receiver.hold('/first');
    const id = await launch([
      http('first'),
      { ...http('second'), needs: [{ step: 'first' }] },
    ]);
    await start();
    await eventually(
      async () => receiver.calls.length,
      (count) => count === 1,
    );
    await request(app.getHttpServer())
      .post(`/executions/${id}/pause`)
      .expect(200);
    receiver.release('/first');
    await eventually(
      () => repo.get(id),
      (value) =>
        value.steps.find((step) => step.id === 'first')?.status === 'succeeded',
    );
    expect((await repo.get(id)).status).toBe('paused');
    expect(await repo.claim(id, 'second')).toBeNull();
    expect(receiver.calls).toHaveLength(1);
    await request(app.getHttpServer())
      .post(`/executions/${id}/resume`)
      .expect(200);
    await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(receiver.calls.map((call) => call.path)).toEqual([
      '/first',
      '/second',
    ]);
  });

  test('two worker processes respect the shared limit and release a join once', async () => {
    receiver.hold('/branch');
    const id = await launch([
      http('one', '/branch'),
      http('two', '/branch'),
      http('three', '/branch'),
      {
        ...http('join'),
        needs: [{ step: 'one' }, { step: 'two' }, { step: 'three' }],
      },
    ]);
    await Promise.all([start(), start()]);
    await eventually(
      async () => receiver.calls.length,
      (count) => count === 2,
    );
    expect(await repo.claim(id, 'three')).toBeNull();
    receiver.release('/branch');
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(receiver.maxActive).toBeLessThanOrEqual(2);
    expect(receiver.calls.filter((call) => call.path === '/join')).toHaveLength(
      1,
    );
    expect(snapshot.steps.every((step) => step.attemptCount === 1)).toBe(true);
  });

  test('a persisted delay survives process restart without resetting its deadline', async () => {
    const id = await launch([
      { id: 'wait', type: 'delay', durationMs: 60000 },
      { ...http('after'), needs: [{ step: 'wait' }] },
    ]);
    const first = await start();
    await eventually(
      () => repo.get(id),
      (value) =>
        value.steps.find((step) => step.id === 'wait')?.status === 'waiting',
    );
    await first.stop('SIGKILL');
    // Advance only the persisted deadline, without a minute-long wall-clock sleep.
    await infra.db.query(
      "UPDATE step_executions SET due_at=clock_timestamp()-interval '1 second' WHERE execution_id=$1 AND step_id='wait'",
      [id],
    );
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(snapshot.steps.every((step) => step.attemptCount === 1)).toBe(true);
    expect(receiver.calls).toHaveLength(1);
  });

  test('manual retry repeats a failed HTTP step and preserves a successful predecessor', async () => {
    receiver.statuses.set('/second', 400);
    const id = await launch([
      http('first'),
      { ...http('second'), needs: [{ step: 'first' }] },
    ]);
    await start();
    await eventually(
      () => repo.get(id),
      (value) => value.status === 'failed',
    );
    receiver.statuses.delete('/second');
    await request(app.getHttpServer())
      .post(`/executions/${id}/retry`)
      .send({})
      .expect(200);
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(
      snapshot.steps.find((step) => step.id === 'first')?.attemptCount,
    ).toBe(1);
    expect(
      snapshot.steps.find((step) => step.id === 'second')?.attemptCount,
    ).toBe(2);
    expect(
      receiver.calls.filter((call) => call.path === '/first'),
    ).toHaveLength(1);
  });
  test('rebuilds lost queue notifications from a committed ready step', async () => {
    const id = await launch([http('crm')]);
    await queue.add(
      'step',
      { executionId: id, stepId: 'crm' },
      { jobId: `${id}-crm-0` },
    );
    expect(await queue.getWaitingCount()).toBe(1);
    await queue.obliterate({ force: true });
    expect(await queue.getWaitingCount()).toBe(0);
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(snapshot.steps[0]!.attemptCount).toBe(1);
    expect(receiver.calls).toHaveLength(1);
  });

  test.each([
    { value: '\ud800' },
    { ['\u0000']: 'value' },
    { value: 'x'.repeat(1024 * 1024) },
  ])(
    'an unsafe accepted response incompatible with JSONB requires operator confirmation',
    async (body) => {
      receiver.bodies.set('/crm', body);
      const id = await launch([{ ...http('crm'), safety: 'unsafe' }]);
      await start();
      const snapshot = await eventually(
        () => repo.get(id),
        (value) => value.status === 'failed',
      );
      expect(snapshot.steps[0]).toMatchObject({
        attemptCount: 1,
        error: { kind: 'permanent_http', status: 201 },
      });
      await request(app.getHttpServer())
        .post(`/executions/${id}/retry`)
        .send({})
        .expect(409);
      expect(receiver.calls).toHaveLength(1);
    },
  );

  test('exhausts a bounded HTTP retry budget after repeated server errors', async () => {
    receiver.statuses.set('/crm', 503);
    const id = await launch([http('crm')]);
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'failed',
    );
    expect(snapshot.steps[0]).toMatchObject({
      attemptCount: 3,
      error: { kind: 'transient_http', status: 503 },
    });
    expect(receiver.calls).toHaveLength(3);
    expect(new Set(receiver.calls.map((call) => call.key)).size).toBe(1);
    expect(
      (await repo.events(id)).filter(
        (event) => event.kind === 'retry_scheduled',
      ),
    ).toHaveLength(2);
  });

  test('times out an unsafe request without automatically repeating its side effect', async () => {
    receiver.hold('/crm');
    const id = await launch([
      { ...http('crm'), safety: 'unsafe', timeoutMs: 100 },
    ]);
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'failed',
    );
    expect(snapshot.steps[0]).toMatchObject({
      attemptCount: 1,
      error: { kind: 'unknown_outcome' },
    });
    expect(receiver.calls).toHaveLength(1);
    expect(receiver.effects.size).toBe(1);
  });

  test('runs the documented CRM-to-ERP example including delay and parallel receipt', async () => {
    const example = JSON.parse(
      await readFile('examples/lead-to-erp.json', 'utf8'),
    ) as { definition: { steps: unknown[] } };
    const id = await launch(example.definition.steps, {
      email: ' SAMPLE@EXAMPLE.TEST ',
      amount: '1250',
    });
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(receiver.calls.map((call) => call.path)).toEqual([
      '/leads',
      '/orders',
    ]);
    expect(receiver.calls.map((call) => call.body)).toEqual([
      { email: 'sample@example.test', amount: 1250 },
      { email: 'sample@example.test', amount: 1250 },
    ]);
    expect(snapshot.steps.find((step) => step.id === 'complete')?.status).toBe(
      'succeeded',
    );
  });
  test('SIGTERM drains the worker and recovery continues an interrupted safe request', async () => {
    receiver.hold('/crm');
    const id = await launch([http('crm')]);
    const first = await start();
    await eventually(
      async () => receiver.calls.length,
      (count) => count === 1,
    );
    await first.stop('SIGTERM');
    expect(receiver.calls).toHaveLength(1);
    receiver.release('/crm');
    await start();
    const snapshot = await eventually(
      () => repo.get(id),
      (value) => value.status === 'succeeded',
    );
    expect(snapshot.steps[0]!.attemptCount).toBe(2);
    expect(receiver.effects.size).toBe(1);
  });
});
