import request from 'supertest';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { createApp } from '../src/app.module';
import { ExecutionsRepository } from '../src/executions/executions.repository';
import { migrate } from '../scripts/migrate';
import { TestInfrastructure } from './support/infrastructure';

const definition = {
  steps: [{ id: 'work', type: 'transform', fields: {} }],
};

describe('workflow HTTP API', () => {
  let infra: TestInfrastructure;
  let app: NestExpressApplication;

  beforeAll(async () => {
    infra = await TestInfrastructure.create();
    await migrate(infra.db);
    app = await createApp(infra.config);
  });

  afterAll(async () => {
    await app?.close();
    await infra?.close();
  });

  test('publishes, reads, and launches a pinned workflow once', async () => {
    const server = app.getHttpServer();
    const published = await request(server)
      .post('/workflows/leads/versions')
      .send({ version: 1, definition })
      .expect(201);
    expect(published.body).toMatchObject({
      name: 'leads',
      version: 1,
      definition,
    });
    expect(typeof published.body.id).toBe('string');

    await request(server)
      .get('/workflows/leads/versions/1')
      .expect(200)
      .expect(({ body }) => expect(body).toEqual(published.body));

    const launched = await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'launch-leads-1')
      .send({ workflowName: 'leads', version: 1, input: { lead: 'Ada' } })
      .expect(201);
    expect(launched.body).toMatchObject({ created: true });
    expect(launched.body.id).toMatch(/^[0-9a-f-]{36}$/i);

    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'launch-leads-1')
      .send({ workflowName: 'leads', version: 1, input: { lead: 'Ada' } })
      .expect(200)
      .expect(({ body }) =>
        expect(body).toEqual({ id: launched.body.id, created: false }),
      );
  });

  test('preserves valid JSON keys and fingerprints the original content', async () => {
    const server = app.getHttpServer();
    const literal = { toString: 'kept', valueOf: 7, hasOwnProperty: true };
    const original = {
      steps: [
        { id: 'work', type: 'transform', fields: { value: { literal } } },
      ],
    };
    const published = await request(server)
      .post('/workflows/json_keys/versions')
      .send({ version: 1, definition: original })
      .expect(201);
    expect(published.body.definition).toEqual(original);
    const launched = await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'json-keys')
      .send({ workflowName: 'json_keys', version: 1, input: literal })
      .expect(201);
    const state = await request(server)
      .get(`/executions/${launched.body.id}`)
      .expect(200);
    expect(state.body.input).toEqual(literal);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'json-keys')
      .send({
        workflowName: 'json_keys',
        version: 1,
        input: { ...literal, toString: 'changed' },
      })
      .expect(409);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'unknown-method')
      .send({
        workflowName: 'json_keys',
        version: 1,
        input: {},
        toString: 'unknown',
      })
      .expect(400);
  });

  test('rejects invalid input before writing state', async () => {
    const server = app.getHttpServer();
    await request(server)
      .post('/workflows/leads/versions')
      .send({ version: 2, definition, unexpected: true })
      .expect(400);
    await request(server).get('/workflows/leads/versions/2').expect(404);

    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'null-input')
      .send({ workflowName: 'leads', version: 1, input: null })
      .expect(201);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'bad-input')
      .send({ workflowName: 'leads', version: 1, input: undefined })
      .expect(400);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'unknown-field')
      .send({ workflowName: 'leads', version: 1, input: {}, unexpected: true })
      .expect(400);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'changed-payload')
      .send({ workflowName: 'leads', version: 1, input: { a: 1 } })
      .expect(201);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'changed-payload')
      .send({ workflowName: 'leads', version: 1, input: { a: 2 } })
      .expect(409);

    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'dangerous-root')
      .set('Content-Type', 'application/json')
      .send(
        '{"workflowName":"leads","version":1,"input":{},"constructor":"hidden"}',
      )
      .expect(400);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'dangerous-input')
      .set('Content-Type', 'application/json')
      .send(
        '{"workflowName":"leads","version":1,"input":{"constructor":"x","__proto__":{"x":1},"items":[{"constructor":"x"}]}}',
      )
      .expect(400);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'dangerous-input')
      .send({ workflowName: 'leads', version: 1, input: { safe: true } })
      .expect(201);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'bad-unicode')
      .send({ workflowName: 'leads', version: 1, input: '\u0000' })
      .expect(400);
    await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'bad-surrogate')
      .send({ workflowName: 'leads', version: 1, input: '\ud800' })
      .expect(400);

    await request(server)
      .post('/workflows/leads/versions')
      .set('Content-Type', 'application/json')
      .send(
        '{"version":4,"definition":{"steps":[{"id":"work","type":"transform","fields":{"value":{"literal":{"constructor":"x","__proto__":{"x":1}}}}}]}}',
      )
      .expect(400);
    await request(server)
      .post('/workflows/leads/versions')
      .send({ version: 4, definition })
      .expect(201);
    await request(server)
      .post('/workflows/leads/versions')
      .send({
        version: 5,
        definition: {
          steps: [
            {
              id: 'work',
              type: 'transform',
              fields: { value: { literal: '\u0000' } },
            },
          ],
        },
      })
      .expect(400);
  });

  test('returns snapshots, events, and control state through public routes', async () => {
    const server = app.getHttpServer();
    const launch = await request(server)
      .post('/executions')
      .set('Idempotency-Key', 'controls')
      .send({ workflowName: 'leads', version: 1, input: {} })
      .expect(201);
    const id = launch.body.id as string;

    await request(server)
      .get(`/executions/${id}`)
      .expect(200)
      .expect(({ body }) => {
        expect(body).toMatchObject({
          id,
          workflowName: 'leads',
          version: 1,
          status: 'running',
        });
        expect(body.steps).toEqual([
          expect.objectContaining({ id: 'work', status: 'ready' }),
        ]);
      });
    await request(server)
      .get(`/executions/${id}/events?after=0&limit=1`)
      .expect(200)
      .expect(({ body }) => {
        expect(Array.isArray(body)).toBe(true);
        expect(body).toHaveLength(1);
      });
    await request(server)
      .post(`/executions/${id}/pause`)
      .expect(200)
      .expect(({ body }) => expect(body.status).toBe('paused'));
    await request(server)
      .post(`/executions/${id}/resume`)
      .expect(200)
      .expect(({ body }) => expect(body.status).toBe('running'));

    const repo = app.get(ExecutionsRepository);
    const claim = await repo.claim(id, 'work');
    expect(claim).not.toBeNull();
    await repo.fail(claim!, { kind: 'invalid_data', message: 'failed' });
    await request(server).post(`/executions/${id}/retry`).send({}).expect(200);
  });

  test('maps invalid routes and errors safely with correlation ids', async () => {
    const server = app.getHttpServer();
    await request(server).get('/executions/not-a-uuid').expect(400);
    await request(server).get('/workflows/missing/versions/1').expect(404);
    const failure = await request(server)
      .post('/executions')
      .set('X-Request-Id', 'request_123')
      .set('Idempotency-Key', 'missing-workflow')
      .send({ workflowName: 'missing', version: 1, input: {} })
      .expect(404);
    expect(failure.headers['x-request-id']).toBe('request_123');
    expect(failure.headers['x-powered-by']).toBeUndefined();
    expect(failure.body).toEqual({
      code: expect.any(String),
      message: expect.any(String),
      requestId: 'request_123',
    });
    expect(JSON.stringify(failure.body)).not.toMatch(/stack|Error:/i);

    await request(server)
      .post('/workflows/leads/versions')
      .set('Content-Type', 'application/json')
      .send(
        JSON.stringify({
          version: 3,
          definition,
          padding: 'x'.repeat(300_000),
        }),
      )
      .expect(413)
      .expect(({ headers, body }) => {
        expect(headers['x-request-id']).toBeDefined();
        expect(body).toEqual({
          code: expect.any(String),
          message: expect.any(String),
          requestId: expect.any(String),
        });
      });
    await request(server)
      .post('/workflows/leads/versions')
      .set('Content-Type', 'application/json')
      .set('X-Request-Id', 'malformed_json')
      .send('{')
      .expect(400)
      .expect(({ headers, body }) => {
        expect(headers['x-request-id']).toBe('malformed_json');
        expect(body).toEqual({
          code: expect.any(String),
          message: expect.any(String),
          requestId: 'malformed_json',
        });
      });
    await request(server)
      .get(
        '/executions/00000000-0000-4000-8000-000000000000/events?after=9223372036854775808',
      )
      .expect(400);
  });
});
