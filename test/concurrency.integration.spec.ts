import { migrate } from '../scripts/migrate';
import { ExecutionsRepository } from '../src/executions/executions.repository';
import { WorkflowsRepository } from '../src/workflows/workflows.repository';
import { validateDefinition } from '../src/workflows/definition';
import { TestInfrastructure, eventually } from './support/infrastructure';

describe('claims and leases', () => {
  let infra: TestInfrastructure;
  let executions: ExecutionsRepository;
  beforeAll(async () => {
    infra = await TestInfrastructure.create();
    await migrate(infra.db);
    executions = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 1,
    });
  });
  afterAll(async () => infra?.close());

  test('allows one concurrent claim and fences a stale completion', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'claims',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'claim-key', {});
    const claims = await Promise.all(
      Array.from({ length: 20 }, () => executions.claim(id, 'work')),
    );
    const claim = claims.find(Boolean)!;
    expect(claims.filter(Boolean)).toHaveLength(1);
    await infra.db.query(
      "UPDATE step_executions SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id=$1",
      [id],
    );
    expect(await executions.succeed(claim, { stale: true })).toBe(false);
    await executions.tick();
    const recovered = await executions.claim(id, 'work');
    expect(recovered).not.toBeNull();
    expect(await executions.heartbeat(claim)).toBe(false);
    expect(await executions.succeed(claim, { stale: true })).toBe(false);
    expect(await executions.succeed(recovered!, { fresh: true })).toBe(true);
    expect((await executions.get(id)).steps[0]).toEqual(
      expect.objectContaining({ status: 'succeeded', output: { fresh: true } }),
    );
  });

  test('enforces one global lease across executions and repository instances', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'global_limit',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const first = await executions.launch(workflow, {}, 'global-one', {});
    const second = await executions.launch(workflow, {}, 'global-two', {});
    const otherRepository = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 1,
    });
    const claims = await Promise.all([
      executions.claim(first.id, 'work'),
      otherRepository.claim(second.id, 'work'),
    ]);
    expect(claims.filter(Boolean)).toHaveLength(1);
    const winner = claims.find(Boolean)!;
    await executions.succeed(winner, {});
    const remaining = await Promise.all([
      executions.claim(first.id, 'work'),
      otherRepository.claim(second.id, 'work'),
    ]);
    const finalClaim = remaining.find(Boolean)!;
    await executions.succeed(finalClaim, {});
  });

  test('records simultaneous branch results and opens a join once', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'join_once',
      1,
      validateDefinition({
        steps: [
          { id: 'left', type: 'transform', fields: {} },
          { id: 'right', type: 'transform', fields: {} },
          {
            id: 'join',
            type: 'transform',
            fields: {},
            needs: [{ step: 'left' }, { step: 'right' }],
          },
        ],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'join-key', {});
    const highLimitRepository = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 2,
    });
    const left = await highLimitRepository.claim(id, 'left');
    const right = await highLimitRepository.claim(id, 'right');
    await Promise.all([
      highLimitRepository.succeed(left!, { side: 'left' }),
      highLimitRepository.succeed(right!, { side: 'right' }),
    ]);
    expect((await executions.get(id)).steps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: 'join', status: 'ready' }),
      ]),
    );
    expect(
      (await executions.events(id)).filter(
        (entry) => entry.stepId === 'join' && entry.kind === 'ready',
      ),
    ).toHaveLength(1);
  });

  test('serializes two scheduler ticks before they mutate a delay', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'two_ticks',
      1,
      validateDefinition({
        steps: [{ id: 'wait', type: 'delay', durationMs: 60_000 }],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'two-ticks-key', {});
    const schedulerA = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 1,
    });
    const schedulerB = new ExecutionsRepository(infra.db, {
      leaseMs: 1_000,
      concurrency: 1,
    });
    await Promise.all([schedulerA.tick(), schedulerB.tick()]);
    expect(
      (await executions.events(id)).filter(
        (entry) => entry.stepId === 'wait' && entry.kind === 'waiting',
      ),
    ).toHaveLength(1);
  });

  test('a failed journal write rolls back the result, attempt, and descendant readiness', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'result_atomicity',
      1,
      validateDefinition({
        steps: [
          { id: 'work', type: 'transform', fields: {} },
          {
            id: 'child',
            type: 'transform',
            fields: {},
            needs: [{ step: 'work' }],
          },
        ],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'atomic-result', {});
    const claim = (await executions.claim(id, 'work'))!;
    await infra.db
      .query(`CREATE FUNCTION fail_completion_event() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.kind='succeeded' THEN RAISE EXCEPTION 'injected journal failure'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER completion_barrier BEFORE INSERT ON execution_events
      FOR EACH ROW EXECUTE FUNCTION fail_completion_event()`);
    try {
      await expect(
        executions.succeed(claim, { accepted: true }),
      ).rejects.toThrow('injected journal failure');
      expect((await executions.get(id)).steps).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            id: 'work',
            status: 'running',
            output: null,
          }),
          expect.objectContaining({ id: 'child', status: 'pending' }),
        ]),
      );
      expect(
        (
          await infra.db.query(
            'SELECT status,finished_at FROM step_attempts WHERE execution_id=$1',
            [id],
          )
        ).rows,
      ).toEqual([{ status: 'running', finished_at: null }]);
      expect(
        (await executions.events(id)).some(
          (event) => event.kind === 'succeeded',
        ),
      ).toBe(false);
    } finally {
      await infra.db.query(
        'DROP TRIGGER completion_barrier ON execution_events; DROP FUNCTION fail_completion_event()',
      );
    }
    expect(await executions.succeed(claim, { accepted: true })).toBe(true);
    expect(
      (await executions.get(id)).steps.find((step) => step.id === 'child')
        ?.status,
    ).toBe('ready');
  });

  test('an uncommitted heartbeat cannot be ignored by another admission', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'renewal_race',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const repo = new ExecutionsRepository(infra.db, {
      leaseMs: 60000,
      concurrency: 1,
    });
    const first = await repo.launch(workflow, {}, 'renewal-first', {});
    const second = await repo.launch(workflow, {}, 'renewal-second', {});
    const claim = (await repo.claim(first.id, 'work'))!;
    await infra.db
      .query(`CREATE FUNCTION hold_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF OLD.status='running' AND NEW.status='running' AND NEW.lease_until>OLD.lease_until
      THEN PERFORM pg_advisory_xact_lock(173845); END IF; RETURN NEW; END $$;
      CREATE TRIGGER heartbeat_barrier AFTER UPDATE ON step_executions
      FOR EACH ROW EXECUTE FUNCTION hold_heartbeat()`);
    const gate = await infra.db.pool.connect();
    let renewal: Promise<boolean> | undefined;
    let admission: ReturnType<ExecutionsRepository['claim']> | undefined;
    try {
      await gate.query('SELECT pg_advisory_lock(173845)');
      await infra.db.query(
        "UPDATE step_executions SET lease_until=clock_timestamp()+interval '1 second' WHERE execution_id=$1",
        [first.id],
      );
      renewal = repo.heartbeat(claim);
      await eventually(
        async () =>
          (
            await infra.db.query<{ n: number }>(
              "SELECT count(*)::int AS n FROM pg_locks WHERE locktype='advisory' AND objid=173845 AND NOT granted",
            )
          ).rows[0]!.n,
        (n) => n === 1,
      );
      await eventually(
        async () =>
          (
            await infra.db.query<{ expired: boolean }>(
              'SELECT lease_until<=clock_timestamp() AS expired FROM step_executions WHERE execution_id=$1',
              [first.id],
            )
          ).rows[0]!.expired,
        Boolean,
      );
      let admissionFinished = false;
      admission = repo.claim(second.id, 'work').then((result) => {
        admissionFinished = true;
        return result;
      });
      // Observe either lock contention or the broken early admission; no timing guess.
      await eventually(
        async () =>
          admissionFinished ||
          (
            await infra.db.query<{ n: number }>(
              "SELECT count(*)::int AS n FROM pg_locks WHERE locktype='advisory' AND objid=72849302 AND NOT granted",
            )
          ).rows[0]!.n > 0,
        Boolean,
      );
      await gate.query('SELECT pg_advisory_unlock(173845)');
      expect(await renewal).toBe(true);
      expect(await admission).toBeNull();
      const active = await infra.db.query<{ n: number }>(
        "SELECT count(*)::int AS n FROM step_executions WHERE status='running' AND lease_until>clock_timestamp()",
      );
      expect(active.rows[0]!.n).toBe(1);
    } finally {
      await gate.query('SELECT pg_advisory_unlock_all()');
      gate.release();
      await Promise.allSettled([renewal, admission]);
      await infra.db.query(
        'DROP TRIGGER heartbeat_barrier ON step_executions; DROP FUNCTION hold_heartbeat()',
      );
    }
  });

  test('cursor pagination returns every event once in numeric order', async () => {
    const workflow = await new WorkflowsRepository(infra.db).publish(
      'journal_pages',
      1,
      validateDefinition({
        steps: [{ id: 'work', type: 'transform', fields: {} }],
      }),
    );
    const { id } = await executions.launch(workflow, {}, 'journal-pages', {});
    await infra.db.query(
      "INSERT INTO execution_events (execution_id,kind,detail) SELECT $1,'test','{}'::jsonb FROM generate_series(1,110)",
      [id],
    );
    const expected = (
      await infra.db.query<{ id: string }>(
        'SELECT id::text FROM execution_events e WHERE execution_id=$1 ORDER BY e.id',
        [id],
      )
    ).rows.map((row) => row.id);
    const actual: string[] = [];
    let after = '0';
    for (;;) {
      const page = await executions.events(id, after, 2);
      if (!page.length) break;
      actual.push(...page.map((entry) => entry.id));
      after = page.at(-1)!.id;
    }
    expect(actual).toEqual(expected);
  });
});
