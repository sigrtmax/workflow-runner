import { createHash, randomUUID } from 'node:crypto';
import type { PoolClient, QueryResultRow } from 'pg';
import { Database } from '../database';
import type { Json, Definition, Step } from '../workflows/definition';
import type { PublishedWorkflow } from '../workflows/workflows.repository';
import { readyTransitions, type StepStatus } from '../worker/graph';
import { retryDelay, type StepFailure } from '../worker/retry-policy';

export class ExecutionNotFound extends Error {
  constructor() {
    super('Execution was not found');
    this.name = 'ExecutionNotFound';
  }
}
export class ExecutionConflict extends Error {
  constructor(message = 'Execution transition conflicts with current state') {
    super(message);
    this.name = 'ExecutionConflict';
  }
}
export interface Claim {
  executionId: string;
  step: Step;
  token: string;
  attempt: number;
  cycleAttempt: number;
  input: Json;
  outputs: Record<string, Json>;
  workflowName: string;
  version: number;
  traceContext: Record<string, string>;
}
export interface ExecutionSnapshot {
  id: string;
  workflowName: string;
  version: number;
  status: 'running' | 'paused' | 'failed' | 'succeeded';
  input: Json;
  createdAt: Date;
  finishedAt: Date | null;
  steps: Array<{
    id: string;
    status: StepStatus;
    output: Json | null;
    attemptCount: number;
    error: StepFailure | null;
  }>;
}
export interface DelayCompletion {
  executionId: string;
  stepId: string;
  workflowName: string;
  version: number;
  traceContext: Record<string, string>;
  attempt: number;
  startedAt: Date;
}

interface ExecutionRow extends QueryResultRow {
  id: string;
  status: ExecutionSnapshot['status'];
  input: Json;
  trace_context: Record<string, string>;
  name: string;
  version: number;
  definition: Definition;
}
interface StepRow extends QueryResultRow {
  step_id: string;
  status: StepStatus;
  output: Json | null;
  attempt_count: number;
  cycle_attempt: number;
  last_failure: StepFailure | null;
  lease_token: string | null;
}
interface TickStepRow extends StepRow {
  lease_expired: boolean;
  due: boolean;
  due_at: Date | null;
}
const lock = 72849302;
const schedulerLock = 72849303;
const stable = (value: Json): string => {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stable(value[key]!)}`)
    .join(',')}}`;
};
const fingerprint = (workflow: PublishedWorkflow, input: Json) =>
  createHash('sha256')
    .update(
      stable({
        id: workflow.id,
        name: workflow.name,
        version: workflow.version,
        input,
      }),
    )
    .digest('hex');
const json = (value: Json | Record<string, string> | StepFailure): string =>
  JSON.stringify(value);
const event = (
  client: PoolClient,
  executionId: string,
  stepId: string | null,
  kind: string,
  detail: Json,
): Promise<unknown> =>
  client.query(
    'INSERT INTO execution_events (execution_id, step_id, kind, detail) VALUES ($1,$2,$3,$4)',
    [executionId, stepId, kind, json(detail)],
  );

export class ExecutionsRepository {
  constructor(
    private readonly db: Database,
    private readonly options: { leaseMs: number; concurrency: number },
  ) {}

  async launch(
    workflow: PublishedWorkflow,
    input: Json,
    key: string,
    traceContext: Record<string, string>,
    executionId = randomUUID(),
  ): Promise<{ id: string; created: boolean }> {
    const hash = fingerprint(workflow, input);
    return this.db.transaction(async (client) => {
      const inserted = await client.query<{ id: string }>(
        'INSERT INTO workflow_executions (id, definition_id, status, input, idempotency_key, fingerprint, trace_context) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id',
        [
          executionId,
          workflow.id,
          'running',
          json(input),
          key,
          hash,
          json(traceContext),
        ],
      );
      if (!inserted.rowCount) {
        const existing = await client.query<{
          id: string;
          fingerprint: string;
        }>(
          'SELECT id, fingerprint FROM workflow_executions WHERE idempotency_key=$1 FOR UPDATE',
          [key],
        );
        if (!existing.rowCount) throw new ExecutionConflict();
        if (existing.rows[0]!.fingerprint !== hash)
          throw new ExecutionConflict(
            'Idempotency key has different request content',
          );
        return { id: existing.rows[0]!.id, created: false };
      }
      for (const step of workflow.definition.steps)
        await client.query(
          'INSERT INTO step_executions (execution_id, step_id, status) VALUES ($1,$2,$3)',
          [executionId, step.id, 'pending'],
        );
      await event(client, executionId, null, 'launched', {
        workflow: workflow.name,
        version: workflow.version,
      });
      await this.advance(client, executionId, workflow.definition);
      return { id: executionId, created: true };
    });
  }

  async get(id: string): Promise<ExecutionSnapshot> {
    return this.db.transaction(async (client) => {
      await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
      const execution = await client.query<
        ExecutionRow & { created_at: Date; finished_at: Date | null }
      >(
        'SELECT e.id,e.status,e.input,e.created_at,e.finished_at,d.name,d.version,d.definition,e.trace_context FROM workflow_executions e JOIN workflow_definitions d ON d.id=e.definition_id WHERE e.id=$1',
        [id],
      );
      if (!execution.rowCount) throw new ExecutionNotFound();
      const steps = await client.query<StepRow>(
        'SELECT step_id,status,output,attempt_count,last_failure FROM step_executions WHERE execution_id=$1 ORDER BY step_id',
        [id],
      );
      const row = execution.rows[0]!;
      return {
        id: row.id,
        workflowName: row.name,
        version: row.version,
        status: row.status,
        input: row.input,
        createdAt: row.created_at,
        finishedAt: row.finished_at,
        steps: steps.rows.map((step) => ({
          id: step.step_id,
          status: step.status,
          output: step.output,
          attemptCount: step.attempt_count,
          error: step.last_failure,
        })),
      };
    });
  }

  async events(
    id: string,
    after = '0',
    limit = 100,
  ): Promise<
    Array<{
      id: string;
      stepId: string | null;
      kind: string;
      detail: Json;
      createdAt: Date;
    }>
  > {
    const capped = Math.max(1, Math.min(100, limit));
    const rows = await this.db.query<{
      id: string;
      step_id: string | null;
      kind: string;
      detail: Json;
      created_at: Date;
    }>(
      'SELECT id::text,step_id,kind,detail,created_at FROM execution_events WHERE execution_id=$1 AND id>$2::bigint ORDER BY execution_events.id LIMIT $3',
      [id, after, capped],
    );
    return rows.rows.map((row) => ({
      id: row.id,
      stepId: row.step_id,
      kind: row.kind,
      detail: row.detail,
      createdAt: row.created_at,
    }));
  }

  async ready(
    limit = 100,
  ): Promise<
    Array<{ executionId: string; stepId: string; attemptCount: number }>
  > {
    const rows = await this.db.query<{
      execution_id: string;
      step_id: string;
      attempt_count: number;
    }>(
      'SELECT s.execution_id,s.step_id,s.attempt_count FROM step_executions s JOIN workflow_executions e ON e.id=s.execution_id WHERE e.status=$1 AND s.status=$2 ORDER BY s.execution_id,s.step_id LIMIT $3',
      ['running', 'ready', Math.max(1, Math.min(100, limit))],
    );
    return rows.rows.map((row) => ({
      executionId: row.execution_id,
      stepId: row.step_id,
      attemptCount: row.attempt_count,
    }));
  }

  async claim(executionId: string, stepId: string): Promise<Claim | null> {
    return this.db.transaction(async (client) => {
      await client.query('SELECT pg_advisory_xact_lock($1)', [lock]);
      const execution = await this.execution(client, executionId, true);
      if (execution.status !== 'running') return null;
      const step = execution.definition.steps.find(
        (item) => item.id === stepId,
      );
      if (!step || step.type === 'delay') return null;
      const state = await client.query<StepRow>(
        'SELECT step_id,status,output,attempt_count,cycle_attempt,last_failure,lease_token FROM step_executions WHERE execution_id=$1 AND step_id=$2 FOR UPDATE',
        [executionId, stepId],
      );
      if (!state.rowCount || state.rows[0]!.status !== 'ready') return null;
      const active = await client.query<{ count: string }>(
        'SELECT count(*) FROM step_executions WHERE status=$1 AND lease_until > clock_timestamp()',
        ['running'],
      );
      if (Number(active.rows[0]!.count) >= this.options.concurrency)
        return null;
      const current = state.rows[0]!;
      const token = randomUUID();
      const attempt = current.attempt_count + 1;
      const cycleAttempt = current.cycle_attempt + 1;
      await client.query(
        "UPDATE step_executions SET status=$3,lease_token=$4,lease_until=clock_timestamp()+($5 * interval '1 millisecond'),attempt_count=$6,cycle_attempt=$7,due_at=NULL WHERE execution_id=$1 AND step_id=$2",
        [
          executionId,
          stepId,
          'running',
          token,
          this.options.leaseMs,
          attempt,
          cycleAttempt,
        ],
      );
      await client.query(
        'INSERT INTO step_attempts (execution_id,step_id,attempt,status,lease_token) VALUES ($1,$2,$3,$4,$5)',
        [executionId, stepId, attempt, 'running', token],
      );
      await event(client, executionId, stepId, 'claimed', {
        attempt,
        cycleAttempt,
      });
      const outputs = await this.outputs(client, executionId);
      return {
        executionId,
        step,
        token,
        attempt,
        cycleAttempt,
        input: execution.input,
        outputs,
        workflowName: execution.name,
        version: execution.version,
        traceContext: execution.trace_context,
      };
    });
  }

  async heartbeat(claim: Claim): Promise<boolean> {
    return this.db.transaction(async (client) => {
      // Renewal and admission must agree even while a renewed lease is uncommitted.
      await client.query('SELECT pg_advisory_xact_lock($1)', [lock]);
      const result = await client.query(
        "UPDATE step_executions SET lease_until=clock_timestamp()+($4 * interval '1 millisecond') WHERE execution_id=$1 AND step_id=$2 AND status=$3 AND lease_token=$5 AND lease_until>clock_timestamp()",
        [
          claim.executionId,
          claim.step.id,
          'running',
          this.options.leaseMs,
          claim.token,
        ],
      );
      return result.rowCount === 1;
    });
  }

  async succeed(claim: Claim, output: Json): Promise<boolean> {
    return this.db.transaction(async (client) => {
      const execution = await this.execution(client, claim.executionId, true);
      const row = await client.query<StepRow>(
        'SELECT step_id,status,output,attempt_count,cycle_attempt,last_failure,lease_token FROM step_executions WHERE execution_id=$1 AND step_id=$2 FOR UPDATE',
        [claim.executionId, claim.step.id],
      );
      if (
        !row.rowCount ||
        row.rows[0]!.status !== 'running' ||
        row.rows[0]!.lease_token !== claim.token
      )
        return false;
      const changed = await client.query(
        'UPDATE step_executions SET status=$3,output=$4,lease_token=NULL,lease_until=NULL,due_at=NULL,last_failure=NULL WHERE execution_id=$1 AND step_id=$2 AND lease_token=$5 AND lease_until>clock_timestamp()',
        [
          claim.executionId,
          claim.step.id,
          'succeeded',
          json(output),
          claim.token,
        ],
      );
      if (!changed.rowCount) return false;
      await client.query(
        'UPDATE step_attempts SET status=$4,finished_at=clock_timestamp() WHERE execution_id=$1 AND step_id=$2 AND attempt=$3 AND lease_token=$5 AND status=$6',
        [
          claim.executionId,
          claim.step.id,
          claim.attempt,
          'succeeded',
          claim.token,
          'running',
        ],
      );
      await event(client, claim.executionId, claim.step.id, 'succeeded', {
        attempt: claim.attempt,
      });
      await this.advance(client, claim.executionId, execution.definition);
      return true;
    });
  }

  async fail(claim: Claim, failure: StepFailure): Promise<boolean> {
    return this.db.transaction(async (client) =>
      this.finishFailure(
        client,
        claim.executionId,
        claim.step,
        claim.token,
        claim.attempt,
        failure,
        true,
      ),
    );
  }

  async tick(): Promise<DelayCompletion[]> {
    return this.db.transaction(async (client) => {
      await client.query('SELECT pg_advisory_xact_lock($1)', [schedulerLock]);
      const completed: DelayCompletion[] = [];
      const candidates = await client.query<{
        execution_id: string;
        step_id: string;
      }>(
        'SELECT s.execution_id,s.step_id FROM step_executions s JOIN workflow_executions e ON e.id=s.execution_id WHERE (s.status=$1 AND s.lease_until<=clock_timestamp()) OR (e.status=$2 AND s.status IN ($3,$4) AND s.due_at<=clock_timestamp()) OR (e.status=$2 AND s.status=$5) ORDER BY CASE WHEN s.status=$1 THEN 0 WHEN s.due_at<=clock_timestamp() THEN 1 ELSE 2 END, s.due_at NULLS LAST LIMIT 100',
        ['running', 'running', 'waiting', 'ready', 'ready'],
      );
      for (const item of candidates.rows) {
        const execution = await this.execution(client, item.execution_id, true);
        const row = await client.query<TickStepRow>(
          'SELECT step_id,status,output,attempt_count,cycle_attempt,last_failure,lease_token,due_at,COALESCE(lease_until<=clock_timestamp(),false) AS lease_expired,COALESCE(due_at<=clock_timestamp(),false) AS due FROM step_executions WHERE execution_id=$1 AND step_id=$2 FOR UPDATE',
          [item.execution_id, item.step_id],
        );
        if (!row.rowCount) continue;
        const step = execution.definition.steps.find(
          (value) => value.id === item.step_id,
        )!;
        const current = row.rows[0]!;
        if (current.status === 'running' && current.lease_expired) {
          const failure: StepFailure =
            step.type === 'http'
              ? step.safety === 'unsafe'
                ? {
                    kind: 'unknown_outcome',
                    message: 'Lease expired after unsafe HTTP attempt',
                  }
                : {
                    kind: 'transport',
                    message: 'Lease expired before HTTP result was recorded',
                  }
              : {
                  kind: 'internal',
                  message: 'Lease expired before result was recorded',
                };
          await this.finishFailure(
            client,
            item.execution_id,
            step,
            current.lease_token!,
            current.attempt_count,
            failure,
            false,
          );
        } else if (
          execution.status === 'running' &&
          current.status === 'ready' &&
          step.type === 'delay'
        ) {
          await client.query(
            "UPDATE step_executions SET status=$3,due_at=clock_timestamp()+($4 * interval '1 millisecond') WHERE execution_id=$1 AND step_id=$2 AND status=$5",
            [
              item.execution_id,
              item.step_id,
              'waiting',
              step.durationMs,
              'ready',
            ],
          );
          await event(client, item.execution_id, item.step_id, 'waiting', {
            durationMs: step.durationMs,
          });
        } else if (
          execution.status === 'running' &&
          current.status === 'waiting' &&
          current.due
        ) {
          if (step.type === 'delay') {
            await client.query(
              'UPDATE step_executions SET status=$3,due_at=NULL,attempt_count=attempt_count+1 WHERE execution_id=$1 AND step_id=$2 AND status=$4',
              [item.execution_id, item.step_id, 'succeeded', 'waiting'],
            );
            const attempt = current.attempt_count + 1;
            const attemptRow = await client.query<{ started_at: Date }>(
              "INSERT INTO step_attempts (execution_id,step_id,attempt,status,lease_token,started_at,finished_at) VALUES ($1,$2,$3,$4,$5,$6::timestamptz - ($7 * interval '1 millisecond'),clock_timestamp()) RETURNING started_at",
              [
                item.execution_id,
                item.step_id,
                attempt,
                'succeeded',
                randomUUID(),
                current.due_at,
                step.durationMs,
              ],
            );
            await event(client, item.execution_id, item.step_id, 'succeeded', {
              attempt,
            });
            await this.advance(client, item.execution_id, execution.definition);
            completed.push({
              executionId: item.execution_id,
              stepId: item.step_id,
              workflowName: execution.name,
              version: execution.version,
              traceContext: execution.trace_context,
              attempt,
              startedAt: attemptRow.rows[0]!.started_at,
            });
          } else
            await client.query(
              'UPDATE step_executions SET status=$3,due_at=NULL WHERE execution_id=$1 AND step_id=$2 AND status=$4',
              [item.execution_id, item.step_id, 'ready', 'waiting'],
            );
        }
      }
      return completed;
    });
  }

  async pause(id: string): Promise<void> {
    await this.control(id, 'pause');
  }
  async resume(id: string): Promise<void> {
    await this.control(id, 'resume');
  }

  async retry(id: string, confirmUnknownOutcome = false): Promise<void> {
    await this.db.transaction(async (client) => {
      const execution = await this.execution(client, id, true);
      const failed = await client.query<StepRow>(
        'SELECT step_id,status,output,attempt_count,cycle_attempt,last_failure,lease_token FROM step_executions WHERE execution_id=$1 AND status=$2 FOR UPDATE',
        [id, 'failed'],
      );
      if (
        !failed.rowCount ||
        (execution.status !== 'failed' && execution.status !== 'paused')
      )
        throw new ExecutionConflict();
      const running = await client.query<{ count: string }>(
        'SELECT count(*) FROM step_executions WHERE execution_id=$1 AND status=$2',
        [id, 'running'],
      );
      if (Number(running.rows[0]!.count))
        throw new ExecutionConflict('Cannot retry while attempts are running');
      const unsafeUnknown = failed.rows.some((row) => {
        const step = execution.definition.steps.find(
          (candidate) => candidate.id === row.step_id,
        );
        const failure = row.last_failure;
        return (
          step?.type === 'http' &&
          step.safety === 'unsafe' &&
          (failure?.kind === 'unknown_outcome' ||
            (failure?.kind === 'permanent_http' &&
              failure.status !== undefined &&
              failure.status >= 200 &&
              failure.status < 300))
        );
      });
      if (unsafeUnknown && !confirmUnknownOutcome)
        throw new ExecutionConflict(
          'Unsafe unknown outcome requires confirmation',
        );
      await client.query(
        'UPDATE step_executions SET status=$2,cycle_attempt=0,due_at=NULL,lease_token=NULL,lease_until=NULL,last_failure=NULL WHERE execution_id=$1 AND status=$3',
        [id, 'ready', 'failed'],
      );
      await client.query(
        'UPDATE workflow_executions SET status=$2,updated_at=clock_timestamp(),finished_at=NULL WHERE id=$1',
        [id, 'running'],
      );
      await event(client, id, null, 'retried', {
        confirmedUnknownOutcome: confirmUnknownOutcome,
      });
    });
  }

  private async control(id: string, action: 'pause' | 'resume'): Promise<void> {
    await this.db.transaction(async (client) => {
      const execution = await this.execution(client, id, true);
      if (action === 'pause') {
        if (execution.status === 'paused') return;
        if (execution.status !== 'running') throw new ExecutionConflict();
        await client.query(
          'UPDATE workflow_executions SET status=$2,updated_at=clock_timestamp() WHERE id=$1',
          [id, 'paused'],
        );
        await event(client, id, null, 'paused', {});
      } else {
        if (execution.status === 'running') return;
        if (execution.status !== 'paused') throw new ExecutionConflict();
        const failures = await client.query<{ count: string }>(
          'SELECT count(*) FROM step_executions WHERE execution_id=$1 AND status=$2',
          [id, 'failed'],
        );
        const status = Number(failures.rows[0]!.count) ? 'failed' : 'running';
        await client.query(
          'UPDATE workflow_executions SET status=$2,updated_at=clock_timestamp(),finished_at=CASE WHEN $2=$3 THEN clock_timestamp() ELSE NULL END WHERE id=$1',
          [id, status, 'failed'],
        );
        await event(client, id, null, 'resumed', {});
        if (status === 'running')
          await this.advance(client, id, execution.definition);
      }
    });
  }

  private async finishFailure(
    client: PoolClient,
    executionId: string,
    step: Step,
    token: string,
    attempt: number,
    failure: StepFailure,
    requireLease: boolean,
  ): Promise<boolean> {
    const execution = await this.execution(client, executionId, true);
    const row = await client.query<StepRow>(
      'SELECT step_id,status,output,attempt_count,cycle_attempt,last_failure,lease_token FROM step_executions WHERE execution_id=$1 AND step_id=$2 FOR UPDATE',
      [executionId, step.id],
    );
    if (
      !row.rowCount ||
      row.rows[0]!.status !== 'running' ||
      row.rows[0]!.lease_token !== token
    )
      return false;
    if (requireLease) {
      const valid = await client.query(
        'SELECT 1 FROM step_executions WHERE execution_id=$1 AND step_id=$2 AND lease_token=$3 AND lease_until>clock_timestamp()',
        [executionId, step.id, token],
      );
      if (!valid.rowCount) return false;
    }
    const current = row.rows[0]!;
    const delay = retryDelay(step, failure, current.cycle_attempt);
    const pureRetry =
      !requireLease &&
      step.type !== 'http' &&
      failure.kind === 'internal' &&
      current.cycle_attempt < 3;
    const waiting = delay !== null;
    const next = waiting ? 'waiting' : pureRetry ? 'ready' : 'failed';
    await client.query(
      "UPDATE step_executions SET status=$3,due_at=CASE WHEN $3=$4 THEN clock_timestamp()+($5 * interval '1 millisecond') ELSE NULL END,lease_token=NULL,lease_until=NULL,last_failure=$6 WHERE execution_id=$1 AND step_id=$2",
      [executionId, step.id, next, 'waiting', delay ?? 0, json(failure)],
    );
    await client.query(
      'UPDATE step_attempts SET status=$4,finished_at=clock_timestamp(),failure=$5 WHERE execution_id=$1 AND step_id=$2 AND attempt=$3 AND lease_token=$6 AND status=$7',
      [
        executionId,
        step.id,
        attempt,
        'failed',
        json(failure),
        token,
        'running',
      ],
    );
    await event(
      client,
      executionId,
      step.id,
      waiting ? 'retry_scheduled' : pureRetry ? 'crash_requeued' : 'failed',
      { attempt, kind: failure.kind },
    );
    if (!waiting && !pureRetry && execution.status !== 'paused')
      await client.query(
        'UPDATE workflow_executions SET status=$2,updated_at=clock_timestamp(),finished_at=clock_timestamp() WHERE id=$1 AND status=$3',
        [executionId, 'failed', 'running'],
      );
    return true;
  }

  private async execution(
    client: PoolClient,
    id: string,
    locked: boolean,
  ): Promise<ExecutionRow> {
    const result = await client.query<ExecutionRow>(
      `SELECT e.id,e.status,e.input,e.trace_context,d.name,d.version,d.definition FROM workflow_executions e JOIN workflow_definitions d ON d.id=e.definition_id WHERE e.id=$1 ${locked ? 'FOR UPDATE OF e' : ''}`,
      [id],
    );
    if (!result.rowCount) throw new ExecutionNotFound();
    return result.rows[0]!;
  }
  private async outputs(
    client: PoolClient,
    executionId: string,
  ): Promise<Record<string, Json>> {
    const result = await client.query<{ step_id: string; output: Json }>(
      'SELECT step_id,output FROM step_executions WHERE execution_id=$1 AND status=$2',
      [executionId, 'succeeded'],
    );
    return Object.fromEntries(
      result.rows.map((row) => [row.step_id, row.output]),
    );
  }
  private async advance(
    client: PoolClient,
    executionId: string,
    definition: Definition,
  ): Promise<void> {
    const states = await client.query<StepRow>(
      'SELECT step_id,status,output,attempt_count,cycle_attempt,last_failure,lease_token FROM step_executions WHERE execution_id=$1',
      [executionId],
    );
    const map: Record<string, { status: StepStatus; output?: Json }> = {};
    for (const row of states.rows)
      map[row.step_id] =
        row.output === null
          ? { status: row.status }
          : { status: row.status, output: row.output };
    const transitions = readyTransitions(definition, map);
    for (const stepId of transitions.skipped) {
      await client.query(
        'UPDATE step_executions SET status=$3 WHERE execution_id=$1 AND step_id=$2 AND status=$4',
        [executionId, stepId, 'skipped', 'pending'],
      );
      await event(client, executionId, stepId, 'skipped', {});
    }
    for (const stepId of transitions.ready) {
      await client.query(
        'UPDATE step_executions SET status=$3 WHERE execution_id=$1 AND step_id=$2 AND status=$4',
        [executionId, stepId, 'ready', 'pending'],
      );
      await event(client, executionId, stepId, 'ready', {});
    }
    const after = await client.query<{ status: StepStatus }>(
      'SELECT status FROM step_executions WHERE execution_id=$1',
      [executionId],
    );
    if (
      after.rows.every(
        (row) => row.status === 'succeeded' || row.status === 'skipped',
      )
    ) {
      const completed = await client.query(
        'UPDATE workflow_executions SET status=$2,finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1 AND status=$3',
        [executionId, 'succeeded', 'running'],
      );
      if (completed.rowCount)
        await event(client, executionId, null, 'succeeded', {});
    }
  }
}
