CREATE TABLE workflow_definitions (
  id uuid PRIMARY KEY,
  name varchar(64) NOT NULL,
  version integer NOT NULL CHECK (version > 0),
  definition jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (name, version)
);

CREATE TABLE workflow_executions (
  id uuid PRIMARY KEY,
  definition_id uuid NOT NULL REFERENCES workflow_definitions(id),
  status text NOT NULL CHECK (status IN ('running', 'paused', 'failed', 'succeeded')),
  input jsonb NOT NULL,
  idempotency_key varchar(128) NOT NULL UNIQUE,
  fingerprint char(64) NOT NULL,
  trace_context jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz
);

CREATE TABLE step_executions (
  execution_id uuid NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
  step_id varchar(64) NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'ready', 'running', 'waiting', 'succeeded', 'failed', 'skipped')),
  output jsonb,
  due_at timestamptz,
  lease_token uuid,
  lease_until timestamptz,
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  cycle_attempt integer NOT NULL DEFAULT 0 CHECK (cycle_attempt >= 0),
  last_failure jsonb,
  PRIMARY KEY (execution_id, step_id),
  CHECK ((status = 'running' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
    OR (status <> 'running' AND lease_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX step_due_work_idx ON step_executions (due_at) WHERE status IN ('ready', 'waiting');
CREATE INDEX step_expired_lease_idx ON step_executions (lease_until) WHERE status = 'running';

CREATE TABLE step_attempts (
  execution_id uuid NOT NULL,
  step_id varchar(64) NOT NULL,
  attempt integer NOT NULL CHECK (attempt > 0),
  status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  failure jsonb,
  lease_token uuid NOT NULL,
  PRIMARY KEY (execution_id, step_id, attempt),
  FOREIGN KEY (execution_id, step_id) REFERENCES step_executions(execution_id, step_id) ON DELETE CASCADE,
  CHECK ((status = 'running') = (finished_at IS NULL))
);

CREATE TABLE execution_events (
  id bigserial PRIMARY KEY,
  execution_id uuid NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
  step_id varchar(64),
  kind text NOT NULL,
  detail jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX execution_events_cursor_idx ON execution_events (execution_id, id);

CREATE FUNCTION reject_definition_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'workflow definitions are immutable' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER workflow_definitions_immutable
BEFORE UPDATE OR DELETE ON workflow_definitions
FOR EACH ROW EXECUTE FUNCTION reject_definition_mutation();
