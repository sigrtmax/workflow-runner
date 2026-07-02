export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };
export type Expression =
  | { literal: Json }
  | { ref: string }
  | { op: 'trim' | 'lowercase' | 'toNumber'; value: Expression };
export interface Dependency {
  step: string;
  when?: boolean;
}
export interface RetryPolicy {
  maxAttempts: number;
  backoffMs: number;
  maxBackoffMs: number;
}
export interface NodeBase {
  id: string;
  needs?: Dependency[];
}
export interface HttpStep extends NodeBase {
  type: 'http';
  integration: string;
  path: string;
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: Expression;
  expectedStatuses: number[];
  timeoutMs: number;
  retry: RetryPolicy;
  safety: 'read-only' | 'idempotency-key' | 'unsafe';
}
export interface ConditionStep extends NodeBase {
  type: 'condition';
  operator: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'exists';
  left: Expression;
  right?: Expression;
}
export interface DelayStep extends NodeBase {
  type: 'delay';
  durationMs: number;
}
export interface TransformStep extends NodeBase {
  type: 'transform';
  fields: Record<string, Expression>;
}
export type Step = HttpStep | ConditionStep | DelayStep | TransformStep;
export interface Definition {
  steps: Step[];
}
export class DefinitionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DefinitionError';
  }
}

const ID = /^[a-z][a-z0-9_]{0,63}$/;
const FORBIDDEN = new Set(['__proto__', 'prototype', 'constructor']);
// Bounds cap persisted payload traversal and worker scheduling/backoff costs.
const MAX_DURATION = 86_400_000;
const MAX_TIMEOUT = 300_000;
const MAX_DEFINITION_DEPTH = 32;

function fail(message: string): never {
  throw new DefinitionError(message);
}

export function validateJsonString(value: string, at = 'definition'): void {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit === 0) fail(`${at} must not contain NUL`);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff))
        fail(`${at} must not contain an unpaired surrogate`);
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      fail(`${at} must not contain an unpaired surrogate`);
    }
  }
}

export function validateJsonData(
  value: unknown,
  at = 'definition',
  depth = 0,
): asserts value is Json {
  if (depth > MAX_DEFINITION_DEPTH) {
    fail(`${at} exceeds maximum definition nesting of ${MAX_DEFINITION_DEPTH}`);
  }
  if (value === null || typeof value === 'boolean') {
    return;
  }
  if (typeof value === 'string') return validateJsonString(value, at);
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) fail(`${at} must be finite`);
    return;
  }
  if (typeof value !== 'object') fail(`${at} must be JSON data`);

  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype) {
      fail(`${at} must be a plain array`);
    }
    if (Object.getOwnPropertySymbols(value).length > 0) {
      fail(`${at} must not contain symbol properties`);
    }
    const names = Object.getOwnPropertyNames(value);
    if (
      names.length !== value.length + 1 ||
      names.some(
        (name) =>
          name !== 'length' &&
          (!/^(0|[1-9][0-9]*)$/.test(name) || Number(name) >= value.length),
      )
    ) {
      fail(`${at} must be a dense JSON array without extra properties`);
    }
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        fail(`${at}[${index}] must be an own data property`);
      }
      validateJsonData(descriptor.value, `${at}[${index}]`, depth + 1);
    }
    return;
  }

  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    fail(`${at} must be a plain object`);
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    fail(`${at} must not contain symbol properties`);
  }
  for (const [key, descriptor] of Object.entries(
    Object.getOwnPropertyDescriptors(value),
  )) {
    validateJsonString(key, at);
    if (FORBIDDEN.has(key)) fail(`${at}.${key} is forbidden`);
    if (!('value' in descriptor) || !descriptor.enumerable) {
      fail(`${at}.${key} must be an enumerable own data property`);
    }
    validateJsonData(descriptor.value, `${at}.${key}`, depth + 1);
  }
}

function record(value: unknown, at: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    fail(`${at} must be an object`);
  return value as Record<string, unknown>;
}
function exact(
  value: Record<string, unknown>,
  allowed: readonly string[],
  at: string,
): void {
  for (const key of Object.keys(value))
    if (!allowed.includes(key)) fail(`${at}.${key} is not allowed`);
}
function safeInteger(
  value: unknown,
  at: string,
  max = MAX_DURATION,
  min = 1,
): number {
  if (
    !Number.isSafeInteger(value) ||
    (value as number) < min ||
    (value as number) > max
  )
    fail(`${at} must be a safe integer from ${min} to ${max}`);
  return value as number;
}
function validateRef(value: unknown, at: string): string {
  if (typeof value !== 'string') fail(`${at} must be a string`);
  const parts = value.split('.');
  if (
    (parts[0] !== 'input' && parts[0] !== 'steps') ||
    parts.some((part) => part.length === 0 || FORBIDDEN.has(part))
  )
    fail(`${at} is invalid`);
  if (parts[0] === 'steps' && (parts.length < 2 || !ID.test(parts[1]!)))
    fail(`${at} is invalid`);
  return value;
}
function expression(
  value: unknown,
  at: string,
  refs: string[],
  depth = 1,
): Expression {
  if (depth > 16) fail(`${at} exceeds maximum expression nesting of 16`);
  const item = record(value, at);
  if (Object.hasOwn(item, 'literal')) {
    exact(item, ['literal'], at);
    return { literal: item.literal as Json };
  }
  if (Object.hasOwn(item, 'ref')) {
    exact(item, ['ref'], at);
    const ref = validateRef(item.ref, `${at}.ref`);
    refs.push(ref);
    return { ref };
  }
  if (Object.hasOwn(item, 'op')) {
    exact(item, ['op', 'value'], at);
    if (
      !['trim', 'lowercase', 'toNumber'].includes(item.op as string) ||
      !Object.hasOwn(item, 'value')
    )
      fail(`${at} is an invalid operation`);
    return {
      op: item.op as 'trim' | 'lowercase' | 'toNumber',
      value: expression(item.value, `${at}.value`, refs, depth + 1),
    };
  }
  return fail(`${at} is not an expression`);
}
function dependencies(value: unknown, at: string): Dependency[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) fail(`${at} must be an array`);
  return value.map((raw, index) => {
    const dep = record(raw, `${at}[${index}]`);
    exact(dep, ['step', 'when'], `${at}[${index}]`);
    if (typeof dep.step !== 'string' || !ID.test(dep.step))
      fail(`${at}[${index}].step is invalid`);
    if (Object.hasOwn(dep, 'when') && typeof dep.when !== 'boolean')
      fail(`${at}[${index}].when must be boolean`);
    return Object.hasOwn(dep, 'when')
      ? { step: dep.step, when: dep.when as boolean }
      : { step: dep.step };
  });
}
function retryPolicy(value: unknown, at: string): RetryPolicy {
  const retry = record(value, at);
  exact(retry, ['maxAttempts', 'backoffMs', 'maxBackoffMs'], at);
  const result = {
    maxAttempts: safeInteger(retry.maxAttempts, `${at}.maxAttempts`, 10),
    backoffMs: safeInteger(retry.backoffMs, `${at}.backoffMs`),
    maxBackoffMs: safeInteger(retry.maxBackoffMs, `${at}.maxBackoffMs`),
  };
  if (result.maxBackoffMs < result.backoffMs)
    fail(`${at}.maxBackoffMs must be at least backoffMs`);
  return result;
}
function validateStep(
  value: unknown,
  index: number,
  refsById: Map<string, string[]>,
): Step {
  const at = `steps[${index}]`;
  const raw = record(value, at);
  if (typeof raw.id !== 'string' || !ID.test(raw.id))
    fail(`${at}.id is invalid`);
  if (typeof raw.type !== 'string') fail(`${at}.type is invalid`);
  const needs = dependencies(raw.needs, `${at}.needs`);
  const base = needs === undefined ? { id: raw.id } : { id: raw.id, needs };
  const refs: string[] = [];
  refsById.set(raw.id, refs);
  switch (raw.type) {
    case 'delay':
      exact(raw, ['id', 'type', 'needs', 'durationMs'], at);
      return {
        ...base,
        type: 'delay',
        durationMs: safeInteger(raw.durationMs, `${at}.durationMs`),
      };
    case 'transform': {
      exact(raw, ['id', 'type', 'needs', 'fields'], at);
      const fieldsRaw = record(raw.fields, `${at}.fields`);
      const fields: Record<string, Expression> = Object.create(null) as Record<
        string,
        Expression
      >;
      for (const key of Object.keys(fieldsRaw)) {
        if (FORBIDDEN.has(key)) fail(`${at}.fields.${key} is forbidden`);
        fields[key] = expression(fieldsRaw[key], `${at}.fields.${key}`, refs);
      }
      return { ...base, type: 'transform', fields };
    }
    case 'condition': {
      exact(raw, ['id', 'type', 'needs', 'operator', 'left', 'right'], at);
      if (
        !['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'exists'].includes(
          raw.operator as string,
        )
      )
        fail(`${at}.operator is invalid`);
      const operator = raw.operator as ConditionStep['operator'];
      const left = expression(raw.left, `${at}.left`, refs);
      if (operator === 'exists' && Object.hasOwn(raw, 'right'))
        fail(`${at}.right is not allowed for exists`);
      if (operator !== 'exists' && !Object.hasOwn(raw, 'right'))
        fail(`${at}.right is required`);
      return operator === 'exists'
        ? { ...base, type: 'condition', operator, left }
        : {
            ...base,
            type: 'condition',
            operator,
            left,
            right: expression(raw.right, `${at}.right`, refs),
          };
    }
    case 'http': {
      exact(
        raw,
        [
          'id',
          'type',
          'needs',
          'integration',
          'path',
          'method',
          'body',
          'expectedStatuses',
          'timeoutMs',
          'retry',
          'safety',
        ],
        at,
      );
      if (typeof raw.integration !== 'string' || !ID.test(raw.integration))
        fail(`${at}.integration is invalid`);
      if (
        typeof raw.path !== 'string' ||
        !raw.path.startsWith('/') ||
        raw.path.startsWith('//') ||
        /[\\\r\n]/.test(raw.path)
      )
        fail(`${at}.path is invalid`);
      if (
        !['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(
          raw.method as string,
        )
      )
        fail(`${at}.method is invalid`);
      if (
        !['read-only', 'idempotency-key', 'unsafe'].includes(
          raw.safety as string,
        )
      )
        fail(`${at}.safety is invalid`);
      if (raw.safety === 'read-only' && raw.method !== 'GET')
        fail(`${at}.safety read-only requires GET`);
      if (raw.method === 'GET' && Object.hasOwn(raw, 'body'))
        fail(`${at}.body is not allowed for GET`);
      if (
        !Array.isArray(raw.expectedStatuses) ||
        raw.expectedStatuses.length === 0
      )
        fail(`${at}.expectedStatuses must be nonempty`);
      const statuses = raw.expectedStatuses.map((status, i) =>
        safeInteger(status, `${at}.expectedStatuses[${i}]`, 299, 200),
      );
      if (new Set(statuses).size !== statuses.length)
        fail(`${at}.expectedStatuses must be unique`);
      const result: HttpStep = {
        ...base,
        type: 'http',
        integration: raw.integration,
        path: raw.path,
        method: raw.method as HttpStep['method'],
        expectedStatuses: statuses,
        timeoutMs: safeInteger(raw.timeoutMs, `${at}.timeoutMs`, MAX_TIMEOUT),
        retry: retryPolicy(raw.retry, `${at}.retry`),
        safety: raw.safety as HttpStep['safety'],
      };
      if (Object.hasOwn(raw, 'body'))
        result.body = expression(raw.body, `${at}.body`, refs);
      return result;
    }
    default:
      return fail(`${at}.type is invalid`);
  }
}

export function validateDefinition(input: unknown): Definition {
  validateJsonData(input);
  const raw = record(input, 'definition');
  exact(raw, ['steps'], 'definition');
  if (
    !Array.isArray(raw.steps) ||
    raw.steps.length < 1 ||
    raw.steps.length > 100
  )
    fail('steps must contain 1 to 100 nodes');
  const refsById = new Map<string, string[]>();
  const steps = raw.steps.map((step, index) =>
    validateStep(step, index, refsById),
  );
  const byId = new Map<string, Step>();
  for (const step of steps) {
    if (byId.has(step.id)) fail(`duplicate step id ${step.id}`);
    byId.set(step.id, step);
  }
  for (const step of steps) {
    const seen = new Set<string>();
    for (const dep of step.needs ?? []) {
      if (seen.has(dep.step))
        fail(`duplicate dependency ${dep.step} on ${step.id}`);
      seen.add(dep.step);
      const parent = byId.get(dep.step);
      if (!parent) fail(`missing dependency ${dep.step}`);
      if (parent.id === step.id) fail(`${step.id} cannot depend on itself`);
      if (parent.type === 'condition' && dep.when === undefined)
        fail(`dependency on condition ${parent.id} requires when`);
      if (parent.type !== 'condition' && dep.when !== undefined)
        fail(`when is only valid for condition dependencies`);
    }
  }
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (id: string): void => {
    if (visiting.has(id)) fail('definition contains a cycle');
    if (visited.has(id)) return;
    visiting.add(id);
    for (const dep of byId.get(id)!.needs ?? []) visit(dep.step);
    visiting.delete(id);
    visited.add(id);
  };
  steps.forEach((step) => visit(step.id));
  const ancestors = (id: string, found = new Set<string>()): Set<string> => {
    for (const dep of byId.get(id)!.needs ?? [])
      if (!found.has(dep.step)) {
        found.add(dep.step);
        ancestors(dep.step, found);
      }
    return found;
  };
  for (const step of steps)
    for (const ref of refsById.get(step.id) ?? [])
      if (
        ref.startsWith('steps.') &&
        !ancestors(step.id).has(ref.split('.')[1]!)
      )
        fail(`${ref} does not reference an ancestor of ${step.id}`);
  return { steps };
}
