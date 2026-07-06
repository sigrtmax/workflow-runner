import { Expression, Json, TransformStep } from '../workflows/definition';
export interface DataContext {
  input: Json;
  steps: Record<string, Json>;
}
export class DataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DataError';
  }
}
export class MissingDataError extends DataError {}

function resolve(ref: string, context: DataContext): Json {
  const parts = ref.split('.');
  let value: Json | Record<string, Json> =
    parts[0] === 'input' ? context.input : context.steps;
  for (const segment of parts.slice(1)) {
    if (
      typeof value !== 'object' ||
      value === null ||
      !Object.hasOwn(value, segment)
    )
      throw new MissingDataError(`Missing value at ${ref}`);
    value = (value as Record<string, Json>)[segment]!;
  }
  return value as Json;
}

export function evaluate(expression: Expression, context: DataContext): Json {
  if ('literal' in expression) return expression.literal;
  if ('ref' in expression) return resolve(expression.ref, context);
  const value = evaluate(expression.value, context);
  if (expression.op === 'trim' || expression.op === 'lowercase') {
    if (typeof value !== 'string')
      throw new DataError(`${expression.op} requires a string`);
    return expression.op === 'trim' ? value.trim() : value.toLowerCase();
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value))
      throw new DataError('toNumber requires a finite number');
    return value;
  }
  if (typeof value !== 'string' || value.trim() === '')
    throw new DataError(
      'toNumber requires a number or nonempty numeric string',
    );
  const number = Number(value);
  if (!Number.isFinite(number))
    throw new DataError('toNumber conversion failed');
  return number;
}

export function transform(step: TransformStep, context: DataContext): Json {
  const result: Record<string, Json> = Object.create(null) as Record<
    string,
    Json
  >;
  for (const [field, expression] of Object.entries(step.fields))
    result[field] = evaluate(expression, context);
  return result;
}
