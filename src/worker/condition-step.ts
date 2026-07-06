import type { ConditionStep, Json } from '../workflows/definition';
import {
  DataContext,
  DataError,
  evaluate,
  MissingDataError,
} from './transform-step';

function equal(left: Json, right: Json): boolean {
  const pending: Array<[Json, Json]> = [[left, right]];
  while (pending.length > 0) {
    const [first, second] = pending.pop()!;
    if (first === second) continue;
    if (Array.isArray(first) || Array.isArray(second)) {
      if (
        !Array.isArray(first) ||
        !Array.isArray(second) ||
        first.length !== second.length
      ) {
        return false;
      }
      for (let index = 0; index < first.length; index += 1) {
        pending.push([first[index]!, second[index]!]);
      }
      continue;
    }
    if (
      first === null ||
      second === null ||
      typeof first !== 'object' ||
      typeof second !== 'object'
    ) {
      return false;
    }
    const keys = Object.keys(first);
    if (keys.length !== Object.keys(second).length) return false;
    for (const key of keys) {
      if (!Object.hasOwn(second, key)) return false;
      pending.push([first[key]!, second[key]!]);
    }
  }
  return true;
}

export function condition(step: ConditionStep, context: DataContext): boolean {
  if (step.operator === 'exists') {
    try {
      evaluate(step.left, context);
      return true;
    } catch (error) {
      if (error instanceof MissingDataError) return false;
      throw error;
    }
  }
  const left = evaluate(step.left, context);
  const right = evaluate(step.right!, context);
  if (step.operator === 'eq') return equal(left, right);
  if (step.operator === 'ne') return !equal(left, right);
  if (
    (typeof left !== 'number' && typeof left !== 'string') ||
    typeof left !== typeof right
  )
    throw new DataError(`${step.operator} requires two numbers or two strings`);
  if (typeof left === 'number' && typeof right === 'number') {
    if (step.operator === 'gt') return left > right;
    if (step.operator === 'gte') return left >= right;
    if (step.operator === 'lt') return left < right;
    return left <= right;
  }
  const rightString = right as string;
  if (step.operator === 'gt') return left > rightString;
  if (step.operator === 'gte') return left >= rightString;
  if (step.operator === 'lt') return left < rightString;
  return left <= rightString;
}
