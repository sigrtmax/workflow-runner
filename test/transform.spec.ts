import { Json, validateDefinition } from '../src/workflows/definition';
import { condition } from '../src/worker/condition-step';
import { DataError, evaluate, transform } from '../src/worker/transform-step';

describe('data expressions', () => {
  test('normalizes nested string expressions', () => {
    const step = validateDefinition({
      steps: [
        {
          id: 'map',
          type: 'transform',
          fields: {
            email: {
              op: 'lowercase',
              value: { op: 'trim', value: { ref: 'input.email' } },
            },
          },
        },
      ],
    }).steps[0]!;
    if (step.type !== 'transform') throw new Error('expected transform');
    expect(
      transform(step, { input: { email: '  A@EXAMPLE.COM ' }, steps: {} }),
    ).toEqual({ email: 'a@example.com' });
  });

  test('throws for missing references and unavailable skipped outputs', () => {
    expect(() =>
      evaluate({ ref: 'input.missing' }, { input: {}, steps: {} }),
    ).toThrow(DataError);
    expect(() =>
      evaluate({ ref: 'steps.skipped.value' }, { input: {}, steps: {} }),
    ).toThrow(DataError);
  });

  test.each([
    { literal: '' },
    { literal: null },
    { literal: true },
    { literal: 'nope' },
  ])('rejects invalid number conversion', (value) => {
    expect(() =>
      evaluate({ op: 'toNumber', value }, { input: null, steps: {} }),
    ).toThrow(DataError);
  });

  test('compares JSON objects independently of key order', () => {
    const step = validateDefinition({
      steps: [
        {
          id: 'same',
          type: 'condition',
          operator: 'eq',
          left: { ref: 'input.left' },
          right: { ref: 'input.right' },
        },
      ],
    }).steps[0]!;
    if (step.type !== 'condition') throw new Error('expected condition');
    expect(
      condition(step, {
        input: { left: { a: 1, b: 2 }, right: { b: 2, a: 1 } },
        steps: {},
      }),
    ).toBe(true);
  });

  test('exists distinguishes missing values from null', () => {
    const step = validateDefinition({
      steps: [
        {
          id: 'present',
          type: 'condition',
          operator: 'exists',
          left: { ref: 'input.value' },
        },
      ],
    }).steps[0]!;
    if (step.type !== 'condition') throw new Error('expected condition');
    expect(condition(step, { input: { value: null }, steps: {} })).toBe(true);
    expect(condition(step, { input: {}, steps: {} })).toBe(false);
  });

  test('compares deeply nested JSON results without exhausting the call stack', () => {
    let left: Json = 1;
    let right: Json = 1;
    let different: Json = 2;
    for (let level = 0; level < 20_000; level += 1) {
      left = [left];
      right = [right];
      different = [different];
    }
    const step = validateDefinition({
      steps: [
        {
          id: 'compare',
          type: 'condition',
          operator: 'eq',
          left: { ref: 'input.left' },
          right: { ref: 'input.right' },
        },
      ],
    }).steps[0]!;
    if (step.type !== 'condition') throw new Error('expected condition');

    expect(condition(step, { input: { left, right }, steps: {} })).toBe(true);
    expect(
      condition(step, { input: { left, right: different }, steps: {} }),
    ).toBe(false);
  });

  test('ordered comparisons require matching scalar types', () => {
    const step = validateDefinition({
      steps: [
        {
          id: 'compare',
          type: 'condition',
          operator: 'gt',
          left: { ref: 'input.left' },
          right: { ref: 'input.right' },
        },
      ],
    }).steps[0]!;
    if (step.type !== 'condition') throw new Error('expected condition');
    expect(() =>
      condition(step, { input: { left: 2, right: '1' }, steps: {} }),
    ).toThrow(DataError);
  });
});
