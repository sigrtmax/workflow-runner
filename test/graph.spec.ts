import { Definition, validateDefinition } from '../src/workflows/definition';
import { readyTransitions, StepState } from '../src/worker/graph';

function definition(steps: unknown[]): Definition {
  return validateDefinition({ steps });
}

describe('graph transitions', () => {
  test('makes roots ready in graph order', () => {
    const graph = definition([
      { id: 'first', type: 'delay', durationMs: 1 },
      { id: 'second', type: 'delay', durationMs: 1 },
    ]);
    expect(readyTransitions(graph, {})).toEqual({
      ready: ['first', 'second'],
      skipped: [],
    });
  });

  test.each([true, false])(
    'selects the branch for condition result %s',
    (outcome) => {
      const graph = definition([
        {
          id: 'gate',
          type: 'condition',
          operator: 'exists',
          left: { ref: 'input.value' },
        },
        {
          id: 'yes',
          type: 'delay',
          durationMs: 1,
          needs: [{ step: 'gate', when: true }],
        },
        {
          id: 'no',
          type: 'delay',
          durationMs: 1,
          needs: [{ step: 'gate', when: false }],
        },
      ]);
      const states: Record<string, StepState> = {
        gate: { status: 'succeeded', output: outcome },
      };
      expect(readyTransitions(graph, states)).toEqual({
        ready: [outcome ? 'yes' : 'no'],
        skipped: [outcome ? 'no' : 'yes'],
      });
    },
  );

  test('propagates a nested skipped branch in one call', () => {
    const graph = definition([
      {
        id: 'gate',
        type: 'condition',
        operator: 'exists',
        left: { ref: 'input.value' },
      },
      {
        id: 'no',
        type: 'delay',
        durationMs: 1,
        needs: [{ step: 'gate', when: false }],
      },
      { id: 'nested', type: 'delay', durationMs: 1, needs: [{ step: 'no' }] },
    ]);
    expect(
      readyTransitions(graph, { gate: { status: 'succeeded', output: true } }),
    ).toEqual({ ready: [], skipped: ['no', 'nested'] });
  });

  test('waits for the unresolved side of a join', () => {
    const graph = definition([
      {
        id: 'gate',
        type: 'condition',
        operator: 'exists',
        left: { ref: 'input.value' },
      },
      {
        id: 'yes',
        type: 'delay',
        durationMs: 1,
        needs: [{ step: 'gate', when: true }],
      },
      {
        id: 'no',
        type: 'delay',
        durationMs: 1,
        needs: [{ step: 'gate', when: false }],
      },
      { id: 'other', type: 'delay', durationMs: 1 },
      {
        id: 'join',
        type: 'delay',
        durationMs: 1,
        needs: [{ step: 'yes' }, { step: 'no' }, { step: 'other' }],
      },
    ]);
    const states: Record<string, StepState> = {
      gate: { status: 'succeeded', output: true },
      yes: { status: 'succeeded' },
      no: { status: 'skipped' },
      other: { status: 'running' },
    };
    expect(readyTransitions(graph, states)).toEqual({ ready: [], skipped: [] });
    expect(
      readyTransitions(graph, { ...states, other: { status: 'succeeded' } }),
    ).toEqual({ ready: ['join'], skipped: [] });
  });

  test('does not schedule through a failed predecessor', () => {
    const graph = definition([
      { id: 'root', type: 'delay', durationMs: 1 },
      { id: 'next', type: 'delay', durationMs: 1, needs: [{ step: 'root' }] },
    ]);
    expect(readyTransitions(graph, { root: { status: 'failed' } })).toEqual({
      ready: [],
      skipped: [],
    });
  });
});
