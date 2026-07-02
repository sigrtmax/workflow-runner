import {
  DefinitionError,
  validateDefinition,
  validateJsonData,
} from '../src/workflows/definition';

const retry = { maxAttempts: 3, backoffMs: 100, maxBackoffMs: 1_000 };

describe('workflow definitions', () => {
  test.each(['\u0000', '\ud800', '\udc00'])(
    'rejects JSONB-incompatible strings',
    (value) => {
      expect(() => validateJsonData(value)).toThrow(DefinitionError);
      expect(() => validateJsonData({ [value]: 'safe' })).toThrow(
        DefinitionError,
      );
    },
  );

  test('accepts valid surrogate pairs in values and keys', () => {
    expect(() => validateJsonData({ '😀': '😀' })).not.toThrow();
  });

  test('accepts a valid lead graph', () => {
    const definition = {
      steps: [
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
          id: 'qualified',
          type: 'condition',
          needs: [{ step: 'normalize' }],
          operator: 'exists',
          left: { ref: 'steps.normalize.email' },
        },
        {
          id: 'crm',
          type: 'http',
          needs: [{ step: 'qualified', when: true }],
          integration: 'crm',
          path: '/leads',
          method: 'POST',
          body: { ref: 'steps.normalize' },
          expectedStatuses: [201],
          timeoutMs: 2_000,
          retry,
          safety: 'idempotency-key',
        },
      ],
    };
    expect(validateDefinition(definition)).toEqual(definition);
  });

  test('rejects unknown configuration recursively', () => {
    expect(() =>
      validateDefinition({
        steps: [{ id: 'wait', type: 'delay', durationMs: 20, extra: true }],
      }),
    ).toThrow(DefinitionError);
    expect(() =>
      validateDefinition({
        steps: [
          {
            id: 'map',
            type: 'transform',
            fields: { value: { literal: { nested: 1, extra: undefined } } },
          },
        ],
      }),
    ).toThrow(DefinitionError);
  });

  test('rejects duplicate IDs, invalid dependencies, and cycles', () => {
    expect(() =>
      validateDefinition({
        steps: [
          { id: 'same', type: 'delay', durationMs: 1 },
          { id: 'same', type: 'delay', durationMs: 1 },
        ],
      }),
    ).toThrow(DefinitionError);
    expect(() =>
      validateDefinition({
        steps: [
          {
            id: 'a',
            type: 'delay',
            durationMs: 1,
            needs: [{ step: 'missing' }],
          },
        ],
      }),
    ).toThrow(DefinitionError);
    expect(() =>
      validateDefinition({
        steps: [
          { id: 'a', type: 'delay', durationMs: 1, needs: [{ step: 'b' }] },
          { id: 'b', type: 'delay', durationMs: 1, needs: [{ step: 'a' }] },
        ],
      }),
    ).toThrow(DefinitionError);
  });

  test('requires explicit condition branches and forbids when elsewhere', () => {
    expect(() =>
      validateDefinition({
        steps: [
          {
            id: 'gate',
            type: 'condition',
            operator: 'exists',
            left: { ref: 'input.x' },
          },
          {
            id: 'next',
            type: 'delay',
            durationMs: 1,
            needs: [{ step: 'gate' }],
          },
        ],
      }),
    ).toThrow(DefinitionError);
    expect(() =>
      validateDefinition({
        steps: [
          { id: 'wait', type: 'delay', durationMs: 1 },
          {
            id: 'next',
            type: 'delay',
            durationMs: 1,
            needs: [{ step: 'wait', when: true }],
          },
        ],
      }),
    ).toThrow(DefinitionError);
  });

  test('rejects references to nodes that are not transitive ancestors', () => {
    expect(() =>
      validateDefinition({
        steps: [
          { id: 'source', type: 'delay', durationMs: 1 },
          {
            id: 'map',
            type: 'transform',
            fields: { value: { ref: 'steps.source' } },
          },
        ],
      }),
    ).toThrow(DefinitionError);
  });

  test.each([
    { steps: [] },
    { steps: [{ id: 'Wait', type: 'delay', durationMs: 1 }] },
    { steps: [{ id: 'wait', type: 'delay', durationMs: 0 }] },
    {
      steps: [
        {
          id: 'call',
          type: 'http',
          integration: 'crm',
          path: 'https://example.com',
          method: 'GET',
          expectedStatuses: [200],
          timeoutMs: 1,
          retry,
          safety: 'read-only',
        },
      ],
    },
    {
      steps: [
        {
          id: 'call',
          type: 'http',
          integration: 'crm',
          path: '/x',
          method: 'POST',
          expectedStatuses: [200],
          timeoutMs: 1,
          retry,
          safety: 'read-only',
        },
      ],
    },
    {
      steps: [
        {
          id: 'call',
          type: 'http',
          integration: 'crm',
          path: '/x',
          method: 'GET',
          body: { literal: null },
          expectedStatuses: [200],
          timeoutMs: 1,
          retry,
          safety: 'read-only',
        },
      ],
    },
  ])('rejects invalid bounded or HTTP definitions', (definition) => {
    expect(() => validateDefinition(definition)).toThrow(DefinitionError);
  });

  test('rejects dangerous keys and ref segments', () => {
    const dangerous = JSON.parse(
      '{"steps":[{"id":"map","type":"transform","fields":{"value":{"literal":{"__proto__":1}}}}]}',
    );
    expect(() => validateDefinition(dangerous)).toThrow(DefinitionError);
    expect(() =>
      validateDefinition({
        steps: [
          {
            id: 'map',
            type: 'transform',
            fields: { value: { ref: 'input.constructor' } },
          },
        ],
      }),
    ).toThrow(DefinitionError);
  });

  test.each([new Date(), new Map(), new (class Example {})()])(
    'rejects non-JSON literal objects',
    (literal) => {
      expect(() =>
        validateDefinition({
          steps: [
            { id: 'map', type: 'transform', fields: { value: { literal } } },
          ],
        }),
      ).toThrow(DefinitionError);
    },
  );

  test('rejects prototype-backed and accessor-backed records', () => {
    const inherited = Object.create({
      steps: [{ id: 'wait', type: 'delay', durationMs: 1 }],
    });
    const accessor = {} as Record<string, unknown>;
    Object.defineProperty(accessor, 'steps', {
      enumerable: true,
      get: () => [{ id: 'wait', type: 'delay', durationMs: 1 }],
    });
    expect(() => validateDefinition(inherited)).toThrow(DefinitionError);
    expect(() => validateDefinition(accessor)).toThrow(DefinitionError);
  });

  test('rejects sparse arrays and arrays with non-index properties', () => {
    const sparse = Array(1);
    const extended = [null] as unknown[] & Record<string, unknown>;
    extended.extra = true;
    for (const literal of [sparse, extended]) {
      expect(() =>
        validateDefinition({
          steps: [
            { id: 'map', type: 'transform', fields: { value: { literal } } },
          ],
        }),
      ).toThrow(DefinitionError);
    }
  });

  test('rejects symbol-bearing arrays and array subclasses', () => {
    const symbolBearing = [null];
    Object.defineProperty(symbolBearing, Symbol('extra'), { value: true });
    class JsonArray extends Array<unknown> {}
    const subclass = new JsonArray(null);
    for (const literal of [symbolBearing, subclass]) {
      expect(() =>
        validateDefinition({
          steps: [
            { id: 'map', type: 'transform', fields: { value: { literal } } },
          ],
        }),
      ).toThrow(DefinitionError);
    }
  });

  test('rejects combined definition nesting deeper than 32', () => {
    let literal: unknown = null;
    for (let index = 0; index < 20; index += 1) literal = [literal];
    let value: unknown = { literal };
    for (let index = 0; index < 10; index += 1) {
      value = { op: 'trim', value };
    }
    expect(() =>
      validateDefinition({
        steps: [{ id: 'map', type: 'transform', fields: { value } }],
      }),
    ).toThrow(DefinitionError);
  });
});
