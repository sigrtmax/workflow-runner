import { Writable } from 'node:stream';

import { ApplicationLogger, createEventLogger } from '../src/logging';

describe('structured application logging', () => {
  it('writes only allowlisted correlation fields', () => {
    let output = '';
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });
    const log = createEventLogger(destination);

    log('workflow.step.finished', {
      executionId: 'execution-42',
      stepId: 'send-crm',
      attempt: 2,
      traceId: 'a'.repeat(32),
      outcome: 'succeeded',
      injectedSecret: 'must-not-appear',
    } as never);

    const entry = JSON.parse(output) as Record<string, unknown>;
    expect(entry).toMatchObject({
      event: 'workflow.step.finished',
      executionId: 'execution-42',
      stepId: 'send-crm',
      attempt: 2,
      traceId: 'a'.repeat(32),
      outcome: 'succeeded',
    });
    expect(entry).not.toHaveProperty('injectedSecret');
  });

  it('maps Nest messages to fixed safe event names without emitting the message text', () => {
    let output = '';
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });
    const logger = new ApplicationLogger(createEventLogger(destination));

    logger.error(
      new Error('authorization: Bearer secret-value'),
      'stack with secret-value',
    );

    const entry = JSON.parse(output) as Record<string, unknown>;
    expect(entry.event).toBe('application.error');
    expect(entry.level).toBe(50);
    expect(output).not.toContain('secret-value');
  });

  it('suppresses routine Nest debug messages and retains startup at info level', () => {
    let output = '';
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });
    const logger = new ApplicationLogger(createEventLogger(destination));

    logger.debug('Mapped {/health, GET} route');
    logger.log('Nest application successfully started');

    const entries = output
      .trim()
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(entries).toEqual([
      expect.objectContaining({ event: 'application.started', level: 30 }),
    ]);
  });
});
