import type { LoggerService } from '@nestjs/common';
import pino, { type DestinationStream } from 'pino';

export interface LogContext {
  requestId?: string;
  executionId?: string;
  stepId?: string;
  attempt?: number;
  traceId?: string;
  outcome?: string;
  status?: number;
}

type EventLogger = ((event: string, fields?: LogContext) => void) & {
  warn(event: string, fields?: LogContext): void;
  error(event: string, fields?: LogContext): void;
  fatal(event: string, fields?: LogContext): void;
};

export function createEventLogger(
  destination?: DestinationStream,
): EventLogger {
  const logger = pino({ base: undefined }, destination);
  const fieldsFor = (event: string, fields: LogContext = {}) => ({
    event,
    ...allowlisted(fields),
  });
  const eventLogger = (event: string, fields?: LogContext) =>
    logger.info(fieldsFor(event, fields));
  return Object.assign(eventLogger, {
    warn: (event: string, fields?: LogContext) =>
      logger.warn(fieldsFor(event, fields)),
    error: (event: string, fields?: LogContext) =>
      logger.error(fieldsFor(event, fields)),
    fatal: (event: string, fields?: LogContext) =>
      logger.fatal(fieldsFor(event, fields)),
  });
}

export const logEvent = createEventLogger();

export class ApplicationLogger implements LoggerService {
  constructor(private readonly eventLogger: EventLogger = logEvent) {}

  log(message?: unknown, ..._optionalParams: unknown[]): void {
    if (
      typeof message === 'string' &&
      message.includes('successfully started')
    ) {
      this.eventLogger('application.started');
    }
  }

  warn(_message?: unknown, ..._optionalParams: unknown[]): void {
    this.eventLogger.warn('application.warning');
  }

  error(_message?: unknown, ..._optionalParams: unknown[]): void {
    this.eventLogger.error('application.error');
  }

  debug(_message?: unknown, ..._optionalParams: unknown[]): void {}

  verbose(_message?: unknown, ..._optionalParams: unknown[]): void {}

  fatal(_message?: unknown, ..._optionalParams: unknown[]): void {
    this.eventLogger.fatal('application.fatal');
  }
}

function allowlisted(fields: LogContext): LogContext {
  const { requestId, executionId, stepId, attempt, traceId, outcome, status } =
    fields;
  return {
    ...(requestId === undefined ? {} : { requestId }),
    ...(executionId === undefined ? {} : { executionId }),
    ...(stepId === undefined ? {} : { stepId }),
    ...(attempt === undefined ? {} : { attempt }),
    ...(traceId === undefined ? {} : { traceId }),
    ...(outcome === undefined ? {} : { outcome }),
    ...(status === undefined ? {} : { status }),
  };
}
