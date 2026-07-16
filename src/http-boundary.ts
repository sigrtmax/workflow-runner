import {
  ArgumentMetadata,
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Injectable,
  PipeTransform,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import { randomUUID } from 'node:crypto';
import {
  ExecutionConflict,
  ExecutionNotFound,
} from './executions/executions.repository';
import { logEvent } from './logging';
import { DefinitionError, validateJsonData } from './workflows/definition';
import {
  LaunchExecutionDto,
  RetryExecutionDto,
} from './executions/execution.dto';
import { PublishWorkflowDto } from './workflows/workflow.dto';
import {
  VersionConflict,
  WorkflowNotFound,
} from './workflows/workflows.repository';

const REQUEST_ID = /^[a-zA-Z0-9_-]{1,64}$/;
const REQUEST_ID_CACHE = Symbol('requestId');

@Injectable()
export class RawPayloadValidationPipe implements PipeTransform {
  transform(value: unknown, metadata: ArgumentMetadata): unknown {
    if (metadata.type !== 'body' || !value || typeof value !== 'object')
      return value;
    const body = value as Record<string, unknown>;
    // Validate original keys: DTO conversion can discard inherited method names.
    const allowed =
      metadata.metatype === LaunchExecutionDto
        ? ['workflowName', 'version', 'input']
        : metadata.metatype === PublishWorkflowDto
          ? ['version', 'definition']
          : metadata.metatype === RetryExecutionDto
            ? ['confirmUnknownOutcome']
            : undefined;
    if (allowed && Object.keys(body).some((key) => !allowed.includes(key)))
      throw new DefinitionError('Unknown request property');
    if (metadata.metatype === LaunchExecutionDto)
      validateJsonData(body.input, 'input');
    if (metadata.metatype === PublishWorkflowDto)
      validateJsonData(body.definition, 'definition');
    return value;
  }
}

export function requestId(request: Request): string {
  const cached = (request as Request & { [REQUEST_ID_CACHE]?: string })[
    REQUEST_ID_CACHE
  ];
  if (cached) return cached;
  const inbound = request.header('x-request-id');
  const id = inbound && REQUEST_ID.test(inbound) ? inbound : randomUUID();
  (request as Request & { [REQUEST_ID_CACHE]?: string })[REQUEST_ID_CACHE] = id;
  return id;
}

@Catch()
export class ApiExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const context = host.switchToHttp();
    const request = context.getRequest<Request>();
    const response = context.getResponse<Response>();
    const id = requestId(request);
    const mapped = mapException(exception);
    response.setHeader('X-Request-Id', id);
    logEvent('request.failed', { requestId: id, status: mapped.status });
    response.status(mapped.status).json({
      code: mapped.code,
      message: mapped.message,
      requestId: id,
    });
  }
}

function mapException(exception: unknown): {
  status: number;
  code: string;
  message: string;
} {
  if (exception instanceof DefinitionError)
    return {
      status: HttpStatus.BAD_REQUEST,
      code: 'invalid_definition',
      message: 'Invalid workflow definition',
    };
  if (exception instanceof VersionConflict)
    return {
      status: HttpStatus.CONFLICT,
      code: 'version_conflict',
      message: 'Workflow version already exists',
    };
  if (exception instanceof ExecutionConflict)
    return {
      status: HttpStatus.CONFLICT,
      code: 'execution_conflict',
      message: exception.message,
    };
  if (
    exception instanceof WorkflowNotFound ||
    exception instanceof ExecutionNotFound
  )
    return {
      status: HttpStatus.NOT_FOUND,
      code: 'not_found',
      message: 'Resource was not found',
    };
  if (exception instanceof HttpException) {
    const status = exception.getStatus();
    return {
      status,
      code:
        HttpStatus[status]?.toLowerCase().replaceAll(' ', '_') ?? 'http_error',
      message:
        status === HttpStatus.PAYLOAD_TOO_LARGE
          ? 'Request body is too large'
          : 'Request validation failed',
    };
  }
  const status =
    exception && typeof exception === 'object' && 'status' in exception
      ? (exception as { status?: unknown }).status
      : undefined;
  if (
    status === HttpStatus.BAD_REQUEST ||
    status === HttpStatus.PAYLOAD_TOO_LARGE
  )
    return {
      status,
      code:
        status === HttpStatus.PAYLOAD_TOO_LARGE
          ? 'payload_too_large'
          : 'invalid_request',
      message:
        status === HttpStatus.PAYLOAD_TOO_LARGE
          ? 'Request body is too large'
          : 'Request validation failed',
    };
  return {
    status: HttpStatus.INTERNAL_SERVER_ERROR,
    code: 'internal_error',
    message: 'Internal server error',
  };
}
