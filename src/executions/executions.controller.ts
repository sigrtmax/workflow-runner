import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Param,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import type { Response } from 'express';
import {
  EventsQuery,
  ExecutionIdParams,
  LaunchExecutionDto,
  RetryExecutionDto,
} from './execution.dto';
import { ExecutionsService } from './executions.service';

@Controller('executions')
export class ExecutionsController {
  constructor(private readonly executions: ExecutionsService) {}

  @Post()
  async launch(
    @Body() body: LaunchExecutionDto,
    @Headers('idempotency-key') key: string | undefined,
    @Res({ passthrough: true }) response: Response,
  ) {
    const launched = await this.executions.launch(
      body.workflowName,
      body.version,
      body.input,
      key,
    );
    response.status(launched.created ? 201 : 200);
    return launched;
  }

  @Get(':id')
  get(@Param() params: ExecutionIdParams) {
    return this.executions.get(params.id);
  }

  @Get(':id/events')
  events(@Param() params: ExecutionIdParams, @Query() query: EventsQuery) {
    return this.executions.events(params.id, query.after, query.limit);
  }

  @Post(':id/pause')
  @HttpCode(200)
  pause(@Param() params: ExecutionIdParams) {
    return this.executions.pause(params.id);
  }

  @Post(':id/resume')
  @HttpCode(200)
  resume(@Param() params: ExecutionIdParams) {
    return this.executions.resume(params.id);
  }

  @Post(':id/retry')
  @HttpCode(200)
  retry(@Param() params: ExecutionIdParams, @Body() body: RetryExecutionDto) {
    return this.executions.retry(params.id, body.confirmUnknownOutcome);
  }
}
