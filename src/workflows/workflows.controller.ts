import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { Type } from 'class-transformer';
import { IsInt, Matches, Max, Min } from 'class-validator';
import { PublishWorkflowDto } from './workflow.dto';
import { WorkflowsService } from './workflows.service';

class WorkflowParams {
  @Matches(/^[a-z][a-z0-9_]{0,63}$/)
  name!: string;
}

class WorkflowVersionParams extends WorkflowParams {
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(2_147_483_647)
  version!: number;
}

@Controller('workflows')
export class WorkflowsController {
  constructor(private readonly workflows: WorkflowsService) {}

  @Post(':name/versions')
  publish(@Param() params: WorkflowParams, @Body() body: PublishWorkflowDto) {
    return this.workflows.publish(params.name, body.version, body.definition);
  }

  @Get(':name/versions/:version')
  get(@Param() params: WorkflowVersionParams) {
    return this.workflows.get(params.name, params.version);
  }
}
