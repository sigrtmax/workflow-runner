import { Type, Transform } from 'class-transformer';
import {
  Allow,
  IsBoolean,
  IsInt,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from 'class-validator';

export class LaunchExecutionDto {
  @Matches(/^[a-z][a-z0-9_]{0,63}$/)
  workflowName!: string;

  @IsInt()
  @Min(1)
  @Max(2_147_483_647)
  version!: number;

  @Allow()
  @Transform(({ obj }: { obj: Record<string, unknown> }) => obj.input, {
    toClassOnly: true,
  })
  input!: unknown;
}

export class RetryExecutionDto {
  @IsOptional()
  @IsBoolean()
  confirmUnknownOutcome?: boolean;
}

export class ExecutionIdParams {
  @IsString()
  @Matches(
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
  )
  id!: string;
}

export class EventsQuery {
  @IsOptional()
  @Matches(/^\d+$/)
  after?: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(100)
  @Type(() => Number)
  limit?: number;
}
