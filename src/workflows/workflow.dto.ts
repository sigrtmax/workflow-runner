import { Transform } from 'class-transformer';
import { IsDefined, IsInt, IsObject, Max, Min } from 'class-validator';

export class PublishWorkflowDto {
  @IsInt()
  @Min(1)
  @Max(2_147_483_647)
  version!: number;

  @IsDefined()
  @IsObject()
  @Transform(({ obj }: { obj: Record<string, unknown> }) => obj.definition, {
    toClassOnly: true,
  })
  definition!: object;
}
