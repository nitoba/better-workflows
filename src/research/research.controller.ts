import { Body, Controller, Get, HttpCode, Param, ParseUUIDPipe, Post, Query, Res, UseGuards } from '@nestjs/common';
import type { FastifyReply } from 'fastify';
import { z } from 'zod';
import { ApiTokenGuard } from '../http/api-token.guard.js';
import { ArtifactsService } from '../storage/artifacts.service.js';
import { RunRepository } from '../runs/run.repository.js';
import { ResearchService } from './research.service.js';
import { CreateResearchSchema } from './research.schemas.js';
import type { CreateResearch } from './research.schemas.js';
const ListQuery = z.object({ limit: z.coerce.number().int().min(1).max(100).default(50) });
const HistoryQuery = z.object({ after: z.coerce.number().int().nonnegative().optional() });
@Controller('research-runs')
@UseGuards(ApiTokenGuard)
export class ResearchController {
  constructor(private readonly research: ResearchService, private readonly files: ArtifactsService, private readonly runs: RunRepository) {}
  @Post() @HttpCode(202)
  start(@Body({ schema: CreateResearchSchema }) input: CreateResearch) { return this.research.start(input); }
  @Get()
  list(@Query({ schema: ListQuery }) query: z.infer<typeof ListQuery>) { return this.research.list(query.limit); }
  @Get(':id')
  status(@Param('id', new ParseUUIDPipe()) id: string) { return this.research.status(id); }
  @Get(':id/result')
  result(@Param('id', new ParseUUIDPipe()) id: string) { return this.research.result(id); }
  @Post(':id/pause') @HttpCode(202)
  pause(@Param('id', new ParseUUIDPipe()) id: string) { return this.research.control(id, 'paused'); }
  @Post(':id/resume') @HttpCode(202)
  resume(@Param('id', new ParseUUIDPipe()) id: string) { return this.research.control(id, 'running'); }
  @Post(':id/interrupt') @HttpCode(202)
  interrupt(@Param('id', new ParseUUIDPipe()) id: string) { return this.research.control(id, 'interrupted'); }
  @Get(':id/history')
  history(@Param('id', new ParseUUIDPipe()) id: string, @Query({ schema: HistoryQuery }) query: z.infer<typeof HistoryQuery>) {
    return this.research.history(id, query.after);
  }
  @Get(':id/papers/:executionId/history')
  paperHistory(@Param('id', new ParseUUIDPipe()) id: string, @Param('executionId') executionId: string,
    @Query({ schema: HistoryQuery }) query: z.infer<typeof HistoryQuery>) { return this.research.paperHistory(id, executionId, query.after); }
  @Get(':id/artifacts')
  artifacts(@Param('id', new ParseUUIDPipe()) id: string) { this.runs.get(id); return this.files.list(id); }
  @Get(':id/artifacts/:key')
  async artifact(@Param('id', new ParseUUIDPipe()) id: string, @Param('key') key: string, @Res() reply: FastifyReply) {
    const ref = this.files.record(id, key);
    return reply.header('X-Content-Type-Options', 'nosniff').header('Content-Disposition', `attachment; filename="${key}"`)
      .type(ref.mediaType).send(await this.files.read(ref));
  }
}
