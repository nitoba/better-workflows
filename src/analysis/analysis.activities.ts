import { Inject } from '@nestjs/common';
import { Activities, Activity, defineQueue } from 'better-workflows';
import type { ActivityContext } from 'better-workflows';
import { z } from 'zod';
import { LLM_GATEWAY } from './llm.gateway.js';
import type { LlmGateway, LlmJob } from './llm.gateway.js';
import { AUTHOR_PROMPT, EXTRACT_PROMPT, JUDGE_PROMPT } from './prompts.js';
import { AnalysisSchema, ChunkNotesSchema, QualityReportSchema, ReviewSchema } from './analysis.schemas.js';
import { evaluateAnalysis } from './quality.js';
import { ArtifactRefSchema } from '../storage/storage.schemas.js';
import type { ArtifactRef } from '../storage/storage.schemas.js';
import { ArtifactsService } from '../storage/artifacts.service.js';
import { AppDatabase } from '../storage/database.js';
import { DocumentSchema, ChunkSchema } from '../documents/document.schemas.js';
import { PaperSchema } from '../arxiv/arxiv.schemas.js';
import { LanguageSchema } from '../research/research.schemas.js';
import { executeActivity } from '../common/failure.js';

export const LlmQueue = defineQueue('analysis.google');
const Base = z.object({ runId: z.string().uuid(), paperId: z.string(), model: z.string(), language: LanguageSchema });
const NotesInput = Base.extend({ chunk: ChunkSchema });
const DraftInput = Base.extend({ paper: PaperSchema, document: DocumentSchema, notes: z.array(ArtifactRefSchema),
  round: z.number().int().positive(), previous: ArtifactRefSchema.nullable(), review: ArtifactRefSchema.nullable() });
const JudgeInput = Base.extend({ document: DocumentSchema, draft: ArtifactRefSchema, round: z.number().int().positive() });
const Reviewed = z.object({ artifact: ArtifactRefSchema, quality: QualityReportSchema });

@Activities({ queue: LlmQueue, timeout: '4m', retry: { maxAttempts: 3, backoff: 'exponential', initialDelay: '10s', maxDelay: '1m' } })
export class AnalysisActivities {
  constructor(@Inject(LLM_GATEWAY) private readonly llm: LlmGateway,
    private readonly files: ArtifactsService, private readonly database: AppDatabase) {}

  private async source(document: z.infer<typeof DocumentSchema>) {
    return Promise.all(document.chunks.map(async chunk => ({ id: chunk.id, text: await this.files.text(chunk.artifact) })));
  }
  private async generate<T>(runId: string, label: string, schema: z.ZodType<T>, job: LlmJob, ctx: ActivityContext): Promise<ArtifactRef> {
    const cached = this.database.getReceipt(ctx.idempotencyKey);
    if (cached) { const ref = ArtifactRefSchema.parse(cached); await this.files.read(ref); return ref; }
    await ctx.heartbeat({ role: job.role, model: job.model });
    const result = await this.llm.complete(schema, job, ctx.signal);
    ctx.signal.throwIfAborted();
    const ref = await this.files.put(runId, label, JSON.stringify(result.data, null, 2), 'application/json');
    await this.files.put(runId, `${label}.usage.json`, JSON.stringify({ model: job.model, role: job.role, usage: result.usage }, null, 2), 'application/json');
    this.database.putReceipt(ctx.idempotencyKey, ref);
    return ref;
  }
  @Activity({ name: 'analysis.extract-evidence', version: 1, input: NotesInput, output: ArtifactRefSchema, key: input => input.runId })
  notes(input: z.infer<typeof NotesInput>, ctx: ActivityContext): Promise<ArtifactRef> {
    return executeActivity(ctx, async () => this.generate(input.runId, `${input.paperId}/notes/${input.chunk.id}.json`, ChunkNotesSchema, {
      role: 'extract', model: input.model, system: EXTRACT_PROMPT,
      prompt: JSON.stringify({ language: input.language, chunkId: input.chunk.id, source: await this.files.text(input.chunk.artifact) }),
    }, ctx));
  }
  @Activity({ name: 'analysis.write-summary', version: 1, input: DraftInput, output: ArtifactRefSchema, key: input => input.runId })
  draft(input: z.infer<typeof DraftInput>, ctx: ActivityContext): Promise<ArtifactRef> {
    return executeActivity(ctx, async () => this.generate(input.runId, `${input.paperId}/round-${input.round}/analysis.json`, AnalysisSchema, {
      role: 'author', model: input.model, system: AUTHOR_PROMPT,
      prompt: JSON.stringify({ language: input.language, metadata: input.paper, sourceChunks: await this.source(input.document),
        extraction: JSON.parse(await this.files.text(input.document.extraction)),
        notes: await Promise.all(input.notes.map(async note => ChunkNotesSchema.parse(JSON.parse(await this.files.text(note))))),
        previousDraft: input.previous ? AnalysisSchema.parse(JSON.parse(await this.files.text(input.previous))) : null,
        review: input.review ? QualityReportSchema.parse(JSON.parse(await this.files.text(input.review))) : null }),
    }, ctx));
  }
  @Activity({ name: 'analysis.judge-summary', version: 1, input: JudgeInput, output: Reviewed, key: input => input.runId })
  judge(input: z.infer<typeof JudgeInput>, ctx: ActivityContext) {
    return executeActivity(ctx, async () => {
      const cached = this.database.getReceipt(`judge:${ctx.idempotencyKey}`);
      if (cached) return Reviewed.parse(cached);
      const source = await this.source(input.document);
      const draft = AnalysisSchema.parse(JSON.parse(await this.files.text(input.draft)));
      const assessment = await this.generate(input.runId, `${input.paperId}/round-${input.round}/judge.json`, ReviewSchema, {
        role: 'judge', model: input.model, system: JUDGE_PROMPT,
        prompt: JSON.stringify({ language: input.language, sourceChunks: source, draft }),
      }, ctx);
      const quality = evaluateAnalysis(draft, ReviewSchema.parse(JSON.parse(await this.files.text(assessment))), source);
      const artifact = await this.files.put(input.runId, `${input.paperId}/round-${input.round}/quality.json`, JSON.stringify(quality, null, 2), 'application/json');
      const result = { artifact, quality };
      this.database.putReceipt(`judge:${ctx.idempotencyKey}`, result);
      return result;
    });
  }
}
