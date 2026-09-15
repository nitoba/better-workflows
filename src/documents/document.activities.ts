import { Activities, Activity, defineQueue } from 'better-workflows';
import type { ActivityContext } from 'better-workflows';
import { z } from 'zod';
import { ArtifactRefSchema } from '../storage/storage.schemas.js';
import { DocumentSchema } from './document.schemas.js';
import { LiteparseService } from './liteparse.service.js';
import { ArtifactsService } from '../storage/artifacts.service.js';
import { AppDatabase } from '../storage/database.js';
import { AppSettings } from '../config/settings.js';
import { executeActivity } from '../common/failure.js';
import { splitMarkdown } from './chunker.js';
const ExtractInput = z.object({ runId: z.string().uuid(), paperId: z.string(), pdf: ArtifactRefSchema });
export const ParseQueue = defineQueue('documents.liteparse');

@Activities({ queue: ParseQueue, timeout: '4m' })
export class DocumentActivities {
  constructor(private readonly parser: LiteparseService, private readonly files: ArtifactsService,
    private readonly database: AppDatabase, private readonly settings: AppSettings) {}
  @Activity({ name: 'documents.extract-markdown', version: 1, input: ExtractInput, output: DocumentSchema })
  extract(input: z.infer<typeof ExtractInput>, ctx: ActivityContext) {
    return executeActivity(ctx, async () => {
      const cached = this.database.getReceipt(ctx.idempotencyKey);
      if (cached) return DocumentSchema.parse(cached);
      await ctx.heartbeat({ phase: 'parsing', paperId: input.paperId });
      const result = await this.parser.parse(input.pdf, ctx.signal);
      const markdown = await this.files.put(input.runId, `${input.paperId}/document.md`, result.markdown, 'text/markdown');
      const extraction = await this.files.put(input.runId, `${input.paperId}/extraction.json`, JSON.stringify({ totalPages: result.totalPages, warnings: result.warnings, parser: '@llamaindex/liteparse', outputFormat: 'markdown' }, null, 2), 'application/json');
      const chunks = [];
      for (const chunk of splitMarkdown(result.markdown, this.settings.env.CHUNK_CHARS)) {
        ctx.signal.throwIfAborted();
        chunks.push({ id: chunk.id, index: chunk.index, start: chunk.start, end: chunk.end,
          artifact: await this.files.put(input.runId, `${input.paperId}/${chunk.id}.md`, chunk.text, 'text/markdown') });
      }
      const document = DocumentSchema.parse({ markdown, extraction, totalPages: result.totalPages, characters: result.markdown.length, chunks });
      this.database.putReceipt(ctx.idempotencyKey, document);
      return document;
    });
  }
}
