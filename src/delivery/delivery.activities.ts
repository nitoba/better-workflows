import { Activities, Activity, defineQueue } from 'better-workflows';
import type { ActivityContext } from 'better-workflows';
import { z } from 'zod';
import { ArtifactRefSchema } from '../storage/storage.schemas.js';
import { ArtifactsService } from '../storage/artifacts.service.js';
import { AppDatabase } from '../storage/database.js';
import { AnalysisSchema } from '../analysis/analysis.schemas.js';
import { LanguageSchema, PaperOutcomeSchema } from '../research/research.schemas.js';
import { executeActivity } from '../common/failure.js';
import { renderDigest } from './render-digest.js';
import { MailService } from './mail.service.js';
export const EmailQueue = defineQueue('delivery.mailpit');
const DigestInput = z.object({ runId: z.string().uuid(), papers: z.array(PaperOutcomeSchema).max(10), language: LanguageSchema });
const Digest = z.object({ text: ArtifactRefSchema, html: ArtifactRefSchema });
const MailInput = z.object({ runId: z.string().uuid(), recipient: z.string().email(), digest: Digest });
@Activities({ queue: EmailQueue, timeout: '1m', retry: { maxAttempts: 3, backoff: 'exponential', initialDelay: '5s', maxDelay: '30s' } })
export class DeliveryActivities {
  constructor(private readonly files: ArtifactsService, private readonly database: AppDatabase, private readonly mail: MailService) {}
  @Activity({ name: 'delivery.build-digest', version: 1, input: DigestInput, output: Digest })
  digest(input: z.infer<typeof DigestInput>, ctx: ActivityContext) {
    return executeActivity(ctx, async () => {
      const papers = await Promise.all(input.papers.map(async outcome => ({ outcome,
        analysis: outcome.analysis ? AnalysisSchema.parse(JSON.parse(await this.files.text(outcome.analysis))) : null })));
      const digest = renderDigest(input.runId, papers, input.language);
      return { text: await this.files.put(input.runId, 'digest.md', digest.text, 'text/markdown'),
        html: await this.files.put(input.runId, 'digest.html.txt', digest.html, 'text/plain') };
    });
  }
  @Activity({ name: 'delivery.send-digest', version: 1, input: MailInput, output: z.object({ messageId: z.string() }) })
  send(input: z.infer<typeof MailInput>, ctx: ActivityContext) {
    return executeActivity(ctx, async () => {
      const cached = this.database.getReceipt(ctx.idempotencyKey);
      if (cached) return z.object({ messageId: z.string() }).parse(cached);
      const messageId = `<${ctx.idempotencyKey}@arxiv-research.local>`;
      const result = { messageId: await this.mail.send({ to: input.recipient,
        subject: `arXiv research digest | ${input.runId}`, messageId,
        text: await this.files.text(input.digest.text), html: await this.files.text(input.digest.html) }, ctx.signal) };
      // SMTP cannot atomically commit with SQLite. A crash after acceptance but
      // before this receipt can duplicate email; Message-ID is diagnostic, not exactly-once.
      this.database.putReceipt(ctx.idempotencyKey, result);
      return result;
    });
  }
}
