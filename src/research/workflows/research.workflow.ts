import { Workflow } from 'better-workflows';
import type { WorkflowContext } from 'better-workflows';
import { ResearchInputSchema, ResearchResultSchema } from '../research.schemas.js';
import type { ResearchInput, ResearchResult } from '../research.schemas.js';
import { ArxivActivities } from '../../arxiv/arxiv.activities.js';
import { DeliveryActivities } from '../../delivery/delivery.activities.js';
import { checkpoint, RunContinue } from '../../runs/run.activities.js';
import { PaperWorkflow } from './paper.workflow.js';

@Workflow({ name: 'research.arxiv-digest', version: 1, input: ResearchInputSchema, output: ResearchResultSchema,
  signals: [RunContinue], idempotencyKey: input => input.requestId })
export class ResearchWorkflow {
  async run(input: ResearchInput, ctx: WorkflowContext): Promise<ResearchResult> {
    await checkpoint(ctx, input.requestId, 'before-search');
    const papers = await ctx.activities(ArxivActivities).search({ runId: input.requestId, query: input.query,
      arxivIds: input.arxivIds, maxResults: input.maxResults }, { stepId: 'search-arxiv' });
    await checkpoint(ctx, input.requestId, 'before-papers');
    const outcomes = await ctx.map('papers', papers, { key: paper => paper.id, concurrency: input.paperConcurrency },
      (paper, branch) => branch.child('analyze-paper', PaperWorkflow, { runId: input.requestId, paper,
        language: input.language, maxReviewRounds: input.maxReviewRounds, analysisModel: input.analysisModel,
        judgeModel: input.judgeModel }, { parentClosePolicy: 'request-cancel' }));
    await checkpoint(ctx, input.requestId, 'before-digest');
    const delivery = ctx.activities(DeliveryActivities);
    const digest = await delivery.digest({ runId: input.requestId, papers: outcomes, language: input.language }, { stepId: 'build-digest' });
    await checkpoint(ctx, input.requestId, 'before-email');
    const receipt = await delivery.send({ runId: input.requestId, recipient: input.recipient, digest }, { stepId: 'email-digest' });
    return { runId: input.requestId, papers: outcomes, digest: digest.text, messageId: receipt.messageId,
      approved: outcomes.filter(paper => paper.status === 'approved').length,
      rejected: outcomes.filter(paper => paper.status === 'rejected').length,
      failed: outcomes.filter(paper => paper.status === 'failed').length };
  }
}
