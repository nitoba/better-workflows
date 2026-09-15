import { Workflow, WorkflowError, ActivityError } from 'better-workflows';
import type { WorkflowContext } from 'better-workflows';
import { PaperInputSchema, PaperOutcomeSchema } from '../research.schemas.js';
import type { PaperInput, PaperOutcome } from '../research.schemas.js';
import { ArxivActivities } from '../../arxiv/arxiv.activities.js';
import { DocumentActivities } from '../../documents/document.activities.js';
import { AnalysisActivities } from '../../analysis/analysis.activities.js';
import { checkpoint, RunActivities, RunContinue } from '../../runs/run.activities.js';
import type { ArtifactRef } from '../../storage/storage.schemas.js';

@Workflow({ name: 'research.paper', version: 1, input: PaperInputSchema, output: PaperOutcomeSchema, signals: [RunContinue] })
export class PaperWorkflow {
  async run(input: PaperInput, ctx: WorkflowContext): Promise<PaperOutcome> {
    const controls = ctx.activities(RunActivities);
    await controls.register({ runId: input.runId, paperId: input.paper.id, title: input.paper.title }, { stepId: 'register-paper' });
    let analysis: ArtifactRef | null = null;
    let review: ArtifactRef | null = null;
    let rounds = 0;
    let outcome: PaperOutcome;
    try {
      await checkpoint(ctx, input.runId, 'before-download');
      const pdf = await ctx.activities(ArxivActivities).download({ runId: input.runId, paper: input.paper }, { stepId: 'download-pdf' });
      await checkpoint(ctx, input.runId, 'before-extraction');
      const document = await ctx.activities(DocumentActivities).extract({ runId: input.runId, paperId: input.paper.id, pdf }, { stepId: 'extract-markdown' });
      await checkpoint(ctx, input.runId, 'before-evidence');
      const notes = await ctx.map('extract-evidence', document.chunks, { key: chunk => chunk.id, concurrency: 2 },
        (chunk, branch) => branch.activities(AnalysisActivities).notes({ runId: input.runId, paperId: input.paper.id,
          model: input.analysisModel, language: input.language, chunk }, { stepId: 'chunk-evidence' }));
      let approved = false;
      let reason = 'Maximum review rounds reached without approval';
      const llm = ctx.activities(AnalysisActivities);
      for (let round = 1; round <= input.maxReviewRounds; round++) {
        await checkpoint(ctx, input.runId, `before-author-${round}`);
        analysis = await llm.draft({ runId: input.runId, paperId: input.paper.id, paper: input.paper,
          model: input.analysisModel, language: input.language, document, notes, round, previous: analysis, review }, { stepId: `author-${round}` });
        await checkpoint(ctx, input.runId, `before-judge-${round}`);
        const judgement = await llm.judge({ runId: input.runId, paperId: input.paper.id, model: input.judgeModel,
          language: input.language, document, draft: analysis, round }, { stepId: `judge-${round}` });
        rounds = round; review = judgement.artifact; approved = judgement.quality.approved;
        reason = judgement.quality.reviewer.reasoning;
        if (judgement.quality.evidenceErrors.length) reason += `; ${judgement.quality.evidenceErrors.join('; ')}`;
        if (judgement.quality.terminal) break;
        if (round === input.maxReviewRounds) reason = `Review limit reached without approval. ${reason}`;
      }
      outcome = { paperId: input.paper.id, title: input.paper.title, executionId: ctx.executionId,
        status: approved ? 'approved' : 'rejected', analysis, review, rounds, reason };
    } catch (error) {
      // Infra interruption/suspension is not delivered to this catch by the library.
      // Known per-paper business failures become visible outcomes in the final digest.
      if (!(error instanceof WorkflowError || error instanceof ActivityError) || error.code === 'RUN_INTERRUPTED') throw error;
      outcome = { paperId: input.paper.id, title: input.paper.title, executionId: ctx.executionId,
        status: 'failed', analysis, review, rounds, reason: `${error.code}: ${error.message}` };
    }
    await controls.finish(outcome, { stepId: 'record-outcome' });
    return outcome;
  }
}
