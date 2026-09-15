import { Injectable, ConflictException, ServiceUnavailableException, Logger } from '@nestjs/common';
import { InjectWorkflow, WorkflowClient, WorkflowError } from 'better-workflows';
import type { WorkflowHandle, WorkflowClass } from 'better-workflows';
import { AppSettings } from '../config/settings.js';
import { AppDatabase } from '../storage/database.js';
import { Mutex } from '../common/mutex.js';
import { RunRepository } from '../runs/run.repository.js';
import type { DesiredState, RunRow } from '../runs/run.repository.js';
import { RunContinue } from '../runs/run.activities.js';
import { ResearchWorkflow } from './workflows/research.workflow.js';
import { PaperWorkflow } from './workflows/paper.workflow.js';
import { ResearchInputSchema, PaperOutcomeSchema } from './research.schemas.js';
import type { CreateResearch } from './research.schemas.js';

const terminal = (status: string) => ['completed', 'failed', 'cancelled'].includes(status);
@Injectable()
export class ResearchService {
  private readonly controls = new Mutex();
  private readonly logger = new Logger(ResearchService.name);
  constructor(private readonly runs: RunRepository, private readonly settings: AppSettings,
    private readonly database: AppDatabase,
    @InjectWorkflow(ResearchWorkflow) readonly research: WorkflowClient<typeof ResearchWorkflow>,
    @InjectWorkflow(PaperWorkflow) readonly papers: WorkflowClient<typeof PaperWorkflow>) {}

  async start(request: CreateResearch) {
    if (!this.settings.env.GOOGLE_GENERATIVE_AI_API_KEY) throw new ServiceUnavailableException('Set GOOGLE_GENERATIVE_AI_API_KEY before starting research');
    const input = ResearchInputSchema.parse({ ...request, query: request.query ?? null, arxivIds: request.arxivIds ?? [],
      analysisModel: this.settings.env.ANALYSIS_MODEL, judgeModel: this.settings.env.JUDGE_MODEL,
      paperConcurrency: this.settings.env.PAPER_CONCURRENCY });
    this.runs.create(input); // Durable application intent. Reconciliation closes the crash window before start/bind.
    const handle = await this.research.start(input);
    this.runs.bind(input.requestId, handle.executionId);
    return { runId: input.requestId, executionId: handle.executionId, created: handle.created,
      statusUrl: `/api/research-runs/${input.requestId}` };
  }
  list(limit: number) {
    return this.runs.recent(limit).map(run => ({ runId: run.id, executionId: run.execution_id,
      desiredState: run.desired, terminal: Boolean(run.terminal), createdAt: run.created_at }));
  }
  async status(id: string) {
    const run = this.runs.get(id);
    return { runId: id, desiredState: run.desired, controlRevision: run.revision,
      workflow: run.execution_id ? await this.research.getHandle(run.execution_id).describe() : null,
      papers: await Promise.all(this.runs.children(id).map(async child => ({
        paperId: child.paper_id, title: child.title, executionId: child.execution_id,
        workflow: await this.papers.getHandle(child.execution_id).describe(),
        outcome: child.result_json ? PaperOutcomeSchema.parse(JSON.parse(child.result_json)) : null,
      }))) };
  }
  async result(id: string) {
    const run = this.runs.get(id);
    if (!run.execution_id) return { ready: false, status: 'accepted' };
    const handle = this.research.getHandle(run.execution_id);
    const snapshot = await handle.describe();
    if (snapshot.status !== 'completed') return { ready: false, status: snapshot.status, failure: snapshot.failure ?? null };
    return { ready: true, result: await handle.result({ timeout: '1s' }) };
  }
  async control(id: string, desired: DesiredState) {
    // Serialize local commands and reconciliation so resume cannot overtake pause.
    return this.controls.use(async () => {
      const existing = this.runs.get(id);
      if (existing.execution_id && terminal((await this.research.getHandle(existing.execution_id).describe()).status)) {
        throw new ConflictException('Finished workflows cannot be paused or resumed; create a new requestId');
      }
      const run = this.runs.setDesired(id, desired);
      try { await this.apply(run); }
      catch (error) {
        // Intent is committed; retry on the next reconciliation pass, including after restart.
        this.logger.warn(`Control propagation pending for run ${id}: ${error instanceof WorkflowError ? error.code : 'infrastructure error'}`);
      }
      return { runId: id, desiredState: desired, controlRevision: run.revision, propagation: 'requested' };
    });
  }
  async reconcile(runId: string): Promise<void> {
    await this.controls.use(async () => {
      let run = this.runs.get(runId);
      if (!run.execution_id) {
        const handle = await this.research.start(this.runs.input(runId));
        this.runs.bind(runId, handle.executionId);
        run = this.runs.get(runId);
      }
      await this.apply(run);
    });
  }
  private async apply(run: RunRow): Promise<void> {
    if (!run.execution_id) return;
    const parent = this.research.getHandle(run.execution_id);
    const parentDone = await this.applyHandle(parent, run);
    let childrenDone = true;
    for (const child of this.runs.children(run.id)) {
      childrenDone = (await this.applyHandle(this.papers.getHandle(child.execution_id), run)) && childrenDone;
    }
    if (parentDone && childrenDone) this.runs.markTerminal(run.id);
  }
  private async applyHandle<W extends WorkflowClass>(handle: WorkflowHandle<W>, run: RunRow): Promise<boolean> {
    const snapshot = await handle.describe();
    if (terminal(snapshot.status)) return true;
    const receiptKey = `control/${handle.executionId}/${run.revision}`;
    if (run.desired === 'paused') {
      if (snapshot.status !== 'paused') await handle.pause();
    } else if (run.desired === 'interrupted') {
      if (snapshot.status !== 'cancelling') await handle.cancel({ reason: 'Research run interrupted via HTTP' });
    } else {
      if (snapshot.status === 'paused') await handle.resume();
      if (run.revision > 0 && !this.database.getReceipt(receiptKey)) {
        await handle.signal(RunContinue, { revision: run.revision }, { idempotencyKey: `resume-${run.revision}` });
        this.database.putReceipt(receiptKey, true);
      }
    }
    return false;
  }
  async history(id: string, after?: number) {
    const run = this.runs.get(id);
    if (!run.execution_id) return { events: [], nextCursor: null };
    return this.research.getHandle(run.execution_id).history({ after, limit: 100 });
  }
  async paperHistory(id: string, executionId: string, after?: number) {
    const child = this.runs.children(id).find(row => row.execution_id === executionId);
    if (!child) throw new ConflictException('Paper execution does not belong to this research run');
    return this.papers.getHandle(executionId).history({ after, limit: 100 });
  }
}
