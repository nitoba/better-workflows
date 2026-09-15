import { Activities, Activity, defineQueue, defineSignal } from 'better-workflows';
import type { ActivityContext, WorkflowContext } from 'better-workflows';
import { WorkflowError } from 'better-workflows';
import { z } from 'zod';
import { RunRepository } from './run.repository.js';
import { PaperOutcomeSchema } from '../research/research.schemas.js';
import type { PaperOutcome } from '../research/research.schemas.js';
import { executeActivity } from '../common/failure.js';
export const RunContinue = defineSignal('research.continue', z.object({ revision: z.number().int().nonnegative() }));
export const ControlQueue = defineQueue('research.control');
const ControlInput = z.object({ runId: z.string().uuid() });
const ChildInput = ControlInput.extend({ paperId: z.string(), title: z.string() });
const ControlResult = z.object({ desired: z.enum(['running', 'paused', 'interrupted']), revision: z.number().int() });
@Activities({ queue: ControlQueue, timeout: '30s' })
export class RunActivities {
  constructor(private readonly runs: RunRepository) {}
  @Activity({ name: 'research.register-paper', version: 1, input: ChildInput, output: z.void() })
  register(input: z.infer<typeof ChildInput>, ctx: ActivityContext): Promise<void> {
    return executeActivity(ctx, async () => { this.runs.registerChild(input.runId, ctx.executionId, input.paperId, input.title); });
  }
  @Activity({ name: 'research.check-control', version: 1, input: ControlInput, output: ControlResult })
  control(input: z.infer<typeof ControlInput>, ctx: ActivityContext) {
    return executeActivity(ctx, async () => { const run = this.runs.get(input.runId); return { desired: run.desired, revision: run.revision }; });
  }
  @Activity({ name: 'research.record-paper', version: 1, input: PaperOutcomeSchema, output: z.void() })
  finish(input: PaperOutcome, ctx: ActivityContext): Promise<void> {
    return executeActivity(ctx, async () => { this.runs.finishPaper(input); });
  }
}

export async function checkpoint(ctx: WorkflowContext, runId: string, name: string): Promise<void> {
  for (let observation = 0; ; observation++) {
    const control = await ctx.activities(RunActivities).control({ runId }, { stepId: `${name}-observe-${observation}` });
    if (control.desired === 'running') return;
    if (control.desired === 'interrupted') throw new WorkflowError('RUN_INTERRUPTED', 'Research run interrupted');
    await ctx.waitForSignal(`${name}-continue-${observation}`, RunContinue);
  }
}
