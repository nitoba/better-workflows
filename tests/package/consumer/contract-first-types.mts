import assert from 'node:assert/strict'
import { ActivitiesContract, Activity, WorkflowContract, defineQueue } from 'better-workflows'
import type {
  ActivityContext,
  ActivityClient,
  StepOptions,
  WorkflowContext,
  WorkflowInput,
  WorkflowOutput
} from 'better-workflows'
import { z } from 'zod'

const inputSchema = z.object({ id: z.string() })

@WorkflowContract({
  name: 'published.contract-first',
  version: 1,
  input: inputSchema,
  output: z.string()
})
abstract class PublishedWorkflowContract {
  abstract run(input: z.infer<typeof inputSchema>, context: WorkflowContext): Promise<string>
}

const input: WorkflowInput<typeof PublishedWorkflowContract> = { id: 'published' }
const output: WorkflowOutput<typeof PublishedWorkflowContract> = 'result'
assert.deepEqual(input, { id: 'published' })
assert.equal(output, 'result')

const activityInputSchema = z.object({ id: z.string() })
const PublishedQueue = defineQueue('published')

@ActivitiesContract()
abstract class PublishedActivitiesContract {
  @Activity({
    name: 'published.activity',
    version: 1,
    queue: PublishedQueue,
    input: activityInputSchema,
    output: z.string()
  })
  execute(_input: z.infer<typeof activityInputSchema>, _context: ActivityContext): Promise<string> {
    throw new Error('contract-only')
  }
}

const activityOptions: StepOptions = { stepId: 'published' }
assert.equal(activityOptions.stepId, 'published')
type PublishedActivityClient = ActivityClient<InstanceType<typeof PublishedActivitiesContract>>
type Execute = PublishedActivityClient['execute']
type ExpectedExecute = (
  input: z.infer<typeof activityInputSchema>,
  options: StepOptions
) => Promise<string>
type AssertAssignable<T extends ExpectedExecute> = T
type VerifiedExecute = AssertAssignable<Execute>
void (null as unknown as VerifiedExecute)
