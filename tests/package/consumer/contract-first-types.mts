import assert from 'node:assert/strict'
import {
  Activities,
  ActivitiesContract,
  Activity,
  WorkflowContract,
  defineQueue
} from 'better-workflows'
import type {
  ActivityContext,
  ActivityClient,
  ActivityImplementationClass,
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
export type VerifiedExecute = AssertAssignable<Execute>

@Activities(PublishedActivitiesContract)
class PublishedActivitiesHandler implements PublishedActivitiesContract {
  async execute(
    input: z.infer<typeof activityInputSchema>,
    _context: ActivityContext
  ): Promise<string> {
    return input.id
  }
}

const publishedHandler: ActivityImplementationClass<typeof PublishedActivitiesContract> =
  PublishedActivitiesHandler
assert.equal(publishedHandler, PublishedActivitiesHandler)

// @ts-expect-error Handler methods must preserve the contract input and output types.
@Activities(PublishedActivitiesContract)
class InvalidActivitiesHandler {
  async execute(_input: number, _context: ActivityContext): Promise<boolean> {
    return false
  }
}

// @ts-expect-error Every decorated contract method is required on the implementation.
@Activities(PublishedActivitiesContract)
class MissingActivitiesHandler {}

assert.ok(InvalidActivitiesHandler)
assert.ok(MissingActivitiesHandler)
