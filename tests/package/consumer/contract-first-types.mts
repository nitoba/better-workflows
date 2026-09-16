import assert from 'node:assert/strict'
import { WorkflowContract } from 'better-workflows'
import type { WorkflowContext, WorkflowInput, WorkflowOutput } from 'better-workflows'
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
