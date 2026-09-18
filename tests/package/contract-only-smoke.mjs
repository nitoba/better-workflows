import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { WorkflowsModule, getWorkflowToken } from 'better-workflows'
import { sqlite } from 'better-workflows/sqlite'
import { BatchWorkflow, EvenActivities, OddActivities, queues } from './advanced-contracts.mjs'
import { Batch, Child } from './advanced-workflow-handlers.mjs'

const orchestratorSource = await readFile(
  new URL('./advanced-workflow-handlers.mjs', import.meta.url),
  'utf8'
)
assert.doesNotMatch(orchestratorSource, /advanced-activity-handlers/u)

class Orchestrator {}
Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'contract-only-package',
      storage: sqlite({ filename: ':memory:' }),
      execution: { activities: { enabled: false } }
    }),
    WorkflowsModule.forFeature({
      name: 'contract-only-package',
      workflows: [Batch, Child],
      activityContracts: [EvenActivities, OddActivities],
      queues
    })
  ]
})(Orchestrator)

const app = await NestFactory.createApplicationContext(Orchestrator, {
  logger: false,
  abortOnError: false
})
try {
  const client = app.get(getWorkflowToken(BatchWorkflow))
  assert.ok(client)
  assert.equal((await client.start('contract-only')).created, true)
} finally {
  await app.close()
}
