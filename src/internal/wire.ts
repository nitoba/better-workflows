import { Schema } from 'effect'
import { DurableDeferred, DurableQueue, Workflow } from 'effect/unstable/workflow'

export const FailureSchema = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
  retryable: Schema.Boolean
})

export function workflowDefinition(namespace: string, name: string, version: number) {
  return Workflow.make(
    `better-workflows/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/${version}`,
    {
      payload: { key: Schema.String, input: Schema.String },
      success: Schema.String,
      error: FailureSchema,
      idempotencyKey: (payload) => payload.key
    }
  )
}

export type EngineWorkflow = ReturnType<typeof workflowDefinition>

export function activityQueue(namespace: string, queue: string, name: string, version: number) {
  return DurableQueue.make({
    name: `better-workflows/${encodeURIComponent(namespace)}/${encodeURIComponent(queue)}/${encodeURIComponent(name)}/${version}`,
    payload: {
      executionId: Schema.String,
      stepId: Schema.String,
      name: Schema.String,
      version: Schema.Number,
      input: Schema.String,
      attempt: Schema.Number,
      timeoutMs: Schema.Number,
      maxAttempts: Schema.Number,
      retryDelayMs: Schema.Number,
      concurrencyKey: Schema.optional(Schema.String)
    },
    success: Schema.String,
    error: FailureSchema,
    idempotencyKey: (payload) => JSON.stringify([payload.stepId, payload.attempt])
  })
}

export type EngineQueue = ReturnType<typeof activityQueue>
export type ActivityEnvelope = EngineQueue['payloadSchema']['Type']

export function signalDeferred(stepId: string) {
  return DurableDeferred.make(`better-workflows/signal/${encodeURIComponent(stepId)}`, {
    success: Schema.String,
    error: FailureSchema
  })
}

export function retryDeferred(stepId: string, attempt: number) {
  return DurableDeferred.make(`better-workflows/retry/${encodeURIComponent(stepId)}/${attempt}`)
}

export function childDeferred(stepId: string) {
  return DurableDeferred.make(`better-workflows/child/${encodeURIComponent(stepId)}`, {
    success: Schema.String,
    error: FailureSchema
  })
}
export function timerDeferred(stepId: string) {
  return DurableDeferred.make(`better-workflows/timer/${encodeURIComponent(stepId)}`)
}
