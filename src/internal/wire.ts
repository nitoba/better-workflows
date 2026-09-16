import { Schema } from 'effect'
import { DurableDeferred, Workflow } from 'effect/unstable/workflow'

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

/** A physical transport queue is owned by better-workflows. */
export function activityQueue(namespace: string, queue: string) {
  return Object.freeze({
    name: `better-workflows/${encodeURIComponent(namespace)}/activities/${encodeURIComponent(queue)}`
  })
}

export type EngineQueue = ReturnType<typeof activityQueue>

/**
 * Application-owned activity transport. The input remains canonically encoded
 * because a physical queue may contain different activity schemas.
 */
export const ActivityEnvelopeSchema = Schema.Struct({
  token: DurableDeferred.Token,
  activityName: Schema.String,
  activityVersion: Schema.Number,
  executionId: Schema.String,
  stepId: Schema.String,
  input: Schema.String,
  attempt: Schema.Number,
  timeoutMs: Schema.Number,
  maxAttempts: Schema.Number,
  retryDelayMs: Schema.Number,
  concurrencyKey: Schema.optional(Schema.String),
  traceId: Schema.String,
  spanId: Schema.String,
  sampled: Schema.Boolean
})

export type ActivityEnvelope = typeof ActivityEnvelopeSchema.Type

/** The deferred is unique per workflow step and business retry attempt. */
export function activityDeferred(stepId: string, attempt: number) {
  return DurableDeferred.make(
    `better-workflows/activity/${encodeURIComponent(stepId)}/${attempt}`,
    { success: Schema.String, error: FailureSchema }
  )
}

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
