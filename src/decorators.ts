import 'reflect-metadata'
import { Inject, Injectable } from '@nestjs/common'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  ActivityDefaults,
  ActivityOptions,
  SignalDefinition,
  WorkflowClass,
  WorkflowOptions
} from './types'
import { identifier, milliseconds, positiveInteger } from './internal/values'
import { WorkflowError } from './errors'
import { queueName } from './queues'

export const WORKFLOW_METADATA = Symbol.for('better-workflows/workflow')
export const ACTIVITIES_METADATA = Symbol.for('better-workflows/activities')
export const ACTIVITY_METADATA = Symbol.for('better-workflows/activity')
const CLIENT_TOKENS = new WeakMap<WorkflowClass, symbol>()

/**
 * Declare a replayable workflow's name, version, schemas and accepted signals.
 * Also applies Nest Injectable. A decorator alone does not register a handler: add the
 * class to forFeature.workflows, or clients when only a producer contract is needed.
 * @typeParam I - Schema-validated input type.
 * @typeParam O - Schema-validated output type.
 * @param options - Stable identity, schemas, optional key resolver and signals.
 * @returns Class decorator for a singleton with an async run(input, context) method.
 * @throws WorkflowError for invalid identity/version or duplicate signal names.
 * @example
 * ```ts
 * import { Workflow } from 'better-workflows'
 * import type { WorkflowContext } from 'better-workflows'
 * import { z } from 'zod'
 * @Workflow({ name: 'reports.wait', version: 1, input: z.string(), output: z.string(), idempotencyKey: id => id })
 * class WaitForReport {
 *   async run(id: string, ctx: WorkflowContext) {
 *     await ctx.sleep('cooldown', '1s')
 *     return id
 *   }
 * }
 * ```
 */
export function Workflow<I, O>(options: WorkflowOptions<I, O>): ClassDecorator {
  identifier(options.name, 'Workflow name')
  positiveInteger(options.version, 'Workflow version')
  const names = new Set<string>()
  for (const signal of options.signals ?? []) {
    identifier(signal.name, 'Signal name')
    if (names.has(signal.name)) throw new WorkflowError('DUPLICATE_SIGNAL', signal.name)
    names.add(signal.name)
  }
  return (target) => {
    Injectable()(target)
    Reflect.defineMetadata(WORKFLOW_METADATA, Object.freeze({ ...options }), target)
  }
}

/**
 * Declare a Nest activities provider and optional defaults for its activity methods.
 * Also applies Injectable. Defaults override the owner feature and root; individual
 * Activity decorators can override them. A retry policy replaces the inherited object.
 * @param defaults - Queue, timeout and retry defaults; does not register a queue.
 * @returns Class decorator; register the class in forFeature.activities to run workers.
 * @throws WorkflowError for an invalid queue reference, duration or retry policy.
 * @example
 * ```ts
 * import { Activities, Activity, defineQueue } from 'better-workflows'
 * import { z } from 'zod'
 * const Reports = defineQueue('reports')
 * @Activities({ queue: Reports, timeout: '2m', retry: { maxAttempts: 3 } })
 * class ReportActivities {
 *   @Activity({ name: 'reports.total', version: 1, input: z.array(z.number()), output: z.number() })
 *   async total(values: number[]) { return values.reduce((sum, value) => sum + value, 0) }
 * }
 * ```
 */
export function Activities(defaults: ActivityDefaults = {}): ClassDecorator {
  validateActivityDefaults(defaults)
  const settings = freezeActivityDefaults(defaults)
  return (target) => {
    Injectable()(target)
    Reflect.defineMetadata(ACTIVITIES_METADATA, settings, target)
  }
}

/**
 * Declare a durable activity contract on a named instance method.
 * Invoke it through ctx.activities for durable dispatch; a direct service call is just
 * ordinary JavaScript. The worker validates schemas, supplies ActivityContext and records
 * results. External effects still need idempotency because delivery is at least once.
 * @typeParam I - Validated method input.
 * @typeParam O - Serializable method output.
 * @param options - Identity, input/output schemas and optional owner-policy overrides.
 * @returns Method decorator for an async method on an Activities provider.
 * @throws WorkflowError for invalid identity/policy or a static, unnamed or non-method target.
 * @example
 * ```ts
 * import { Activities, Activity, defineQueue } from 'better-workflows'
 * import type { ActivityContext } from 'better-workflows'
 * import { z } from 'zod'
 * const Reports = defineQueue('reports')
 * @Activities({ queue: Reports })
 * class ReportActivities {
 *   @Activity({ name: 'reports.normalize', version: 1, input: z.string(), output: z.string(), timeout: '30s' })
 *   async normalize(text: string, ctx: ActivityContext) {
 *     ctx.signal.throwIfAborted()
 *     return text.trim()
 *   }
 * }
 * ```
 */
export function Activity<I, O>(options: ActivityOptions<I, O>): MethodDecorator {
  identifier(options.name, 'Activity name')
  validateActivityDefaults(options)
  positiveInteger(options.version, 'Activity version')
  return (target, property, descriptor) => {
    // oxlint-disable anti-slop/no-runtime-typeof -- Decorator boundary must reject static methods and non-method descriptors.
    if (
      typeof target === 'function' ||
      typeof descriptor.value !== 'function' ||
      typeof property !== 'string'
    ) {
      throw new WorkflowError('INVALID_ACTIVITY', 'Activities must be named instance methods')
    }
    // oxlint-enable anti-slop/no-runtime-typeof
    Reflect.defineMetadata(
      ACTIVITY_METADATA,
      Object.freeze({ ...options, ...freezeActivityDefaults(options) }),
      target,
      property
    )
  }
}

/**
 * Create an immutable named signal contract without delivering or subscribing to it.
 * List this definition in WorkflowOptions.signals, wait through ctx.waitForSignal,
 * and deliver through handle.signal. Each wait consumes one event, not a broadcast.
 * @typeParam I - Producer payload type accepted by the schema.
 * @typeParam O - Durable schema output returned by the wait.
 * @param name - Stable signal name (1–256 characters, no ASCII control characters).
 * @param schema - Standard Schema validator; its output must remain serializable.
 * @returns Reusable signal definition.
 * @throws WorkflowError for an invalid name.
 * @example
 * ```ts
 * import { defineSignal } from 'better-workflows'
 * import { z } from 'zod'
 * const Approved = defineSignal('approved', z.object({ reviewerId: z.string() }))
 * ```
 */
export function defineSignal<I, O>(
  name: string,
  schema: StandardSchemaV1<I, O>
): SignalDefinition<I, O> {
  identifier(name, 'Signal name')
  return Object.freeze({ name, schema })
}

/**
 * Return the Nest injection token for a workflow's typed client.
 * The token is cached by constructor identity. It does not register a client; use the
 * same exported workflow class in forFeature and when retrieving or overriding it.
 * @param workflow - Workflow constructor registered under workflows or clients.
 * @returns Symbol used by InjectWorkflow, app.get and testing provider overrides.
 * @example
 * ```ts
 * import type { INestApplicationContext } from '@nestjs/common'
 * import { getWorkflowToken } from 'better-workflows'
 * import type { WorkflowClient } from 'better-workflows'
 * declare class GenerateReport { run(input: { reportId: string }): Promise<void> }
 * declare const app: INestApplicationContext
 * const reports = app.get<WorkflowClient<typeof GenerateReport>>(getWorkflowToken(GenerateReport))
 * ```
 */
export function getWorkflowToken(workflow: WorkflowClass): symbol {
  let token = CLIENT_TOKENS.get(workflow)
  if (!token) {
    token = Symbol(`better-workflows/client/${workflow.name}`)
    CLIENT_TOKENS.set(workflow, token)
  }
  return token
}

/**
 * Inject a workflow's typed client through Nest constructor or property injection.
 * The containing module must register or import the client. This decorator does not
 * instantiate the workflow handler and does not make clients global.
 * @param workflow - Same workflow constructor used during client registration.
 * @returns Nest injection decorator for a parameter or property.
 * @example
 * ```ts
 * import { Injectable } from '@nestjs/common'
 * import { InjectWorkflow, WorkflowClient } from 'better-workflows'
 * declare class GenerateReport { run(input: { reportId: string }): Promise<void> }
 * @Injectable()
 * class ReportsService {
 *   constructor(@InjectWorkflow(GenerateReport) readonly reports: WorkflowClient<typeof GenerateReport>) {}
 * }
 * ```
 */
export function InjectWorkflow(workflow: WorkflowClass): ParameterDecorator & PropertyDecorator {
  return Inject(getWorkflowToken(workflow))
}

export function validateActivityDefaults(options: ActivityDefaults): void {
  if (options.queue !== undefined) queueName(options.queue)
  if (options.retry) {
    positiveInteger(options.retry.maxAttempts, 'maxAttempts')
    if (
      options.retry.backoff !== undefined &&
      !['fixed', 'exponential'].includes(options.retry.backoff)
    )
      throw new WorkflowError('INVALID_CONFIGURATION', 'Invalid retry backoff')
    milliseconds(options.retry.initialDelay ?? '1s')
    milliseconds(options.retry.maxDelay ?? '1m')
  }
  if (options.timeout !== undefined && milliseconds(options.timeout) === 0)
    throw new WorkflowError('INVALID_CONFIGURATION', 'Activity timeout must be greater than zero')
}

export function freezeActivityDefaults(options: ActivityDefaults): ActivityDefaults {
  const result = { ...options }
  if (options.retry) result.retry = Object.freeze({ ...options.retry })
  return Object.freeze(result)
}
