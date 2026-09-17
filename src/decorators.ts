import 'reflect-metadata'
import { Inject, Injectable } from '@nestjs/common'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  ActivityDefaults,
  ActivityOptions,
  SignalDefinition,
  WorkflowContractClass,
  WorkflowImplementationClass,
  WorkflowOptions,
  CronOptions,
  IntervalOptions
} from './types'
import { identifier, milliseconds, positiveInteger } from './internal/values'
import { WorkflowError } from './errors'
import { queueName } from './queues'
import { normalizeCron, normalizeInterval } from './internal/schedule'
import { SCHEDULE_METADATA } from './internal/schedule-metadata'

export { SCHEDULE_METADATA } from './internal/schedule-metadata'

export const WORKFLOW_METADATA = Symbol.for('better-workflows/workflow')
export const WORKFLOW_CONTRACT_METADATA = Symbol.for('better-workflows/workflow-contract')
export const WORKFLOW_HANDLER_METADATA = Symbol.for('better-workflows/workflow-handler')
export const ACTIVITIES_METADATA = Symbol.for('better-workflows/activities')
export const ACTIVITY_METADATA = Symbol.for('better-workflows/activity')
const CLIENT_TOKENS = new WeakMap<WorkflowContractClass, symbol>()

export interface WorkflowHandlerMetadata {
  readonly contract: WorkflowContractClass
}

/**
 * Declare a durable cron schedule for the workflow contract represented by this class.
 * The decorator only records a definition; a later runtime phase materializes occurrences
 * and starts the durable workflow. In advanced mode apply it to the abstract contract,
 * not to its concrete {@link Workflow} handler.
 * @typeParam I - Input value accepted by the workflow input schema.
 * @param options - Stable name, cron expression, optional timezone and policies.
 * @returns Class decorator storing independent schedule metadata.
 * @throws WorkflowError for invalid names, expressions, timezones or policies.
 * @example
 * ```ts
 * import { Cron, WorkflowContract } from 'better-workflows'
 * import { z } from 'zod'
 * const Input = z.object({ date: z.string() })
 * @Cron({ name: 'reports.daily', expression: '0 8 * * *', timezone: 'UTC', input: ({ scheduledAt }) => ({ date: scheduledAt }) })
 * @WorkflowContract({ name: 'reports.daily-report', version: 1, input: Input, output: z.void() })
 * abstract class DailyReport { abstract run(input: z.infer<typeof Input>): Promise<void> }
 * ```
 */
export function Cron<I = unknown>(options: CronOptions<I>): ClassDecorator {
  const metadata = normalizeCron(options)
  return (target) => {
    if (Reflect.hasOwnMetadata(SCHEDULE_METADATA, target))
      throw new WorkflowError(
        'DUPLICATE_SCHEDULE',
        `${target.name} declares more than one schedule`
      )
    Reflect.defineMetadata(SCHEDULE_METADATA, metadata, target)
  }
}

/**
 * Declare a durable interval schedule for the workflow contract represented by this class.
 * Intervals advance from their persisted cursor rather than from workflow completion.
 * In advanced mode apply it to the abstract contract, not to its concrete handler.
 * @typeParam I - Input value accepted by the workflow input schema.
 * @param options - Stable name, positive interval and optional policies.
 * @returns Class decorator storing independent schedule metadata.
 * @throws WorkflowError for invalid names, durations or policies.
 * @example
 * ```ts
 * import { Interval, Workflow } from 'better-workflows'
 * import { z } from 'zod'
 * @Interval({ name: 'catalog.sync', every: '15m', input: { kind: 'catalog' } })
 * @Workflow({ name: 'catalog.sync', version: 1, input: z.object({ kind: z.string() }), output: z.void() })
 * class SyncCatalog { async run(input: { kind: string }): Promise<void> { void input } }
 * ```
 */
export function Interval<I = unknown>(options: IntervalOptions<I>): ClassDecorator {
  const metadata = normalizeInterval(options)
  return (target) => {
    if (Reflect.hasOwnMetadata(SCHEDULE_METADATA, target))
      throw new WorkflowError(
        'DUPLICATE_SCHEDULE',
        `${target.name} declares more than one schedule`
      )
    Reflect.defineMetadata(SCHEDULE_METADATA, metadata, target)
  }
}

function validateWorkflowOptions(options: WorkflowOptions<any, any>): void {
  identifier(options.name, 'Workflow name')
  positiveInteger(options.version, 'Workflow version')
  const names = new Set<string>()
  for (const signal of options.signals ?? []) {
    identifier(signal.name, 'Signal name')
    if (names.has(signal.name)) throw new WorkflowError('DUPLICATE_SIGNAL', signal.name)
    names.add(signal.name)
  }
}

/**
 * Declare a workflow contract without creating a Nest provider.
 * The contract owns durable identity, schemas, signals and the TypeScript run signature.
 * Use {@link Workflow} on a separate concrete class to provide the executable handler.
 * @typeParam I - Schema-validated input type.
 * @typeParam O - Schema-validated output type.
 * @param options - Stable identity, schemas, optional key resolver and signals.
 * @returns Class decorator for a contract, including an abstract contract class.
 * @throws WorkflowError for invalid identity/version or duplicate signal names.
 * @example
 * ```ts
 * import { WorkflowContract } from 'better-workflows'
 * import type { WorkflowContext } from 'better-workflows'
 * import { z } from 'zod'
 * const Input = z.object({ reportId: z.string() })
 * @WorkflowContract({ name: 'reports.generate', version: 1, input: Input, output: z.string() })
 * abstract class GenerateReportWorkflow {
 *   abstract run(input: { reportId: string }, ctx: WorkflowContext): Promise<string>
 * }
 * ```
 */
export function WorkflowContract<I, O>(options: WorkflowOptions<I, O>): ClassDecorator {
  validateWorkflowOptions(options)
  return (target) => {
    Reflect.defineMetadata(WORKFLOW_CONTRACT_METADATA, Object.freeze({ ...options }), target)
  }
}

/**
 * Declare a replayable workflow's name, version, schemas and accepted signals.
 * Also applies Nest Injectable. A decorator alone does not register a handler: add the
 * class to forFeature.workflows, or clients when only a producer contract is needed.
 * @typeParam I - Schema-validated input type.
 * @typeParam O - Schema-validated output type.
 * @param options - Stable identity, schemas, optional key resolver and signals; or a
 * contract class declared with {@link WorkflowContract}.
 * @param contract - Contract class declared with {@link WorkflowContract} when using
 * the handler-association overload.
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
export function Workflow<I, O>(options: WorkflowOptions<I, O>): ClassDecorator
export function Workflow<C extends WorkflowContractClass>(
  contract: C
): <T extends WorkflowImplementationClass<C>>(target: T) => void
export function Workflow(
  optionsOrContract: WorkflowOptions<any, any> | WorkflowContractClass
): ClassDecorator {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Decorator overloads distinguish options objects from class constructors here.
  if (typeof optionsOrContract === 'function') {
    const contract = optionsOrContract
    // SAFETY: @WorkflowContract is the only writer of this metadata and writes WorkflowOptions.
    const options = Reflect.getOwnMetadata(WORKFLOW_CONTRACT_METADATA, contract) as
      | WorkflowOptions<any, any>
      | undefined
    // A simple workflow also owns contract metadata, but remains a handler and is
    // intentionally not accepted as the contract argument of the advanced overload.
    // SAFETY: @Workflow is the only writer of this metadata and writes a contract reference.
    const handler = Reflect.getOwnMetadata(WORKFLOW_HANDLER_METADATA, contract) as
      | WorkflowHandlerMetadata
      | undefined
    if (!options || handler)
      throw new WorkflowError(
        'INVALID_WORKFLOW_CONTRACT',
        `${contract.name} must be declared with @WorkflowContract() before it can be used by @Workflow()`
      )
    return (target) => {
      Injectable()(target)
      Reflect.defineMetadata(
        WORKFLOW_HANDLER_METADATA,
        Object.freeze({ contract } satisfies WorkflowHandlerMetadata),
        target
      )
    }
  }
  validateWorkflowOptions(optionsOrContract)
  return (target) => {
    Injectable()(target)
    const options = Object.freeze({ ...optionsOrContract })
    // Keep the old metadata available for consumers that inspected it directly.
    Reflect.defineMetadata(WORKFLOW_METADATA, options, target)
    Reflect.defineMetadata(WORKFLOW_CONTRACT_METADATA, options, target)
    Reflect.defineMetadata(WORKFLOW_HANDLER_METADATA, Object.freeze({ contract: target }), target)
  }
}

/**
 * Resolve the contract represented by a workflow registration class.
 * This accepts a handler internally so feature registration can normalize simple and
 * advanced modes to one contract; client tokens must use the returned contract itself.
 * @param workflow - Contract or decorated handler constructor.
 * @returns The class that owns the durable workflow metadata.
 * @throws WorkflowError when the class is not decorated as a workflow contract/handler.
 */
export function workflowContractClass(workflow: WorkflowContractClass): WorkflowContractClass {
  // SAFETY: @Workflow is the only writer of this metadata and writes a contract reference.
  const handler = Reflect.getOwnMetadata(WORKFLOW_HANDLER_METADATA, workflow) as
    | WorkflowHandlerMetadata
    | undefined
  if (handler && handler.contract !== workflow) return handler.contract
  if (Reflect.hasOwnMetadata(WORKFLOW_CONTRACT_METADATA, workflow)) return workflow
  throw new WorkflowError(
    'MISSING_DECORATOR',
    `${workflow.name} has no @WorkflowContract decorator`
  )
}

function requireClientContract(workflow: WorkflowContractClass): WorkflowContractClass {
  // SAFETY: @Workflow is the only writer of this metadata and writes a contract reference.
  const handler = Reflect.getOwnMetadata(WORKFLOW_HANDLER_METADATA, workflow) as
    | WorkflowHandlerMetadata
    | undefined
  if (handler && handler.contract !== workflow)
    throw new WorkflowError(
      'INVALID_WORKFLOW_CONTRACT',
      `${workflow.name} is a workflow handler; use its contract class for client injection`
    )
  if (!Reflect.hasOwnMetadata(WORKFLOW_CONTRACT_METADATA, workflow))
    throw new WorkflowError(
      'INVALID_WORKFLOW_CONTRACT',
      `${workflow.name} must be declared with @WorkflowContract() or @Workflow()`
    )
  return workflow
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
export function getWorkflowToken(workflow: WorkflowContractClass): symbol {
  requireClientContract(workflow)
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
export function InjectWorkflow(
  workflow: WorkflowContractClass
): ParameterDecorator & PropertyDecorator {
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
