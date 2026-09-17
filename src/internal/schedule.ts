import { createHash } from 'node:crypto'
import { Cron, DateTime, Option } from 'effect'
import type {
  CronOptions,
  Duration,
  IntervalOptions,
  JsonValue,
  ScheduleCommonOptions,
  ScheduleMisfirePolicy,
  ScheduleOccurrence,
  ScheduleOverlapPolicy,
  WorkflowContractClass,
  WorkflowOptions
} from '../types'
import { WorkflowError } from '../errors'
import { encode, identifier, milliseconds, positiveInteger } from './values'
import type { EngineWorkflow } from './wire'

/** Runtime metadata shared by the Cron and Interval decorators. */
export interface ScheduleMetadata {
  readonly name: string
  readonly kind: 'cron' | 'interval'
  readonly expression: string | undefined
  readonly timezone: string | undefined
  readonly intervalMs: number | undefined
  readonly misfire: ScheduleMisfirePolicy
  readonly overlap: ScheduleOverlapPolicy
  readonly maxCatchUp: number
  readonly input: ScheduleInputDefinition | undefined
  readonly inputResolver: (occurrence: ScheduleOccurrence) => ScheduleInputValue
  readonly cron: Cron.Cron | undefined
}

export type ScheduleInputValue = JsonValue | undefined
export type ScheduleInputDefinition =
  | ScheduleInputValue
  | ((occurrence: ScheduleOccurrence) => ScheduleInputValue)

/** Reserved idempotency-key namespace for durable schedule occurrences. */
export const SCHEDULE_IDEMPOTENCY_PREFIX = '@better-workflows/schedule/'

const DEFAULT_MISFIRE: ScheduleMisfirePolicy = 'latest'
const DEFAULT_OVERLAP: ScheduleOverlapPolicy = 'allow'
export const DEFAULT_MAX_CATCH_UP = 100

function validateCommon<I>(options: ScheduleCommonOptions<I>): void {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Decorator configuration is untyped at runtime.
  if (options === null || typeof options !== 'object')
    throw new WorkflowError('INVALID_SCHEDULE', 'Schedule options must be an object')
  identifier(options.name, 'Schedule name')
  if (
    options.misfire !== undefined &&
    options.misfire !== 'skip' &&
    options.misfire !== 'latest' &&
    options.misfire !== 'catch-up'
  )
    throw new WorkflowError('INVALID_SCHEDULE_POLICY', 'Invalid schedule misfire policy')
  if (options.overlap !== undefined && options.overlap !== 'allow' && options.overlap !== 'skip')
    throw new WorkflowError('INVALID_SCHEDULE_POLICY', 'Invalid schedule overlap policy')
  if (options.maxCatchUp !== undefined) positiveInteger(options.maxCatchUp, 'Schedule maxCatchUp')
}

function validateTimezone(timezone: unknown): asserts timezone is string | undefined {
  if (timezone === undefined) return
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Decorators validate untyped configuration at the public boundary.
  if (typeof timezone !== 'string' || !Option.isSome(DateTime.zoneMakeNamed(timezone)))
    throw new WorkflowError(
      'INVALID_SCHEDULE_TIMEZONE',
      `Invalid schedule timezone: ${JSON.stringify(timezone)}`
    )
}

function inputResolver(
  input: ScheduleInputDefinition | undefined
): (occurrence: ScheduleOccurrence) => ScheduleInputValue {
  if (input === undefined) return (_occurrence: ScheduleOccurrence): undefined => undefined
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- A schedule input is either a value or its pure resolver callback.
  if (typeof input === 'function') {
    // SAFETY: Schedule input callbacks are validated as synchronous pure functions at the dispatch boundary.
    return input as (occurrence: ScheduleOccurrence) => ScheduleInputValue
  }
  return (_occurrence: ScheduleOccurrence) => input
}

/** Validate and normalize a cron decorator configuration. */
export function normalizeCron<I>(options: CronOptions<I>): ScheduleMetadata {
  validateCommon(options)
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Decorator configuration is untyped at runtime.
  if (typeof options.expression !== 'string')
    throw new WorkflowError('INVALID_CRON_EXPRESSION', 'Cron expression must be a string')
  validateTimezone(options.timezone)
  // Never inherit the host timezone: distributed schedulers must calculate the
  // same timeline and definition hash on every machine.
  const timezone = options.timezone ?? 'UTC'
  let cron: Cron.Cron
  try {
    cron = Cron.parseUnsafe(options.expression, timezone)
  } catch (error) {
    throw new WorkflowError(
      'INVALID_CRON_EXPRESSION',
      error instanceof Error ? error.message : `Invalid cron expression: ${options.expression}`
    )
  }
  // SAFETY: Durable schedule inputs are parsed against the workflow schema before acceptance.
  const input = options.input as ScheduleInputDefinition | undefined
  return Object.freeze({
    name: options.name,
    kind: 'cron' as const,
    expression: options.expression,
    timezone,
    intervalMs: undefined,
    misfire: options.misfire ?? DEFAULT_MISFIRE,
    overlap: options.overlap ?? DEFAULT_OVERLAP,
    maxCatchUp: options.maxCatchUp ?? DEFAULT_MAX_CATCH_UP,
    input,
    inputResolver: inputResolver(input),
    cron
  })
}

/** Validate and normalize an interval decorator configuration. */
export function normalizeInterval<I>(options: IntervalOptions<I>): ScheduleMetadata {
  validateCommon(options)
  // SAFETY: Public Duration is narrowed by the shared duration parser at this boundary.
  const intervalMs = milliseconds(options.every as Duration)
  if (intervalMs === 0)
    throw new WorkflowError('INVALID_INTERVAL', 'Schedule interval must be greater than zero')
  // SAFETY: Durable schedule inputs are parsed against the workflow schema before acceptance.
  const input = options.input as ScheduleInputDefinition | undefined
  return Object.freeze({
    name: options.name,
    kind: 'interval' as const,
    expression: undefined,
    timezone: undefined,
    intervalMs,
    misfire: options.misfire ?? DEFAULT_MISFIRE,
    overlap: options.overlap ?? DEFAULT_OVERLAP,
    maxCatchUp: options.maxCatchUp ?? DEFAULT_MAX_CATCH_UP,
    input,
    inputResolver: inputResolver(input),
    cron: undefined
  })
}

/** Produce the deterministic identity of a static schedule definition. */
export function scheduleDefinitionHash(
  workflow: Pick<
    {
      options: { readonly name: string; readonly version: number }
    },
    'options'
  >,
  schedule: ScheduleMetadata
): string {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The public input union uses a function only for resolver callbacks.
  const input =
    schedule.input === undefined
      ? null
      : // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The public input union uses a function only for resolver callbacks.
        typeof schedule.input === 'function'
        ? 'resolver'
        : encode(schedule.input)
  const definition = {
    kind: schedule.kind,
    expression: schedule.expression ?? null,
    timezone: schedule.timezone ?? null,
    intervalMs: schedule.intervalMs ?? null,
    misfire: schedule.misfire,
    overlap: schedule.overlap,
    maxCatchUp: schedule.maxCatchUp,
    input,
    workflowName: workflow.options.name,
    workflowVersion: workflow.options.version
  }
  return createHash('sha256').update(JSON.stringify(definition)).digest('hex')
}

/** Calculate the first timeline deadline without exposing Effect's Cron type publicly. */
export function nextScheduleAt(schedule: ScheduleMetadata, now: number): number {
  if (schedule.kind === 'interval') {
    const intervalMs = schedule.intervalMs
    if (intervalMs === undefined)
      throw new WorkflowError('INVALID_SCHEDULE', 'Interval schedule duration is missing')
    const next = now + intervalMs
    if (!Number.isSafeInteger(next))
      throw new WorkflowError('INVALID_SCHEDULE', 'Schedule deadline exceeds safe integer range')
    return next
  }
  const expression = schedule.expression
  if (expression === undefined)
    throw new WorkflowError('INVALID_SCHEDULE', 'Cron schedule expression is missing')
  const cron = schedule.cron ?? Cron.parseUnsafe(expression, schedule.timezone)
  let next = Cron.next(cron, new Date(now))
  // Effect's DST disambiguation can return the first copy of a repeated local
  // time even when the supplied instant is already after it. Keep the pinned
  // Cron primitive as the source of truth and advance it until the result is
  // strictly after the persisted cursor.
  for (let attempts = 0; next.getTime() <= now && attempts < 2; attempts++)
    next = Cron.next(cron, next)
  if (next.getTime() <= now)
    throw new WorkflowError('INVALID_SCHEDULE', 'Cron schedule did not produce a future deadline')
  return next.getTime()
}

/** Build the normalized schedule record owned by one registered workflow contract. */
export function registeredSchedule(
  workflow: WorkflowContractClass,
  options: Pick<WorkflowOptions<any, any>, 'name' | 'version' | 'input'>,
  metadata: ScheduleMetadata,
  definition: EngineWorkflow,
  enabled: boolean
) {
  return Object.freeze({
    name: metadata.name,
    enabled,
    workflow,
    workflowName: options.name,
    workflowVersion: options.version,
    kind: metadata.kind,
    expression: metadata.expression,
    timezone: metadata.timezone,
    intervalMs: metadata.intervalMs,
    misfire: metadata.misfire,
    overlap: metadata.overlap,
    maxCatchUp: metadata.maxCatchUp,
    input: metadata.input,
    inputResolver: metadata.inputResolver,
    inputSchema: options.input,
    cron: metadata.cron,
    definition,
    definitionHash: scheduleDefinitionHash({ options }, metadata)
  })
}

export type RegisteredSchedule = ReturnType<typeof registeredSchedule>
