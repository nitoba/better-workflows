import { createHash, randomUUID } from 'node:crypto'
import { Cause, Effect, Exit } from 'effect'
import type { Failure } from '../errors'
import { WorkflowError, toFailure } from '../errors'
import type {
  ScheduleOccurrence,
  ScheduleOccurrenceSnapshot,
  ScheduleSnapshot,
  ScheduleStatus
} from '../types'
import type {
  ScheduleDefinitionUpdateOptions,
  ScheduleListOptions,
  ScheduleOccurrenceListOptions,
  ScheduleOccurrencePage,
  ScheduleRemoveOptions,
  ScheduleTriggerOptions,
  ScheduleTriggerResult
} from '../admin-types'
import { Journal } from './journal'
import type { RegisteredSchedule } from './schedule'
import {
  nextScheduleAt,
  normalizeCron,
  normalizeInterval,
  scheduleDefinitionHash,
  SCHEDULE_IDEMPOTENCY_PREFIX
} from './schedule'
import { encode, identifier, positiveInteger, validate } from './values'
import { promised } from './effects'
import { workflowDefinition } from './wire'
import {
  logAnnotations,
  TelemetryAttributeKey,
  TelemetryLogComponent,
  TelemetrySpanName
} from './telemetry'
import type { TelemetryAttributes } from './telemetry'

/** Row shape owned by the durable schedule-definition table. */
export interface ScheduleRow {
  readonly namespace: string
  readonly schedule_name: string
  readonly workflow_name: string
  readonly workflow_version: number
  readonly kind: 'cron' | 'interval'
  readonly expression: string | null
  readonly timezone: string | null
  readonly interval_ms: number | null
  readonly definition_hash: string
  readonly state: ScheduleStatus
  readonly last_occurrence_at: number | null
  readonly next_occurrence_at: number
  readonly last_execution_id: string | null
  readonly revision: number
  readonly claim_owner: string | null
  readonly claim_until: number | null
  readonly paused_at: number | null
  readonly created_at: number
  readonly updated_at: number
}

/** Runtime-owned schedule registration options, including optional implementation ownership. */
export interface ScheduleRegistrationOptions {
  readonly reconcileMissing?: boolean
  readonly implementationWorkflowKeys?: ReadonlySet<string>
  readonly ownership?: {
    readonly ownerId: string
    readonly leaseMs: number
  }
}

/** Row shape owned by durable schedule-occurrence history. */
export interface ScheduleOccurrenceRow {
  readonly namespace: string
  readonly schedule_name: string
  readonly scheduled_at: number
  readonly sequence: number
  readonly trigger_type: 'scheduled' | 'catch-up' | 'manual'
  readonly state: 'started' | 'skipped' | 'failed'
  readonly execution_id: string | null
  readonly reason_code: string | null
  readonly created_at: number
}

interface ScheduleInputRow {
  readonly input_json: string | null
}

interface ScheduleManualKeyRow {
  readonly scheduled_at: number
  readonly sequence: number
  readonly execution_id: string
}

/** Internal result which lets a failed input resolver remain visible in occurrence history. */
export type ScheduleTriggerOutcome =
  | {
      readonly status: 'started'
      readonly result: ScheduleTriggerResult
      readonly workflowCreated: boolean
      readonly workflowName: string
      readonly workflowVersion: number
    }
  | {
      readonly status: 'failed'
      readonly occurrence: ScheduleOccurrence
      readonly failure: Failure
    }

export interface SchedulePassResult {
  readonly schedules: number
  readonly occurrences: number
}

interface MaterializedOccurrence {
  readonly at: number
  readonly trigger: ScheduleOccurrence['trigger']
  readonly state: ScheduleOccurrenceRow['state']
  readonly reason: string | null
  readonly workflowCreated: boolean
}

interface MaterializationResult {
  readonly processed: number
  readonly started: number
  readonly materializedAt: number
  readonly limited: boolean
  readonly occurrences: readonly MaterializedOccurrence[]
}

const fail = (code: string, message: string): Effect.Effect<never, Failure> =>
  Effect.fail<Failure>({ code, message, retryable: false })

// Misfire policies that discard history are still bounded per transaction. The
// cursor remains due and subsequent scheduler passes finish the backlog.
const maxTimelineOccurrencesPerPass = 100_000
const NO_MATERIALIZED_OCCURRENCES: readonly MaterializedOccurrence[] = []
const permanentScheduleFailureCodes = new Set([
  'SCHEDULE_INPUT_ERROR',
  'EXECUTION_PRUNED',
  'IDEMPOTENCY_CONFLICT',
  'MISSING_WORKFLOW_VERSION',
  'INVALID_TRANSPORT',
  'PAYLOAD_TOO_LARGE'
])

function scheduleAttributes(
  schedule: Pick<
    RegisteredSchedule,
    'name' | 'workflowName' | 'workflowVersion' | 'kind' | 'misfire' | 'overlap'
  >,
  trigger: ScheduleOccurrence['trigger'],
  scheduledAt: number
): TelemetryAttributes {
  return {
    [TelemetryAttributeKey.scheduleName]: schedule.name,
    [TelemetryAttributeKey.workflowName]: schedule.workflowName,
    [TelemetryAttributeKey.workflowVersion]: schedule.workflowVersion,
    [TelemetryAttributeKey.scheduleType]: schedule.kind,
    [TelemetryAttributeKey.scheduleTrigger]: trigger,
    [TelemetryAttributeKey.scheduleMisfirePolicy]: schedule.misfire,
    [TelemetryAttributeKey.scheduleOverlapPolicy]: schedule.overlap,
    [TelemetryAttributeKey.scheduleScheduledAt]: new Date(scheduledAt).toISOString()
  }
}

function recordMaterializationTelemetry(
  journal: Journal,
  schedule: RegisteredSchedule,
  result: MaterializationResult
): Effect.Effect<void> {
  return Effect.gen(function* () {
    for (const occurrence of result.occurrences) {
      const attributes = scheduleAttributes(schedule, occurrence.trigger, occurrence.at)
      journal.telemetry?.count('scheduleOccurrence', attributes)
      journal.telemetry?.observe(
        'scheduleLag',
        Math.max(0, result.materializedAt - occurrence.at),
        attributes
      )
      yield* Effect.annotateLogs(
        Effect.logDebug('Schedule occurrence evaluated'),
        logAnnotations(TelemetryLogComponent.scheduler, attributes)
      )
      if (occurrence.state === 'started') {
        journal.telemetry?.count('scheduleStarted', attributes)
        if (occurrence.workflowCreated)
          journal.telemetry?.count('workflowStarted', {
            [TelemetryAttributeKey.workflowName]: schedule.workflowName,
            [TelemetryAttributeKey.workflowVersion]: schedule.workflowVersion
          })
        if (occurrence.trigger === 'catch-up') {
          journal.telemetry?.count('scheduleCatchUp', attributes)
          journal.telemetry?.count('scheduleMisfire', attributes)
        }
        yield* Effect.annotateLogs(
          Effect.logDebug('Schedule occurrence materialized'),
          logAnnotations(TelemetryLogComponent.scheduler, attributes)
        )
      } else if (occurrence.state === 'skipped') {
        const skipAttributes = {
          ...attributes,
          [TelemetryAttributeKey.scheduleSkipReason]: occurrence.reason ?? 'unknown'
        }
        journal.telemetry?.count('scheduleSkipped', skipAttributes)
        if (occurrence.reason === 'misfire' || occurrence.trigger === 'catch-up')
          journal.telemetry?.count('scheduleMisfire', attributes)
        yield* Effect.annotateLogs(
          Effect.logDebug('Schedule occurrence skipped'),
          logAnnotations(TelemetryLogComponent.scheduler, skipAttributes)
        )
      } else {
        journal.telemetry?.count('scheduleFailure', {
          ...attributes,
          [TelemetryAttributeKey.failureCode]: occurrence.reason ?? 'SCHEDULE_INPUT_ERROR'
        })
        yield* Effect.annotateLogs(
          Effect.logError('Schedule occurrence failed'),
          logAnnotations(TelemetryLogComponent.scheduler, {
            ...attributes,
            [TelemetryAttributeKey.failureCode]: occurrence.reason ?? 'SCHEDULE_INPUT_ERROR'
          })
        )
      }
    }
    if (result.occurrences.some((occurrence) => occurrence.trigger === 'catch-up'))
      yield* Effect.annotateLogs(
        Effect.logWarning('Schedule misfire detected'),
        logAnnotations(
          TelemetryLogComponent.scheduler,
          scheduleAttributes(schedule, 'catch-up', result.materializedAt)
        )
      )
    if (result.limited)
      yield* Effect.annotateLogs(
        Effect.logWarning('Schedule catch-up limited by maxCatchUp'),
        logAnnotations(
          TelemetryLogComponent.scheduler,
          scheduleAttributes(schedule, 'catch-up', result.materializedAt)
        )
      )
  })
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- User resolver/schema failures are untrusted values converted at this boundary.
function inputFailure(error: unknown): Failure {
  const failure = toFailure(error)
  return {
    code: 'SCHEDULE_INPUT_ERROR',
    message: failure.message,
    retryable: false
  }
}

function isPermanentScheduleFailure(failure: Failure): boolean {
  return permanentScheduleFailureCodes.has(failure.code)
}

function scheduleOccurrence(
  schedule: RegisteredSchedule,
  at: number,
  sequence: number,
  trigger: ScheduleOccurrence['trigger']
): ScheduleOccurrence {
  return {
    schedule: schedule.name,
    scheduledAt: new Date(at).toISOString(),
    occurrence: sequence,
    trigger
  }
}

/**
 * Stable occurrence key used only for schedule-originated workflow acceptance.
 * The readable form is retained for normal names; long user identifiers are
 * compacted so they remain valid workflow idempotency keys.
 */
export function scheduleOccurrenceKey(schedule: string, scheduledAt: string): string {
  const readable = `${SCHEDULE_IDEMPOTENCY_PREFIX}${encodeURIComponent(schedule)}/${scheduledAt}`
  if (readable.length <= 256) return readable
  return `${SCHEDULE_IDEMPOTENCY_PREFIX}${createHash('sha256')
    .update(JSON.stringify(['scheduled', schedule, scheduledAt]))
    .digest('hex')}`
}

function manualKey(schedule: string, idempotencyKey: string | undefined): string {
  const suffix = idempotencyKey ?? randomUUID()
  const readable = `${SCHEDULE_IDEMPOTENCY_PREFIX}${encodeURIComponent(schedule)}/manual/${encodeURIComponent(suffix)}`
  if (readable.length <= 256) return readable
  return `${SCHEDULE_IDEMPOTENCY_PREFIX}${createHash('sha256')
    .update(JSON.stringify(['manual', schedule, suffix]))
    .digest('hex')}`
}

/** Convert a persisted row to the public, storage-independent schedule view. */
export function scheduleSnapshot(row: ScheduleRow): ScheduleSnapshot {
  const base = {
    name: row.schedule_name,
    workflow: row.workflow_name,
    workflowVersion: Number(row.workflow_version),
    type: row.kind,
    status: row.state,
    nextOccurrence: new Date(Number(row.next_occurrence_at)).toISOString(),
    revision: Number(row.revision)
  }
  const lastOccurrence =
    row.last_occurrence_at === null
      ? undefined
      : new Date(Number(row.last_occurrence_at)).toISOString()
  const lastExecutionId = row.last_execution_id === null ? undefined : row.last_execution_id
  if (lastOccurrence !== undefined && lastExecutionId !== undefined)
    return Object.freeze({ ...base, lastOccurrence, lastExecutionId })
  if (lastOccurrence !== undefined) return Object.freeze({ ...base, lastOccurrence })
  if (lastExecutionId !== undefined) return Object.freeze({ ...base, lastExecutionId })
  return Object.freeze(base)
}

function occurrenceFromRow(row: ScheduleOccurrenceRow): ScheduleOccurrence {
  return {
    schedule: row.schedule_name,
    scheduledAt: new Date(Number(row.scheduled_at)).toISOString(),
    occurrence: Number(row.sequence),
    trigger: row.trigger_type
  }
}

/** Convert an occurrence row to public metadata without exposing its input. */
function occurrenceSnapshot(row: ScheduleOccurrenceRow): ScheduleOccurrenceSnapshot {
  const base = {
    ...occurrenceFromRow(row),
    state: row.state,
    createdAt: new Date(Number(row.created_at)).toISOString()
  }
  if (row.execution_id !== null && row.reason_code !== null)
    return Object.freeze({ ...base, executionId: row.execution_id, reasonCode: row.reason_code })
  if (row.execution_id !== null) return Object.freeze({ ...base, executionId: row.execution_id })
  if (row.reason_code !== null) return Object.freeze({ ...base, reasonCode: row.reason_code })
  return Object.freeze(base)
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  /* oxlint-disable anti-slop/no-runtime-typeof -- Promise-like detection is intentionally structural. */
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    'then' in value &&
    typeof value.then === 'function'
  )
  /* oxlint-enable anti-slop/no-runtime-typeof */
}

function nextAfter(schedule: RegisteredSchedule, at: number): number {
  const next = nextScheduleAt(schedule, at)
  if (!Number.isSafeInteger(next) || next <= at)
    throw new WorkflowError(
      'INVALID_SCHEDULE',
      `Schedule ${schedule.name} did not advance its cursor`
    )
  return next
}

function validateStaticInput(schedule: RegisteredSchedule): Effect.Effect<void, Failure> {
  if (schedule.input === undefined) return Effect.void
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- A schedule input is a value or a resolver callback.
  if (typeof schedule.input === 'function') return Effect.void
  return promised(() =>
    validate(schedule.inputSchema, schedule.input, `${schedule.workflowName} schedule input`)
  ).pipe(Effect.asVoid)
}

/**
 * Durable persistence boundary for schedule definitions, claims and occurrences.
 * A materialization transaction owns the schedule row lock, occurrence history,
 * workflow acceptance and cursor advancement together.
 */
export class ScheduleStore {
  constructor(readonly journal: Journal) {}

  /** Keep auxiliary schedule tables compatible with databases created before migration v9. */
  private ensureAdminTables() {
    return this.journal.sql`
      CREATE TABLE IF NOT EXISTS better_workflows_schedule_inputs(
        namespace TEXT NOT NULL,
        schedule_name TEXT NOT NULL,
        input_json TEXT,
        PRIMARY KEY(namespace, schedule_name)
      )
    `.pipe(
      Effect.andThen(
        this.journal.sql`
          CREATE TABLE IF NOT EXISTS better_workflows_schedule_manual_keys(
            namespace TEXT NOT NULL,
            schedule_name TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            scheduled_at DOUBLE PRECISION NOT NULL,
            sequence INTEGER NOT NULL,
            execution_id TEXT NOT NULL,
            created_at DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(namespace, schedule_name, idempotency_key)
          )
        `
      )
    )
  }

  /**
   * Register the immutable catalog, initialize new cursors and retain removed rows.
   * Existing definitions must have the same hash; no deployment silently changes
   * a persisted schedule timeline. Missing catalog entries become orphaned instead.
   */
  registerAll(schedules: Iterable<RegisteredSchedule>, options: ScheduleRegistrationOptions = {}) {
    const definitions = [...schedules]
    const { sql, namespace } = this.journal
    const journal = this.journal
    const self = this
    const operation = sql.withTransaction(
      Effect.gen(function* () {
        yield* self.ensureAdminTables()
        const now = yield* journal.now()
        const ownershipNow =
          options.ownership === undefined ? undefined : yield* journal.databaseNow()
        if (ownershipNow !== undefined)
          yield* sql`DELETE FROM better_workflows_schedule_owners
            WHERE namespace=${namespace} AND lease_until <= ${ownershipNow}`
        for (const schedule of definitions) {
          yield* validateStaticInput(schedule)
          const next = nextScheduleAt(schedule, now)
          yield* sql`INSERT INTO better_workflows_schedules(
            namespace, schedule_name, workflow_name, workflow_version, kind,
            expression, timezone, interval_ms, definition_hash, state,
            next_occurrence_at, revision, created_at, updated_at
          ) VALUES (
            ${namespace}, ${schedule.name}, ${schedule.workflowName}, ${schedule.workflowVersion},
            ${schedule.kind}, ${schedule.expression ?? null}, ${schedule.timezone ?? null},
            ${schedule.intervalMs ?? null}, ${schedule.definitionHash}, 'active',
            ${next}, 0, ${now}, ${now}
          ) ON CONFLICT(namespace, schedule_name) DO NOTHING`
          const rows = yield* sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
            WHERE namespace=${namespace} AND schedule_name=${schedule.name}`
          const row = rows[0]
          if (!row)
            return yield* fail(
              'SCHEDULE_REGISTRATION_FAILED',
              `Schedule ${schedule.name} was not available after registration`
            )
          if (row.definition_hash !== schedule.definitionHash)
            return yield* fail(
              'SCHEDULE_DEFINITION_CHANGED',
              `Schedule ${schedule.name} has a different persisted definition`
            )
          if (row.state === 'orphaned')
            yield* sql`UPDATE better_workflows_schedules
              SET state='active', paused_at=NULL, revision=revision+1, updated_at=${now}
              WHERE namespace=${namespace} AND schedule_name=${schedule.name}
                AND state='orphaned'`
          if (options.ownership !== undefined)
            yield* sql`INSERT INTO better_workflows_schedule_owners(
              namespace, schedule_name, owner_id, lease_until, created_at, updated_at
            ) VALUES (
              ${namespace}, ${schedule.name}, ${options.ownership.ownerId},
              ${ownershipNow! + options.ownership.leaseMs}, ${now}, ${now}
            )
            ON CONFLICT(namespace, schedule_name, owner_id) DO UPDATE SET
              lease_until=excluded.lease_until, updated_at=excluded.updated_at`
          const inputJson =
            // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Static inputs and pure resolver callbacks are the public union.
            typeof schedule.input === 'function' ? null : encode(schedule.input)
          yield* sql`INSERT INTO better_workflows_schedule_inputs(
            namespace, schedule_name, input_json
          ) VALUES (${namespace}, ${schedule.name}, ${inputJson})
          ON CONFLICT(namespace, schedule_name) DO NOTHING`
        }

        if (options.reconcileMissing !== false && options.implementationWorkflowKeys) {
          const registeredNames = new Set(definitions.map((schedule) => schedule.name))
          const existing = yield* sql<{
            schedule_name: string
            state: ScheduleStatus
            workflow_name: string
            workflow_version: number
          }>`SELECT schedule_name, state
            , workflow_name, workflow_version
            FROM better_workflows_schedules WHERE namespace=${namespace}`
          for (const row of existing) {
            if (registeredNames.has(row.schedule_name) || row.state === 'orphaned') continue
            const workflowKey = JSON.stringify([row.workflow_name, Number(row.workflow_version)])
            const liveOwner =
              options.ownership === undefined
                ? []
                : yield* sql`SELECT 1 FROM better_workflows_schedule_owners
                WHERE namespace=${namespace} AND schedule_name=${row.schedule_name}
                  AND lease_until > ${ownershipNow} LIMIT 1`
            // A live owner from another deployment protects the row even when this
            // deployment implements the workflow but intentionally removed its schedule.
            if (liveOwner.length > 0) continue
            if (
              options.implementationWorkflowKeys.has(workflowKey) ||
              options.ownership !== undefined
            )
              yield* sql`UPDATE better_workflows_schedules
                SET state='orphaned', claim_owner=NULL, claim_until=NULL,
                    revision=revision+1, updated_at=${now}
                WHERE namespace=${namespace} AND schedule_name=${row.schedule_name}`
          }
        }
      })
    )
    return operation
  }

  /** Renew, or reacquire, implementation ownership leases without changing cursors. */
  renewOwnership(scheduleNames: Iterable<string>, ownerId: string, leaseMs: number) {
    const names = [...scheduleNames]
    if (names.length === 0) return Effect.void
    const journal = this.journal
    const scheduleFilter = journal.sql.in('schedule_name', names)
    return journal.sql.withTransaction(
      Effect.gen(function* () {
        const leaseNow = yield* journal.databaseNow()
        const updatedAt = yield* journal.now()
        const schedules = yield* journal.sql<{
          schedule_name: string
          state: ScheduleStatus
        }>`SELECT schedule_name, state
          FROM better_workflows_schedules
          WHERE namespace=${journal.namespace} AND ${scheduleFilter}`
        for (const schedule of schedules) {
          yield* journal.sql`INSERT INTO better_workflows_schedule_owners(
            namespace, schedule_name, owner_id, lease_until, created_at, updated_at
          ) VALUES (
            ${journal.namespace}, ${schedule.schedule_name}, ${ownerId},
            ${leaseNow + leaseMs}, ${updatedAt}, ${updatedAt}
          )
          ON CONFLICT(namespace, schedule_name, owner_id) DO UPDATE SET
            lease_until=excluded.lease_until, updated_at=excluded.updated_at`
          if (schedule.state === 'orphaned')
            yield* journal.sql`UPDATE better_workflows_schedules
              SET state='active', paused_at=NULL, revision=revision+1, updated_at=${updatedAt}
              WHERE namespace=${journal.namespace} AND schedule_name=${schedule.schedule_name}
                AND state='orphaned'`
        }
      })
    )
  }

  /** Reconcile definitions absent from this deployment after owner leases expire. */
  reconcileMissing(scheduleNames: Iterable<string>) {
    const registeredNames = new Set(scheduleNames)
    const journal = this.journal
    return journal.sql.withTransaction(
      Effect.gen(function* () {
        const ownershipNow = yield* journal.databaseNow()
        const now = yield* journal.now()
        yield* journal.sql`DELETE FROM better_workflows_schedule_owners
          WHERE namespace=${journal.namespace} AND lease_until <= ${ownershipNow}`
        const existing = yield* journal.sql<{
          schedule_name: string
          state: ScheduleStatus
          workflow_name: string
          workflow_version: number
        }>`SELECT schedule_name, state, workflow_name, workflow_version
          FROM better_workflows_schedules WHERE namespace=${journal.namespace}`
        for (const row of existing) {
          if (registeredNames.has(row.schedule_name) || row.state === 'orphaned') continue
          const liveOwner = yield* journal.sql`SELECT 1 FROM better_workflows_schedule_owners
            WHERE namespace=${journal.namespace} AND schedule_name=${row.schedule_name}
              AND lease_until > ${ownershipNow} LIMIT 1`
          if (liveOwner.length === 0)
            yield* journal.sql`UPDATE better_workflows_schedules
              SET state='orphaned', claim_owner=NULL, claim_until=NULL,
                  revision=revision+1, updated_at=${now}
              WHERE namespace=${journal.namespace} AND schedule_name=${row.schedule_name}`
        }
      })
    )
  }

  /** Return a bounded, database-ordered batch of schedules due on the business clock. */
  due(scheduleNames: Iterable<string>, limit = 100) {
    const journal = this.journal
    const names = [...scheduleNames]
    if (names.length === 0) return Effect.succeed<readonly ScheduleRow[]>([])
    const scheduleFilter = journal.sql.in('schedule_name', names)
    return Effect.gen(function* () {
      const now = yield* journal.now()
      return yield* journal.sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
        WHERE namespace=${journal.namespace} AND state='active'
          AND ${scheduleFilter}
          AND next_occurrence_at <= ${now}
        ORDER BY next_occurrence_at, schedule_name LIMIT ${limit}`
    })
  }

  /** Read one persisted definition for internal administration/runtime use. */
  get(name: string) {
    return this.journal.sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
      WHERE namespace=${this.journal.namespace} AND schedule_name=${name}`
  }

  /** Read bounded definitions in stable name order for internal administration/runtime use. */
  list() {
    return this.journal.sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
      WHERE namespace=${this.journal.namespace} ORDER BY schedule_name`
  }

  /** Return a bounded, public schedule-definition page without exposing storage rows. */
  listSnapshots(options: ScheduleListOptions = {}) {
    const limit = options.limit ?? 1000
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000)
      return fail('INVALID_ARGUMENT', 'Schedule limit must be between 1 and 1000')
    if (
      options.status !== undefined &&
      options.status !== 'active' &&
      options.status !== 'paused' &&
      options.status !== 'orphaned'
    )
      return fail('INVALID_ARGUMENT', 'Unknown schedule status')
    const namespace = this.journal.namespace
    const sql = this.journal.sql
    return Effect.gen(function* () {
      const rows = options.status
        ? options.cursor === undefined
          ? yield* sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
              WHERE namespace=${namespace} AND state=${options.status}
              ORDER BY schedule_name LIMIT ${limit}`
          : yield* sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
              WHERE namespace=${namespace} AND state=${options.status} AND schedule_name>${options.cursor}
              ORDER BY schedule_name LIMIT ${limit}`
        : options.cursor === undefined
          ? yield* sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
              WHERE namespace=${namespace} ORDER BY schedule_name LIMIT ${limit}`
          : yield* sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
              WHERE namespace=${namespace} AND schedule_name>${options.cursor}
              ORDER BY schedule_name LIMIT ${limit}`
      return rows.map(scheduleSnapshot)
    }).pipe(Effect.mapError(toFailure))
  }

  /** Return bounded, storage-independent occurrence history for one schedule. */
  listOccurrenceSnapshots(name: string, options: ScheduleOccurrenceListOptions = {}) {
    identifier(name, 'Schedule name')
    const limit = options.limit ?? 100
    const after = options.after ?? 0
    if (
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > 1000 ||
      !Number.isSafeInteger(after) ||
      after < 0
    )
      return fail('INVALID_PAGINATION', 'after must be >= 0; limit must be between 1 and 1000')
    if (
      options.state !== undefined &&
      options.state !== 'started' &&
      options.state !== 'skipped' &&
      options.state !== 'failed'
    )
      return fail('INVALID_ARGUMENT', 'Unknown schedule occurrence state')
    const namespace = this.journal.namespace
    const sql = this.journal.sql
    return Effect.gen(function* () {
      const scheduleRows = yield* sql<{ schedule_name: string }>`SELECT schedule_name
        FROM better_workflows_schedules
        WHERE namespace=${namespace} AND schedule_name=${name}`
      if (!scheduleRows.length) {
        const historyRows = yield* sql<{ schedule_name: string }>`SELECT schedule_name
          FROM better_workflows_schedule_occurrences
          WHERE namespace=${namespace} AND schedule_name=${name} LIMIT 1`
        if (!historyRows.length)
          return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
      }
      const rows =
        options.state === undefined
          ? yield* sql<ScheduleOccurrenceRow>`SELECT *
              FROM better_workflows_schedule_occurrences
              WHERE namespace=${namespace} AND schedule_name=${name} AND sequence>${after}
              ORDER BY sequence LIMIT ${limit + 1}`
          : yield* sql<ScheduleOccurrenceRow>`SELECT *
              FROM better_workflows_schedule_occurrences
              WHERE namespace=${namespace} AND schedule_name=${name} AND state=${options.state}
                AND sequence>${after}
              ORDER BY sequence LIMIT ${limit + 1}`
      const hasMore = rows.length > limit
      const occurrences = rows.slice(0, limit).map(occurrenceSnapshot)
      const page: ScheduleOccurrencePage = hasMore
        ? { occurrences, nextCursor: occurrences.at(-1)!.occurrence }
        : { occurrences }
      return page
    }).pipe(Effect.mapError(toFailure))
  }

  /** Read one public schedule snapshot and fail closed for unknown names. */
  getSnapshot(name: string) {
    identifier(name, 'Schedule name')
    const self = this
    return Effect.gen(function* () {
      const rows = yield* self.get(name).pipe(Effect.mapError(toFailure))
      const row = rows[0]
      if (!row) return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
      return scheduleSnapshot(row)
    }).pipe(Effect.mapError(toFailure))
  }

  /** Pause future materialization and return the resulting public state. */
  pauseSnapshot(name: string) {
    return this.pause(name).pipe(
      Effect.map(scheduleSnapshot),
      Effect.tap(() =>
        Effect.annotateLogs(
          Effect.logInfo('Schedule paused'),
          logAnnotations(TelemetryLogComponent.scheduler, {
            [TelemetryAttributeKey.scheduleName]: name
          })
        )
      ),
      Effect.mapError(toFailure)
    )
  }

  /** Resume a schedule; the next scheduler pass applies its persisted misfire policy. */
  resumeSnapshot(name: string) {
    return this.resume(name).pipe(
      Effect.map(scheduleSnapshot),
      Effect.tap(() =>
        Effect.annotateLogs(
          Effect.logInfo('Schedule resumed'),
          logAnnotations(TelemetryLogComponent.scheduler, {
            [TelemetryAttributeKey.scheduleName]: name
          })
        )
      ),
      Effect.mapError(toFailure)
    )
  }

  /** Explicitly replace a definition and reset its recurring cursor from the business clock. */
  updateDefinition(name: string, options: ScheduleDefinitionUpdateOptions) {
    identifier(name, 'Schedule name')
    if (options.confirm !== true)
      return fail(
        'CONFIRMATION_REQUIRED',
        'Reconcile a schedule with { confirm: true, from: "now" }'
      )
    if (options.from !== 'now')
      return fail('INVALID_ARGUMENT', 'Only schedule reconciliation from "now" is supported')
    const definition = options.definition
    if (definition.type !== 'cron' && definition.type !== 'interval')
      return fail('INVALID_ARGUMENT', 'Schedule type must be "cron" or "interval"')
    identifier(definition.workflow, 'Workflow name')
    positiveInteger(definition.workflowVersion, 'Workflow version')
    positiveInteger(definition.maxCatchUp, 'Schedule maxCatchUp')
    const inputMode = definition.inputMode ?? (definition.input === undefined ? 'none' : 'static')
    if (inputMode !== 'none' && inputMode !== 'static' && inputMode !== 'resolver')
      return fail('INVALID_ARGUMENT', 'Schedule inputMode must be none, static or resolver')
    if (inputMode === 'static' && definition.input === undefined)
      return fail('INVALID_ARGUMENT', 'Static schedule input is required when inputMode is static')
    if (inputMode === 'resolver' && definition.input !== undefined)
      return fail('INVALID_ARGUMENT', 'Resolver schedule definitions cannot include static input')
    if (inputMode === 'none' && definition.input !== undefined)
      return fail('INVALID_ARGUMENT', 'Static schedule input requires inputMode: "static"')
    if (definition.type === 'cron' && definition.intervalMs !== undefined)
      return fail('INVALID_ARGUMENT', 'Cron schedules cannot include intervalMs')
    if (definition.type === 'interval' && definition.expression !== undefined)
      return fail('INVALID_ARGUMENT', 'Interval schedules cannot include expression')
    if (definition.type === 'interval' && definition.timezone !== undefined)
      return fail('INVALID_ARGUMENT', 'Interval schedules cannot include timezone')
    const scheduleInput =
      inputMode === 'resolver'
        ? () => undefined
        : inputMode === 'static'
          ? definition.input
          : undefined
    const commonDefinition = {
      name,
      misfire: definition.misfire,
      overlap: definition.overlap,
      maxCatchUp: definition.maxCatchUp,
      input: scheduleInput
    }
    const metadata =
      definition.type === 'cron'
        ? definition.timezone === undefined
          ? normalizeCron({
              ...commonDefinition,
              expression: definition.expression ?? ''
            })
          : normalizeCron({
              ...commonDefinition,
              expression: definition.expression ?? '',
              timezone: definition.timezone
            })
        : normalizeInterval({
            name,
            every: definition.intervalMs ?? 0,
            misfire: definition.misfire,
            overlap: definition.overlap,
            maxCatchUp: definition.maxCatchUp,
            input:
              inputMode === 'resolver'
                ? () => undefined
                : inputMode === 'static'
                  ? definition.input
                  : undefined
          })
    const hash = scheduleDefinitionHash(
      { options: { name: definition.workflow, version: definition.workflowVersion } },
      metadata
    )
    const inputJson =
      inputMode === 'resolver'
        ? null
        : encode(inputMode === 'static' ? definition.input : undefined)
    const journal = this.journal
    const sql = journal.sql
    const self = this
    return sql
      .withTransaction(
        Effect.gen(function* () {
          yield* self.ensureAdminTables()
          const now = yield* journal.now()
          const next = nextScheduleAt(metadata, now)
          const rows = yield* sql<ScheduleRow>`UPDATE better_workflows_schedules SET
          workflow_name=${definition.workflow}, workflow_version=${definition.workflowVersion},
          kind=${metadata.kind}, expression=${metadata.expression ?? null},
          timezone=${metadata.timezone ?? null}, interval_ms=${metadata.intervalMs ?? null},
          definition_hash=${hash}, state='active', last_occurrence_at=NULL,
          next_occurrence_at=${next}, last_execution_id=NULL, revision=revision+1,
          claim_owner=NULL, claim_until=NULL, paused_at=NULL, updated_at=${now}
          WHERE namespace=${journal.namespace} AND schedule_name=${name}
          RETURNING *`
          if (!rows.length) return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
          yield* sql`INSERT INTO better_workflows_schedule_inputs(
          namespace, schedule_name, input_json
        ) VALUES (${journal.namespace}, ${name}, ${inputJson})
        ON CONFLICT(namespace, schedule_name) DO UPDATE SET input_json=${inputJson}`
          return scheduleSnapshot(rows[0]!)
        })
      )
      .pipe(Effect.mapError(toFailure))
  }

  /** Pause future materialization without changing the normal timeline cursor. */
  pause(name: string) {
    identifier(name, 'Schedule name')
    const journal = this.journal
    return journal.sql.withTransaction(
      Effect.gen(function* () {
        const now = yield* journal.now()
        const rows = yield* journal.sql<ScheduleRow>`UPDATE better_workflows_schedules
          SET state='paused', paused_at=COALESCE(paused_at, ${now}),
              claim_owner=NULL, claim_until=NULL, revision=revision+1, updated_at=${now}
          WHERE namespace=${journal.namespace} AND schedule_name=${name} AND state='active'
          RETURNING *`
        if (rows.length) return rows[0]!
        const existing = yield* journal.sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
          WHERE namespace=${journal.namespace} AND schedule_name=${name}`
        if (!existing.length) return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
        return existing[0]!
      })
    )
  }

  /** Resume a paused schedule; the next scheduler pass applies its misfire policy. */
  resume(name: string) {
    identifier(name, 'Schedule name')
    const journal = this.journal
    return journal.sql.withTransaction(
      Effect.gen(function* () {
        const now = yield* journal.now()
        const rows = yield* journal.sql<ScheduleRow>`UPDATE better_workflows_schedules
          SET state='active', paused_at=NULL, revision=revision+1, updated_at=${now}
          WHERE namespace=${journal.namespace} AND schedule_name=${name} AND state='paused'
          RETURNING *`
        if (rows.length) return rows[0]!
        const existing = yield* journal.sql<ScheduleRow>`SELECT * FROM better_workflows_schedules
          WHERE namespace=${journal.namespace} AND schedule_name=${name}`
        if (!existing.length) return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
        return existing[0]!
      })
    )
  }

  /** Remove only a paused or orphaned definition; occurrence history is retained. */
  remove(name: string, options: ScheduleRemoveOptions | undefined) {
    identifier(name, 'Schedule name')
    if (options?.confirm !== true)
      return fail(
        'CONFIRMATION_REQUIRED',
        'Remove a schedule with { confirm: true } after pausing it'
      )
    const journal = this.journal
    const sql = journal.sql
    const self = this
    return sql
      .withTransaction(
        Effect.gen(function* () {
          yield* self.ensureAdminTables()
          const removed = yield* sql<{
            schedule_name: string
          }>`DELETE FROM better_workflows_schedules
            WHERE namespace=${journal.namespace} AND schedule_name=${name}
              AND state IN ('paused', 'orphaned')
            RETURNING schedule_name`
          if (!removed.length) {
            const existing = yield* sql<Pick<ScheduleRow, 'state'>>`SELECT state
              FROM better_workflows_schedules
              WHERE namespace=${journal.namespace} AND schedule_name=${name}`
            if (!existing.length)
              return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
            return yield* fail(
              'SCHEDULE_MUST_BE_PAUSED',
              `Schedule ${name} must be paused or orphaned before removal`
            )
          }
          // Keep occurrence and manual-key history. Only the current static input is tied to
          // the removed definition and must not shadow a future re-registration.
          yield* sql`DELETE FROM better_workflows_schedule_inputs
            WHERE namespace=${journal.namespace} AND schedule_name=${name}`
        })
      )
      .pipe(
        Effect.tap(() =>
          Effect.annotateLogs(
            Effect.logInfo('Schedule removed'),
            logAnnotations(TelemetryLogComponent.scheduler, {
              [TelemetryAttributeKey.scheduleName]: name
            })
          )
        ),
        Effect.mapError(toFailure)
      )
  }

  /**
   * Materialize one operator-triggered occurrence without moving the recurring cursor.
   * The runtime supplies a registered schedule so resolver input is validated exactly like
   * a scheduled occurrence. Standalone administration can trigger schedules with persisted
   * static input; resolver-only schedules require the application runtime.
   */
  trigger(
    name: string,
    schedule: RegisteredSchedule | undefined,
    options: ScheduleTriggerOptions = {}
  ) {
    identifier(name, 'Schedule name')
    if (options.idempotencyKey !== undefined) identifier(options.idempotencyKey, 'Idempotency key')
    const journal = this.journal
    const sql = journal.sql
    const self = this
    const operation = sql.withTransaction(
      Effect.gen(function* () {
        yield* self.ensureAdminTables()
        // An UPDATE obtains the row lock on PostgreSQL and serializes manual sequence
        // allocation with another trigger without changing the normal cursor values.
        const locked = yield* sql<ScheduleRow>`UPDATE better_workflows_schedules
          SET revision=revision
          WHERE namespace=${journal.namespace} AND schedule_name=${name}
          RETURNING *`
        const row = locked[0]
        if (!row) return yield* fail('SCHEDULE_NOT_FOUND', `Unknown schedule ${name}`)
        if (row.state === 'orphaned')
          return yield* fail(
            'SCHEDULE_ORPHANED',
            `Schedule ${name} is orphaned and cannot accept manual triggers`
          )

        if (options.idempotencyKey !== undefined) {
          const existing =
            yield* sql<ScheduleManualKeyRow>`SELECT scheduled_at, sequence, execution_id
              FROM better_workflows_schedule_manual_keys
            WHERE namespace=${journal.namespace} AND schedule_name=${name}
              AND idempotency_key=${options.idempotencyKey}`
          if (existing[0]) {
            const [recorded] = yield* sql<ScheduleOccurrenceRow>`SELECT *
              FROM better_workflows_schedule_occurrences
              WHERE namespace=${journal.namespace} AND schedule_name=${name}
                AND sequence=${existing[0].sequence}`
            if (!recorded)
              return yield* fail(
                'STORAGE_INTEGRITY',
                `Manual trigger ${name} is missing its occurrence`
              )
            if (recorded.state === 'failed')
              return {
                status: 'failed' as const,
                occurrence: occurrenceFromRow(recorded),
                failure: {
                  code: recorded.reason_code ?? 'SCHEDULE_INPUT_ERROR',
                  message: `Schedule ${name} manual occurrence failed`,
                  retryable: false
                }
              }
            const live = yield* sql<{ execution_id: string }>`SELECT execution_id
              FROM better_workflows_runs
              WHERE namespace=${journal.namespace} AND execution_id=${existing[0].execution_id}`
            if (!live.length) {
              const tombstone = yield* sql<{ execution_id: string }>`SELECT execution_id
                FROM better_workflows_tombstones
                WHERE namespace=${journal.namespace} AND execution_id=${existing[0].execution_id}`
              if (tombstone.length)
                return yield* fail(
                  'EXECUTION_PRUNED',
                  `Execution ${existing[0].execution_id} was pruned; its manual idempotency key is still reserved`
                )
              return yield* fail(
                'STORAGE_INTEGRITY',
                `Manual trigger ${name} points to a missing execution`
              )
            }
            return {
              status: 'started' as const,
              result: {
                occurrence: occurrenceFromRow(recorded),
                executionId: existing[0].execution_id,
                created: false
              },
              workflowCreated: false,
              workflowName: row.workflow_name,
              workflowVersion: row.workflow_version
            }
          }
        }

        // Manual occurrences use the current business/database clock exactly.
        // Their sequence, rather than their timestamp, provides independent identity
        // when a trigger shares an instant with another manual or recurring row.
        const scheduledAt = Number(yield* journal.now())
        const sequence = yield* nextSequence(sql, journal.namespace, name)
        const occurrence: ScheduleOccurrence = {
          schedule: name,
          scheduledAt: new Date(scheduledAt).toISOString(),
          occurrence: sequence,
          trigger: 'manual'
        }

        let input: string
        if (schedule) {
          const inputExit = yield* Effect.exit(resolveInput(schedule, occurrence))
          if (Exit.isFailure(inputExit)) {
            const failure = toFailure(Cause.squash(inputExit.cause))
            yield* insertOccurrence(
              sql,
              journal,
              schedule ? schedule.name : name,
              scheduledAt,
              sequence,
              'manual',
              'failed',
              null,
              failure.code
            )
            if (options.idempotencyKey !== undefined)
              yield* sql`INSERT INTO better_workflows_schedule_manual_keys(
                namespace, schedule_name, idempotency_key, scheduled_at, sequence, execution_id, created_at
              ) VALUES (
                ${journal.namespace}, ${name}, ${options.idempotencyKey}, ${scheduledAt}, ${sequence}, '', ${yield* journal.now()}
              )`
            return { status: 'failed' as const, occurrence, failure }
          }
          input = inputExit.value
        } else {
          const [stored] = yield* sql<ScheduleInputRow>`SELECT input_json
            FROM better_workflows_schedule_inputs
            WHERE namespace=${journal.namespace} AND schedule_name=${name}`
          if (!stored || stored.input_json === null)
            return yield* fail(
              'SCHEDULE_INPUT_UNAVAILABLE',
              `Schedule ${name} uses an input resolver and requires a running application`
            )
          input = stored.input_json
        }

        const key = manualKey(name, options.idempotencyKey)
        const definition =
          schedule?.definition ??
          workflowDefinition(journal.namespace, row.workflow_name, row.workflow_version)
        const executionId = yield* definition.executionId({ key, input })
        const attributes: TelemetryAttributes = schedule
          ? scheduleAttributes(schedule, 'manual', scheduledAt)
          : {
              [TelemetryAttributeKey.scheduleName]: name,
              [TelemetryAttributeKey.workflowName]: row.workflow_name,
              [TelemetryAttributeKey.workflowVersion]: row.workflow_version,
              [TelemetryAttributeKey.scheduleType]: row.kind,
              [TelemetryAttributeKey.scheduleTrigger]: 'manual'
            }
        const accepted = yield* Effect.useSpan(
          TelemetrySpanName.scheduleTrigger,
          { attributes, kind: 'producer' },
          (scheduleSpan) =>
            Effect.withParentSpan(
              Effect.useSpan(
                TelemetrySpanName.workflowStart,
                {
                  attributes: {
                    ...attributes,
                    [TelemetryAttributeKey.workflowName]: row.workflow_name,
                    [TelemetryAttributeKey.workflowVersion]: row.workflow_version
                  },
                  kind: 'producer'
                },
                (workflowStartSpan) =>
                  Effect.withParentSpan(
                    Effect.gen(function* () {
                      const accepted = yield* journal.acceptInTransaction(
                        executionId,
                        row.workflow_name,
                        row.workflow_version,
                        key,
                        input,
                        {
                          traceId: workflowStartSpan.traceId,
                          spanId: workflowStartSpan.spanId,
                          sampled: workflowStartSpan.sampled
                        }
                      )
                      scheduleSpan.attribute(TelemetryAttributeKey.executionId, executionId)
                      yield* Effect.annotateCurrentSpan(
                        TelemetryAttributeKey.executionId,
                        executionId
                      )
                      yield* Effect.annotateCurrentSpan(
                        TelemetryAttributeKey.workflowCreated,
                        accepted.created
                      )
                      return accepted
                    }),
                    workflowStartSpan
                  )
              ),
              scheduleSpan
            )
        )
        yield* insertOccurrence(
          sql,
          journal,
          schedule ? schedule.name : name,
          scheduledAt,
          sequence,
          'manual',
          'started',
          accepted.row.execution_id,
          null
        )
        const updatedAt = yield* journal.now()
        yield* sql`UPDATE better_workflows_schedules
          SET last_execution_id=${accepted.row.execution_id}, revision=revision+1,
              updated_at=${updatedAt}
          WHERE namespace=${journal.namespace} AND schedule_name=${name}`
        if (options.idempotencyKey !== undefined)
          yield* sql`INSERT INTO better_workflows_schedule_manual_keys(
            namespace, schedule_name, idempotency_key, scheduled_at, sequence, execution_id, created_at
          ) VALUES (
            ${journal.namespace}, ${name}, ${options.idempotencyKey}, ${scheduledAt}, ${sequence},
            ${accepted.row.execution_id}, ${yield* journal.now()}
          )`
        const result: ScheduleTriggerResult = {
          occurrence,
          executionId: accepted.row.execution_id,
          created: accepted.created
        }
        return {
          status: 'started' as const,
          result,
          workflowCreated: accepted.created,
          workflowName: row.workflow_name,
          workflowVersion: row.workflow_version
        }
      })
    )
    return operation.pipe(
      Effect.tap((outcome) => {
        if (outcome.status === 'failed')
          return Effect.gen(function* () {
            journal.telemetry?.count('scheduleOccurrence', {
              [TelemetryAttributeKey.scheduleName]: name,
              [TelemetryAttributeKey.scheduleTrigger]: 'manual'
            })
            journal.telemetry?.count('scheduleFailure', {
              [TelemetryAttributeKey.scheduleName]: name,
              [TelemetryAttributeKey.scheduleTrigger]: 'manual',
              [TelemetryAttributeKey.failureCode]: outcome.failure.code
            })
            yield* Effect.annotateLogs(
              Effect.logError('Schedule occurrence failed'),
              logAnnotations(TelemetryLogComponent.scheduler, {
                [TelemetryAttributeKey.scheduleName]: name,
                [TelemetryAttributeKey.scheduleTrigger]: 'manual',
                [TelemetryAttributeKey.failureCode]: outcome.failure.code
              })
            )
          })
        if (!outcome.result.created) return Effect.void
        const attributes = schedule
          ? scheduleAttributes(
              schedule,
              'manual',
              Date.parse(outcome.result.occurrence.scheduledAt)
            )
          : {
              [TelemetryAttributeKey.scheduleName]: name,
              [TelemetryAttributeKey.scheduleTrigger]: 'manual'
            }
        if (outcome.workflowCreated)
          journal.telemetry?.count('workflowStarted', {
            [TelemetryAttributeKey.workflowName]: outcome.workflowName,
            [TelemetryAttributeKey.workflowVersion]: outcome.workflowVersion
          })
        journal.telemetry?.count('scheduleOccurrence', attributes)
        journal.telemetry?.count('scheduleStarted', attributes)
        journal.telemetry?.count('scheduleManualTrigger', attributes)
        journal.telemetry?.observe('scheduleLag', 0, attributes)
        return Effect.annotateLogs(
          Effect.logDebug('Schedule occurrence materialized'),
          logAnnotations(TelemetryLogComponent.scheduler, attributes)
        )
      })
    )
  }

  /**
   * Claim and materialize one due schedule. The claim is fenced by owner and lease
   * in the same transaction that changes the cursor, so a lost process cannot commit
   * an old cursor after another process has taken over.
   */
  materialize(
    schedule: RegisteredSchedule,
    owner: string,
    leaseMs: number,
    misfireGraceMs = 0,
    refreshMs = Math.max(1, Math.floor(leaseMs / 3))
  ) {
    const journal = this.journal
    const sql = journal.sql
    const operation = sql.withTransaction(
      Effect.gen(function* () {
        const businessNow = yield* journal.now()
        const leaseNow = yield* journal.databaseNow()
        const claimUntil = leaseNow + leaseMs
        const claimed = yield* sql<ScheduleRow>`UPDATE better_workflows_schedules
          SET claim_owner=${owner}, claim_until=${claimUntil},
              revision=revision+1, updated_at=${businessNow}
          WHERE namespace=${journal.namespace} AND schedule_name=${schedule.name}
            AND definition_hash=${schedule.definitionHash}
            AND state='active' AND next_occurrence_at <= ${businessNow}
            AND (claim_until IS NULL OR claim_until <= ${leaseNow})
          RETURNING *`
        const row = claimed[0]
        if (!row) {
          const current = yield* sql<Pick<ScheduleRow, 'definition_hash' | 'state'>>`
            SELECT definition_hash, state FROM better_workflows_schedules
            WHERE namespace=${journal.namespace} AND schedule_name=${schedule.name}`
          if (
            current[0]?.state === 'active' &&
            current[0].definition_hash !== schedule.definitionHash
          )
            return yield* fail(
              'SCHEDULE_DEFINITION_CHANGED',
              `Schedule ${schedule.name} changed while this runtime was active; restart is required`
            )
          return {
            processed: 0,
            started: 0,
            materializedAt: Number(businessNow),
            limited: false,
            occurrences: NO_MATERIALIZED_OCCURRENCES
          }
        }

        let currentClaimUntil = claimUntil
        const renewLease = (force = false) =>
          Effect.gen(function* () {
            const now = Number(yield* journal.databaseNow())
            if (!force && now < currentClaimUntil - refreshMs) return
            const renewedUntil = now + leaseMs
            const renewed = yield* sql<{ claim_until: number }>`UPDATE better_workflows_schedules
              SET claim_until=${renewedUntil}
              WHERE namespace=${journal.namespace} AND schedule_name=${schedule.name}
                AND claim_owner=${owner}
              RETURNING claim_until`
            if (!renewed.length)
              return yield* fail('SCHEDULE_LEASE_LOST', `Schedule ${schedule.name} lease expired`)
            currentClaimUntil = Number(renewed[0]!.claim_until)
          })

        const due: number[] = []
        let cursor = Number(row.next_occurrence_at)
        let moreDue = false
        while (cursor <= businessNow) {
          due.push(cursor)
          const next = nextAfter(schedule, cursor)
          if (
            (schedule.misfire === 'catch-up' && due.length >= schedule.maxCatchUp) ||
            due.length >= maxTimelineOccurrencesPerPass
          ) {
            moreDue = next <= businessNow
            break
          }
          cursor = next
        }
        if (!due.length)
          return {
            processed: 0,
            started: 0,
            materializedAt: Number(businessNow),
            limited: false,
            occurrences: NO_MATERIALIZED_OCCURRENCES
          }

        const current = due.filter((at) => businessNow - at <= misfireGraceMs)
        const currentDeadline = !moreDue ? current.at(-1) : undefined
        const selected =
          schedule.misfire === 'skip'
            ? currentDeadline === undefined
              ? []
              : [currentDeadline]
            : schedule.misfire === 'latest'
              ? moreDue
                ? []
                : [due.at(-1)!]
              : due
        const selectedSet = new Set(selected)
        let sequence = yield* nextSequence(sql, journal.namespace, schedule.name)
        let lastExecutionId: string | null = row.last_execution_id
        let started = 0
        let lastOccurrenceAt: number | null = row.last_occurrence_at
        const occurrences: MaterializedOccurrence[] = []

        for (const at of due) {
          yield* renewLease()
          const trigger: ScheduleOccurrence['trigger'] =
            at === currentDeadline ? 'scheduled' : 'catch-up'
          const existing = yield* sql<ScheduleOccurrenceRow>`SELECT *
              FROM better_workflows_schedule_occurrences
              WHERE namespace=${journal.namespace} AND schedule_name=${schedule.name}
                AND scheduled_at=${at} AND trigger_type <> 'manual'`
          const recorded = existing[0]
          const occurrenceSequence = recorded?.sequence ?? sequence++
          lastOccurrenceAt = at
          if (recorded) continue

          if (!selectedSet.has(at)) {
            yield* insertOccurrence(
              sql,
              journal,
              schedule.name,
              at,
              occurrenceSequence,
              trigger,
              'skipped',
              null,
              'misfire'
            )
            occurrences.push({
              at,
              trigger,
              state: 'skipped',
              reason: 'misfire',
              workflowCreated: false
            })
            continue
          }

          if (
            schedule.overlap === 'skip' &&
            (yield* hasActiveExecution(sql, journal, schedule.name))
          ) {
            yield* insertOccurrence(
              sql,
              journal,
              schedule.name,
              at,
              occurrenceSequence,
              trigger,
              'skipped',
              null,
              'overlap'
            )
            occurrences.push({
              at,
              trigger,
              state: 'skipped',
              reason: 'overlap',
              workflowCreated: false
            })
            continue
          }

          const occurrence = scheduleOccurrence(schedule, at, occurrenceSequence, trigger)
          const materializedExit = yield* Effect.exit(
            Effect.useSpan(
              TelemetrySpanName.scheduleTrigger,
              {
                attributes: scheduleAttributes(schedule, trigger, at),
                kind: 'producer'
              },
              (scheduleSpan) =>
                Effect.withParentSpan(
                  Effect.gen(function* () {
                    const inputExit = yield* Effect.exit(resolveInput(schedule, occurrence))
                    if (Exit.isFailure(inputExit)) {
                      const details = toFailure(Cause.squash(inputExit.cause))
                      scheduleSpan.attribute(TelemetryAttributeKey.failureCode, details.code)
                      return { status: 'failed' as const, reason: details.code }
                    }

                    const input = inputExit.value
                    const key = scheduleOccurrenceKey(schedule.name, occurrence.scheduledAt)
                    const accepted = yield* Effect.useSpan(
                      TelemetrySpanName.workflowStart,
                      {
                        attributes: {
                          ...scheduleAttributes(schedule, trigger, at),
                          [TelemetryAttributeKey.workflowName]: schedule.workflowName,
                          [TelemetryAttributeKey.workflowVersion]: schedule.workflowVersion
                        },
                        kind: 'producer'
                      },
                      (workflowStartSpan) =>
                        Effect.withParentSpan(
                          Effect.gen(function* () {
                            const executionId = yield* schedule.definition.executionId({
                              key,
                              input
                            })
                            const accepted = yield* journal.acceptInTransaction(
                              executionId,
                              schedule.workflowName,
                              schedule.workflowVersion,
                              key,
                              input,
                              {
                                traceId: workflowStartSpan.traceId,
                                spanId: workflowStartSpan.spanId,
                                sampled: workflowStartSpan.sampled
                              }
                            )
                            yield* Effect.annotateCurrentSpan(
                              TelemetryAttributeKey.executionId,
                              executionId
                            )
                            yield* Effect.annotateCurrentSpan(
                              TelemetryAttributeKey.workflowCreated,
                              accepted.created
                            )
                            return accepted
                          }),
                          workflowStartSpan
                        )
                    )
                    scheduleSpan.attribute(
                      TelemetryAttributeKey.executionId,
                      accepted.row.execution_id
                    )
                    return {
                      status: 'started' as const,
                      executionId: accepted.row.execution_id,
                      workflowCreated: accepted.created
                    }
                  }),
                  scheduleSpan
                )
            )
          )
          let materialized:
            | {
                readonly status: 'started'
                readonly executionId: string
                readonly workflowCreated: boolean
              }
            | {
                readonly status: 'failed'
                readonly reason: string
              }
          if (Exit.isFailure(materializedExit)) {
            const failure = toFailure(Cause.squash(materializedExit.cause))
            if (!isPermanentScheduleFailure(failure)) return yield* Effect.fail(failure)
            materialized = { status: 'failed', reason: failure.code }
          } else materialized = materializedExit.value
          // Validation and workflow acceptance may be asynchronous. Renew after
          // that work as well, including when the configured lease was exceeded.
          yield* renewLease(true)
          if (materialized.status === 'failed') {
            yield* insertOccurrence(
              sql,
              journal,
              schedule.name,
              at,
              occurrenceSequence,
              trigger,
              'failed',
              null,
              materialized.reason
            )
            occurrences.push({
              at,
              trigger,
              state: 'failed',
              reason: materialized.reason,
              workflowCreated: false
            })
            continue
          }

          yield* insertOccurrence(
            sql,
            journal,
            schedule.name,
            at,
            occurrenceSequence,
            trigger,
            'started',
            materialized.executionId,
            null
          )
          occurrences.push({
            at,
            trigger,
            state: 'started',
            reason: null,
            workflowCreated: materialized.workflowCreated
          })
          lastExecutionId = materialized.executionId
          started++
        }

        const last = due.at(-1)!
        const next = nextAfter(schedule, last)
        yield* renewLease(true)
        const finalBusinessNow = yield* journal.now()
        const finalLeaseNow = yield* journal.databaseNow()
        const updated = yield* sql`UPDATE better_workflows_schedules
          SET last_occurrence_at=${lastOccurrenceAt}, next_occurrence_at=${next},
              last_execution_id=${lastExecutionId}, claim_owner=NULL, claim_until=NULL,
              revision=revision+1, updated_at=${finalBusinessNow}
          WHERE namespace=${journal.namespace} AND schedule_name=${schedule.name}
            AND claim_owner=${owner} AND claim_until > ${finalLeaseNow}
          RETURNING schedule_name`
        if (!updated.length)
          return yield* fail('SCHEDULE_LEASE_LOST', `Schedule ${schedule.name} lease expired`)

        return {
          processed: due.length,
          started,
          materializedAt: Number(businessNow),
          limited: moreDue && schedule.misfire === 'catch-up',
          occurrences
        }
      })
    )
    return operation.pipe(
      Effect.tap((result) => recordMaterializationTelemetry(journal, schedule, result))
    )
  }
}

function nextSequence(sql: Journal['sql'], namespace: string, schedule: string) {
  return sql<{ sequence: number }>`SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
    FROM better_workflows_schedule_occurrences
    WHERE namespace=${namespace} AND schedule_name=${schedule}`.pipe(
    Effect.map((rows) => Number(rows[0]?.sequence ?? 1))
  )
}

function insertOccurrence(
  sql: Journal['sql'],
  journal: Journal,
  scheduleName: string,
  at: number,
  sequence: number,
  trigger: ScheduleOccurrence['trigger'],
  state: ScheduleOccurrenceRow['state'],
  executionId: string | null,
  reason: string | null
) {
  return Effect.gen(function* () {
    const createdAt = yield* journal.now()
    yield* sql`INSERT INTO better_workflows_schedule_occurrences(
      namespace, schedule_name, scheduled_at, sequence, trigger_type, state,
      execution_id, reason_code, created_at
    ) VALUES (
      ${journal.namespace}, ${scheduleName}, ${at}, ${sequence}, ${trigger}, ${state},
      ${executionId}, ${reason}, ${createdAt}
    )`
  })
}

function resolveInput(schedule: RegisteredSchedule, occurrence: ScheduleOccurrence) {
  return Effect.gen(function* () {
    let value: unknown
    try {
      value = schedule.inputResolver(occurrence)
    } catch (error) {
      return yield* Effect.fail<Failure>(inputFailure(error))
    }
    if (isPromiseLike(value))
      return yield* Effect.fail<Failure>({
        code: 'SCHEDULE_INPUT_ERROR',
        message: 'Schedule input resolvers must be synchronous',
        retryable: false
      })
    const parsed = yield* promised(() =>
      validate(schedule.inputSchema, value, `${schedule.workflowName} schedule input`)
    ).pipe(Effect.mapError(inputFailure))
    try {
      return encode(parsed)
    } catch (error) {
      return yield* Effect.fail<Failure>(inputFailure(error))
    }
  })
}

function hasActiveExecution(sql: Journal['sql'], journal: Journal, schedule: string) {
  // A started occurrence identifies a continuation chain, not necessarily its
  // currently executing generation. Resolve the newest generation in SQL so the
  // overlap check remains one set-based query instead of one query per occurrence.
  return sql<{ active: number }>`SELECT 1 AS active
    FROM better_workflows_schedule_occurrences AS occurrences
    JOIN better_workflows_runs AS origin
      ON origin.namespace=occurrences.namespace AND origin.execution_id=occurrences.execution_id
    JOIN better_workflows_runs AS current_run
      ON current_run.namespace=origin.namespace AND current_run.chain_id=origin.chain_id
     AND current_run.generation=(
       SELECT MAX(latest.generation)
       FROM better_workflows_runs AS latest
       WHERE latest.namespace=origin.namespace AND latest.chain_id=origin.chain_id
     )
    WHERE occurrences.namespace=${journal.namespace} AND occurrences.schedule_name=${schedule}
      AND occurrences.state='started' AND occurrences.execution_id IS NOT NULL
      AND current_run.state NOT IN ('continued','completed','failed','cancelled')
    LIMIT 1`.pipe(Effect.map((rows) => rows.length > 0))
}
