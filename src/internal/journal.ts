import { Effect } from 'effect'
import { createHash } from 'node:crypto'
import type { BusinessClock } from './clock'
import { migrateAdvanced } from './migrations'
import type { SqlClient } from 'effect/unstable/sql/SqlClient'
import type { Failure } from '../errors'
import type { HistoryPage, JsonValue } from '../types'
import { decode, encode } from './values'

export interface RunRow {
  readonly execution_id: string
  readonly namespace: string
  readonly workflow_name: string
  readonly version: number
  readonly dedupe_key: string
  readonly input_json: string
  readonly created_at: number
  readonly updated_at: number
  readonly state:
    | 'accepted'
    | 'running'
    | 'waiting'
    | 'continued'
    | 'completed'
    | 'failed'
    | 'cancelled'
  readonly control: 'run' | 'pause' | 'cancel'
  readonly control_revision: number
  readonly applied_revision: number
  readonly dispatched: number
  readonly wait_type: string | null
  readonly wait_step: string | null
  readonly result_json: string | null
  readonly failure_json: string | null
  readonly chain_id: string
  readonly generation: number
  readonly continued_from: string | null
  readonly continued_to: string | null
}

export interface CommandRow {
  readonly scope: string
  readonly protocol: number
  readonly step_id: string
  readonly ordinal: number
  readonly signature: string
  readonly state: string
}

export interface WaitRow {
  readonly execution_id: string
  readonly step_id: string
  readonly signal_name: string
  readonly deadline: number | null
  readonly state: 'pending' | 'success' | 'timeout'
  readonly result_json: string | null
  readonly delivered: number
  readonly wake_requested: number
}

export interface RetryRow {
  readonly execution_id: string
  readonly step_id: string
  readonly attempt: number
  readonly deadline: number
}

export interface ReconciliationRow {
  readonly execution_id: string
  readonly namespace: string
  readonly result_json: string | null
  readonly failure_json: string | null
  readonly delivered: number
}

export interface ClaimRow {
  readonly execution_id: string
  readonly step_id: string
  readonly attempt: number
  readonly delivery_attempt: number
  readonly owner_token: string
  readonly lease_until: number
  readonly state: 'running' | 'completed'
  readonly result_json: string | null
  readonly failure_json: string | null
}

interface SignalRow {
  readonly sequence: number
  readonly payload_json: string
  readonly accepted_at: number
}

const fail = (code: string, message: string) =>
  Effect.fail<Failure>({ code, message, retryable: false })

export class Journal {
  constructor(
    readonly sql: SqlClient,
    readonly namespace: string,
    readonly clock?: BusinessClock
  ) {}

  readonly now = () => (this.clock ? Effect.sync(() => this.clock!.now()) : this.databaseNow())

  readonly databaseNow = () =>
    this.sql
      .onDialectOrElse({
        pg: () =>
          this.sql<{
            now: number
          }>`SELECT (extract(epoch FROM clock_timestamp()) * 1000)::double precision AS now`,
        orElse: () =>
          this.sql<{
            now: number
          }>`SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) AS now`
      })
      .pipe(Effect.map((rows) => Math.floor(rows[0]!.now)))

  migrateBase() {
    const sql = this.sql
    return sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_schema (version INTEGER PRIMARY KEY)`
        const versions = yield* sql<{
          version: number
        }>`SELECT version FROM better_workflows_schema`
        if (versions.some((row) => row.version > 5)) {
          return yield* fail(
            'SCHEMA_TOO_NEW',
            'This database was migrated by a newer better-workflows version'
          )
        }
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_runs (
        execution_id TEXT PRIMARY KEY,
        namespace TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        version INTEGER NOT NULL,
        dedupe_key TEXT NOT NULL,
        input_json TEXT NOT NULL,
        created_at DOUBLE PRECISION NOT NULL,
        updated_at DOUBLE PRECISION NOT NULL,
        state TEXT NOT NULL DEFAULT 'accepted',
        control TEXT NOT NULL DEFAULT 'run',
        control_revision INTEGER NOT NULL DEFAULT 0,
        applied_revision INTEGER NOT NULL DEFAULT 0,
        dispatched INTEGER NOT NULL DEFAULT 0,
        event_sequence INTEGER NOT NULL DEFAULT 0,
        signal_sequence INTEGER NOT NULL DEFAULT 0,
        wait_type TEXT,
        wait_step TEXT,
        result_json TEXT,
        failure_json TEXT,
        UNIQUE(namespace, workflow_name, dedupe_key)
      )`
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_commands (
        execution_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        signature TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'scheduled',
        PRIMARY KEY(execution_id, step_id),
        UNIQUE(execution_id, ordinal)
      )`
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_events (
        execution_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        at DOUBLE PRECISION NOT NULL,
        type TEXT NOT NULL,
        step_id TEXT,
        details_json TEXT NOT NULL,
        PRIMARY KEY(execution_id, sequence)
      )`
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_signals (
        execution_id TEXT NOT NULL,
        signal_name TEXT NOT NULL,
        event_key TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        accepted_at DOUBLE PRECISION NOT NULL,
        consumed_by TEXT,
        PRIMARY KEY(execution_id, signal_name, event_key),
        UNIQUE(execution_id, sequence)
      )`
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_waits (
        execution_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        signal_name TEXT NOT NULL,
        deadline DOUBLE PRECISION,
        state TEXT NOT NULL DEFAULT 'pending',
        result_json TEXT,
        delivered INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(execution_id, step_id)
      )`
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_retries (
        execution_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        deadline DOUBLE PRECISION NOT NULL,
        delivered INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(execution_id, step_id, attempt)
      )`
        yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_retry_outbox ON better_workflows_retries(delivered, deadline)`
        yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_claims (
        execution_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        delivery_attempt INTEGER NOT NULL,
        owner_token TEXT NOT NULL,
        lease_until DOUBLE PRECISION NOT NULL,
        state TEXT NOT NULL,
        result_json TEXT,
        failure_json TEXT,
        PRIMARY KEY(execution_id, step_id, attempt)
      )`
        yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_dispatch ON better_workflows_runs(namespace, dispatched, execution_id)`
        yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_active ON better_workflows_runs(namespace, state, execution_id)`
        yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_signal_inbox ON better_workflows_signals(execution_id, signal_name, consumed_by, sequence)`
        yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_wait_outbox ON better_workflows_waits(delivered, execution_id)`
        yield* sql`INSERT INTO better_workflows_schema(version) VALUES (1) ON CONFLICT DO NOTHING`
      })
    )
  }

  migrate() {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.sql.onDialectOrElse({
          pg: () => self.sql`SELECT pg_advisory_xact_lock(748023196)`,
          orElse: () => Effect.void
        })
        yield* self.migrateBase()
        yield* migrateAdvanced(self.sql)
      })
    )
  }

  /** Caller must hold the run's transaction when adding a history event. */
  event(
    executionId: string,
    type: string,
    details: JsonValue = null,
    stepId: string | null = null
  ) {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.now()
      const [row] = yield* self.sql<{ event_sequence: number }>`
        UPDATE better_workflows_runs SET event_sequence = event_sequence + 1, updated_at = ${now}
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace}
        RETURNING event_sequence`
      if (!row) return yield* fail('EXECUTION_NOT_FOUND', executionId)
      yield* self.sql`INSERT INTO better_workflows_events(execution_id, sequence, at, type, step_id, details_json)
        VALUES (${executionId}, ${row.event_sequence}, ${now}, ${type}, ${stepId}, ${encode(details)})`
    })
  }

  lockDedupe(name: string, key: string) {
    return this.sql.onDialectOrElse({
      pg: () =>
        this
          .sql`SELECT pg_advisory_xact_lock(hashtextextended(${JSON.stringify([this.namespace, name, key])}, 0))`,
      orElse: () => this.sql`UPDATE better_workflows_schema SET version=version WHERE version=1`
    })
  }

  lockRun(executionId: string) {
    return this
      .sql`UPDATE better_workflows_runs SET event_sequence = event_sequence WHERE execution_id = ${executionId} AND namespace = ${this.namespace}`
  }

  get(executionId: string, workflowName?: string) {
    const self = this
    return Effect.gen(function* () {
      const [row] = yield* self.sql<RunRow>`SELECT * FROM better_workflows_runs
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace}`
      if (!row || (workflowName !== undefined && row.workflow_name !== workflowName)) {
        return yield* fail('EXECUTION_NOT_FOUND', executionId)
      }
      return row
    })
  }

  /** Resolve a child continuation chain without treating an active generation as terminal. */
  followContinuation(executionId: string) {
    const self = this
    return Effect.gen(function* () {
      const seen = new Set<string>()
      let row = yield* self.get(executionId)
      while (true) {
        if (seen.has(row.execution_id))
          return yield* fail(
            'STORAGE_INTEGRITY',
            `Continuation chain contains a cycle at execution ${row.execution_id}`
          )
        seen.add(row.execution_id)
        if (row.state !== 'continued') return row
        if (!row.continued_to)
          return yield* fail(
            'STORAGE_INTEGRITY',
            `Execution ${row.execution_id} is continued without a next generation`
          )
        const next = yield* self.get(row.continued_to)
        if (
          next.workflow_name !== row.workflow_name ||
          next.version !== row.version ||
          next.chain_id !== row.chain_id ||
          next.generation !== row.generation + 1 ||
          next.continued_from !== row.execution_id
        )
          return yield* fail(
            'STORAGE_INTEGRITY',
            `Continuation ${next.execution_id} does not match execution ${row.execution_id}`
          )
        row = next
      }
    })
  }

  accept(executionId: string, name: string, version: number, key: string, input: string) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lockDedupe(name, key)
        const [tombstone] = yield* self.sql<{ input_hash: string; execution_id: string }>`
          SELECT input_hash, execution_id FROM better_workflows_tombstones
          WHERE namespace = ${self.namespace} AND workflow_name = ${name} AND dedupe_key = ${key}`
        if (tombstone)
          return yield* fail(
            tombstone.input_hash === createHash('sha256').update(input).digest('hex')
              ? 'EXECUTION_PRUNED'
              : 'IDEMPOTENCY_CONFLICT',
            `Execution ${tombstone.execution_id} was pruned; its idempotency key is still reserved`
          )
        const now = yield* self.now()
        const rows = yield* self.sql<RunRow>`INSERT INTO better_workflows_runs
        (execution_id, namespace, workflow_name, version, dedupe_key, input_json, created_at, updated_at, chain_id, generation)
        VALUES (${executionId}, ${self.namespace}, ${name}, ${version}, ${key}, ${input}, ${now}, ${now}, ${executionId}, 0)
        ON CONFLICT(namespace, workflow_name, dedupe_key) DO NOTHING RETURNING *`
        const created = rows.length > 0
        const [existing] = created
          ? rows
          : yield* self.sql<RunRow>`SELECT * FROM better_workflows_runs
        WHERE namespace = ${self.namespace} AND workflow_name = ${name} AND dedupe_key = ${key}`
        if (!existing)
          return yield* fail('STORAGE_CONFLICT', 'Could not resolve execution deduplication')
        if (existing.input_json !== input)
          return yield* fail(
            'IDEMPOTENCY_CONFLICT',
            'The idempotency key already belongs to a different payload'
          )
        if (created)
          yield* self.event(existing.execution_id, 'workflow.accepted', { workflow: name, version })
        return { row: existing, created }
      })
    )
  }

  /**
   * Atomically replace one execution with the next generation of its chain.
   * The caller supplies the engine-derived next ID, while the journal owns the
   * state transition and all replay/idempotency checks.
   */
  continueAsNew(executionId: string, nextExecutionId: string, key: string, input: string) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lockRun(executionId)
        const current = yield* self.get(executionId)
        const nextGeneration = current.generation + 1
        const expectedChain = current.chain_id
        if (current.state === 'continued') {
          if (current.continued_to !== nextExecutionId)
            return yield* fail(
              'STORAGE_INTEGRITY',
              `Execution ${executionId} already continued to a different generation`
            )
          const [next] = yield* self.sql<RunRow>`SELECT * FROM better_workflows_runs
            WHERE execution_id = ${nextExecutionId} AND namespace = ${self.namespace}`
          if (
            !next ||
            next.chain_id !== expectedChain ||
            next.generation !== nextGeneration ||
            next.continued_from !== executionId ||
            next.workflow_name !== current.workflow_name ||
            next.version !== current.version ||
            next.dedupe_key !== key ||
            next.input_json !== input
          )
            return yield* fail(
              'STORAGE_INTEGRITY',
              `Continuation ${nextExecutionId} is missing or does not match execution ${executionId}`
            )
          return next
        }
        if (['completed', 'failed', 'cancelled'].includes(current.state))
          return yield* fail(
            'TERMINAL_EXECUTION',
            `Cannot continue terminal execution ${executionId}`
          )
        if (current.control !== 'run')
          return yield* fail(
            'CONTROL_CONFLICT',
            `Execution ${executionId} has an outstanding ${current.control} control request`
          )
        if (nextExecutionId === executionId)
          return yield* fail(
            'STORAGE_INTEGRITY',
            'A continuation cannot reuse its current execution ID'
          )

        const [existing] = yield* self.sql<RunRow>`SELECT * FROM better_workflows_runs
          WHERE namespace = ${self.namespace} AND chain_id = ${expectedChain} AND generation = ${nextGeneration}`
        if (existing) {
          if (
            existing.execution_id !== nextExecutionId ||
            existing.workflow_name !== current.workflow_name ||
            existing.version !== current.version ||
            existing.dedupe_key !== key ||
            existing.input_json !== input ||
            existing.continued_from !== executionId
          )
            return yield* fail(
              'STORAGE_INTEGRITY',
              `Continuation generation ${nextGeneration} conflicts with execution ${executionId}`
            )
          return yield* fail(
            'STORAGE_INTEGRITY',
            `Continuation ${nextExecutionId} exists before its source transition committed`
          )
        }

        const now = yield* self.now()
        yield* self.sql`INSERT INTO better_workflows_runs
          (execution_id, namespace, workflow_name, version, dedupe_key, input_json,
           created_at, updated_at, state, chain_id, generation, continued_from)
          VALUES (${nextExecutionId}, ${self.namespace}, ${current.workflow_name}, ${current.version},
            ${key}, ${input}, ${now}, ${now}, 'accepted', ${expectedChain}, ${nextGeneration}, ${executionId})`
        const updated = yield* self.sql`UPDATE better_workflows_runs
          SET state = 'continued', wait_type = NULL, wait_step = NULL, continued_to = ${nextExecutionId}
          WHERE execution_id = ${executionId} AND namespace = ${self.namespace}
          AND control = 'run'
          AND state NOT IN ('continued', 'completed', 'failed', 'cancelled')
          RETURNING execution_id`
        if (!updated.length)
          return yield* fail(
            'STORAGE_CONFLICT',
            `Execution ${executionId} changed while continuing as new`
          )
        yield* self.event(executionId, 'workflow.continued', {
          fromExecutionId: executionId,
          toExecutionId: nextExecutionId,
          generation: nextGeneration
        })
        yield* self.event(nextExecutionId, 'workflow.continue-as-new', {
          fromExecutionId: executionId,
          toExecutionId: nextExecutionId,
          generation: nextGeneration
        })
        return yield* self.get(nextExecutionId)
      })
    )
  }

  pendingDispatch() {
    return this.sql<RunRow>`SELECT * FROM better_workflows_runs WHERE namespace = ${this.namespace}
      AND (dispatched = 0 OR control_revision > applied_revision) AND control <> 'pause'
      AND state NOT IN ('continued', 'completed', 'failed', 'cancelled') ORDER BY created_at, execution_id LIMIT 100`
  }

  activeAfter(cursor: string) {
    return this.sql<RunRow>`SELECT * FROM better_workflows_runs WHERE namespace = ${this.namespace}
      AND dispatched = 1 AND state NOT IN ('continued', 'completed', 'failed', 'cancelled')
      AND execution_id > ${cursor} ORDER BY execution_id LIMIT 100`
  }

  dispatched(row: RunRow) {
    return this
      .sql`UPDATE better_workflows_runs SET dispatched = 1, applied_revision = ${row.control_revision}
      WHERE execution_id = ${row.execution_id} AND namespace = ${this.namespace}
      AND applied_revision <= ${row.control_revision}`
  }

  control(executionId: string, action: 'run' | 'pause' | 'cancel', reason: string) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        const rows =
          yield* self.sql`UPDATE better_workflows_runs SET control = ${action}, control_revision = control_revision + 1
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace}
        AND control <> ${action} AND state NOT IN ('continued', 'completed', 'failed', 'cancelled')
        AND control <> 'cancel' RETURNING execution_id`
        if (rows.length > 0)
          yield* self.event(
            executionId,
            `workflow.${action === 'run' ? 'resumed' : action === 'pause' ? 'paused' : 'cancel-requested'}`,
            { reason }
          )
        const row = yield* self.get(executionId)
        if (
          action === 'run' &&
          (row.state === 'continued' ||
            row.state === 'failed' ||
            row.state === 'cancelled' ||
            row.control === 'cancel')
        ) {
          return yield* fail(
            'TERMINAL_EXECUTION',
            'resume does not restart failed or cancelled executions'
          )
        }
        return row
      })
    )
  }

  running(executionId: string) {
    return this
      .sql`UPDATE better_workflows_runs SET state = 'running', wait_type = NULL, wait_step = NULL
      WHERE execution_id = ${executionId} AND namespace = ${this.namespace}
      AND state NOT IN ('continued', 'completed', 'failed', 'cancelled')`
  }

  beginCommand(
    executionId: string,
    stepId: string,
    ordinal: number,
    signature: string,
    kind: string,
    scope = ''
  ) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        const inserted =
          yield* self.sql`INSERT INTO better_workflows_commands(execution_id, step_id, ordinal, signature, scope, protocol)
        VALUES (${executionId}, ${stepId}, ${ordinal}, ${signature}, ${scope}, 2) ON CONFLICT DO NOTHING RETURNING step_id`
        const [row] = yield* self.sql<CommandRow>`SELECT * FROM better_workflows_commands
        WHERE execution_id = ${executionId} AND step_id = ${stepId}`
        if (!row || row.ordinal !== ordinal || row.signature !== signature || row.scope !== scope) {
          return yield* fail(
            'NON_DETERMINISTIC_WORKFLOW',
            `Command ${ordinal} (${stepId}) differs from the persisted history; retain the old workflow version`
          )
        }
        if (inserted.length)
          yield* self.event(executionId, 'command.scheduled', { kind, ordinal, scope }, stepId)
        yield* self.sql`UPDATE better_workflows_runs SET state = 'waiting', wait_type = ${kind}, wait_step = ${stepId}
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace}
        AND state NOT IN ('continued', 'completed', 'failed', 'cancelled')`
      })
    )
  }

  finishCommand(executionId: string, stepId: string, success: boolean) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        const rows =
          yield* self.sql`UPDATE better_workflows_commands SET state = ${success ? 'completed' : 'failed'}
        WHERE execution_id = ${executionId} AND step_id = ${stepId} AND state = 'scheduled' RETURNING step_id`
        if (rows.length)
          yield* self.event(
            executionId,
            success ? 'command.completed' : 'command.failed',
            null,
            stepId
          )
        yield* self.sql`UPDATE better_workflows_runs SET state = 'running', wait_type = NULL, wait_step = NULL
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace} AND wait_step = ${stepId}
        AND state NOT IN ('continued', 'completed', 'failed', 'cancelled')`
      })
    )
  }

  assertEnd(executionId: string, count: number, scope = '') {
    const self = this
    return Effect.gen(function* () {
      const [row] = yield* self.sql<CommandRow>`SELECT * FROM better_workflows_commands
        WHERE execution_id = ${executionId} AND scope = ${scope} AND ordinal >= ${count} LIMIT 1`
      if (row)
        return yield* fail(
          'NON_DETERMINISTIC_WORKFLOW',
          `Previously recorded command ${row.step_id} was removed`
        )
    })
  }

  complete(executionId: string, result: string | null, failure: Failure | null, cancelled = false) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        const state = cancelled ? 'cancelled' : failure ? 'failed' : 'completed'
        const rows = yield* self.sql`UPDATE better_workflows_runs
        SET state = ${state}, result_json = ${result}, failure_json = ${failure ? encode(failure) : null}, wait_type = NULL, wait_step = NULL
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace}
        AND state NOT IN ('continued', 'completed', 'failed', 'cancelled') RETURNING execution_id`
        if (rows.length)
          yield* self.event(
            executionId,
            `workflow.${state}`,
            failure ? { code: failure.code, message: failure.message } : null
          )
      })
    )
  }

  enqueueReconciliation(executionId: string, result: string | null, failure: Failure | null) {
    const self = this
    return this.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lockRun(executionId)
        yield* self.sql`INSERT INTO better_workflows_reconciliations
          (execution_id, namespace, result_json, failure_json)
          VALUES (${executionId}, ${self.namespace}, ${result}, ${failure ? encode(failure) : null})
          ON CONFLICT(execution_id) DO NOTHING`
      })
    )
  }

  pendingReconciliations() {
    return this.sql<ReconciliationRow>`SELECT * FROM better_workflows_reconciliations
      WHERE namespace = ${this.namespace} AND delivered = 0
      ORDER BY execution_id LIMIT 100`
  }

  reconciliationDelivered(reconciliation: ReconciliationRow) {
    return this.sql`UPDATE better_workflows_reconciliations SET delivered = 1
      WHERE execution_id = ${reconciliation.execution_id} AND namespace = ${this.namespace}`
  }

  wait(executionId: string, stepId: string, signalName: string, timeout: number | undefined) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lockRun(executionId)
        const now = yield* self.now()
        yield* self.sql`INSERT INTO better_workflows_waits(execution_id, step_id, signal_name, deadline)
        VALUES (${executionId}, ${stepId}, ${signalName}, ${timeout === undefined ? null : now + timeout})
        ON CONFLICT DO NOTHING`
        // A signal may have been accepted before the workflow reached this wait.
        yield* self.sql`UPDATE better_workflows_waits SET wake_requested = 1
        WHERE execution_id = ${executionId} AND step_id = ${stepId} AND state = 'pending' AND delivered = 0
        AND (SELECT COUNT(*) FROM better_workflows_signals
          WHERE execution_id = ${executionId} AND signal_name = ${signalName} AND consumed_by IS NULL) >
          (SELECT COUNT(*) FROM better_workflows_waits
            WHERE execution_id = ${executionId} AND signal_name = ${signalName}
            AND state = 'pending' AND delivered = 0 AND wake_requested = 1)`
      })
    )
  }

  signal(executionId: string, signalName: string, key: string, payload: string) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        // The run row serializes signal acceptance, consumption and timeout decisions on both databases.
        yield* self.sql`UPDATE better_workflows_runs SET signal_sequence = signal_sequence
        WHERE execution_id = ${executionId} AND namespace = ${self.namespace}`
        const run = yield* self.get(executionId)
        if (run.state === 'continued')
          return yield* fail(
            'TERMINAL_EXECUTION',
            'Cannot send a new signal to a continued execution'
          )
        const [existing] = yield* self.sql<SignalRow>`SELECT * FROM better_workflows_signals
        WHERE execution_id = ${executionId} AND signal_name = ${signalName} AND event_key = ${key}`
        if (existing) {
          if (existing.payload_json !== payload)
            return yield* fail(
              'IDEMPOTENCY_CONFLICT',
              'Signal key already belongs to a different payload'
            )
          return { accepted: false }
        }
        if (
          run.control === 'cancel' ||
          ['continued', 'completed', 'failed', 'cancelled'].includes(run.state)
        ) {
          return yield* fail(
            'TERMINAL_EXECUTION',
            'Cannot send a new signal to a terminal execution'
          )
        }
        const now = yield* self.now()
        const [sequence] = yield* self.sql<{ signal_sequence: number }>`UPDATE better_workflows_runs
        SET signal_sequence = signal_sequence + 1 WHERE execution_id = ${executionId} RETURNING signal_sequence`
        yield* self.sql`INSERT INTO better_workflows_signals(execution_id, signal_name, event_key, sequence, payload_json, accepted_at)
        VALUES (${executionId}, ${signalName}, ${key}, ${sequence!.signal_sequence}, ${payload}, ${now})`
        yield* self.sql`UPDATE better_workflows_waits SET wake_requested = 1
        WHERE execution_id = ${executionId} AND step_id = (
          SELECT step_id FROM better_workflows_waits
          WHERE execution_id = ${executionId} AND signal_name = ${signalName}
          AND state = 'pending' AND delivered = 0 AND wake_requested = 0
          ORDER BY step_id LIMIT 1
        )`
        yield* self.event(executionId, 'signal.accepted', {
          signal: signalName,
          sequence: sequence!.signal_sequence
        })
        return { accepted: true }
      })
    )
  }

  pendingWaits() {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.now()
      return yield* self.sql<WaitRow>`SELECT w.* FROM better_workflows_waits w
        JOIN better_workflows_runs r ON r.execution_id = w.execution_id
        WHERE r.namespace = ${self.namespace} AND w.delivered = 0
        AND r.control <> 'cancel' AND r.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
        AND (w.wake_requested = 1 OR (w.deadline IS NOT NULL AND w.deadline <= ${now}))
        ORDER BY w.execution_id, w.step_id LIMIT 100`
    })
  }

  resolveWait(wait: WaitRow) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.sql`UPDATE better_workflows_runs SET signal_sequence = signal_sequence
        WHERE execution_id = ${wait.execution_id} AND namespace = ${self.namespace}`
        const [current] = yield* self.sql<WaitRow>`SELECT * FROM better_workflows_waits
        WHERE execution_id = ${wait.execution_id} AND step_id = ${wait.step_id}`
        if (!current || current.state !== 'pending') return current
        const now = yield* self.now()
        const [signal] = yield* self.sql<SignalRow>`SELECT * FROM better_workflows_signals
        WHERE execution_id = ${wait.execution_id} AND signal_name = ${wait.signal_name} AND consumed_by IS NULL
        ORDER BY sequence LIMIT 1`
        if (signal && (current.deadline === null || signal.accepted_at <= current.deadline)) {
          yield* self.sql`UPDATE better_workflows_signals SET consumed_by = ${wait.step_id}
          WHERE execution_id = ${wait.execution_id} AND sequence = ${signal.sequence}`
          yield* self.sql`UPDATE better_workflows_waits SET state = 'success', result_json = ${signal.payload_json}, wake_requested = 1
          WHERE execution_id = ${wait.execution_id} AND step_id = ${wait.step_id}`
          yield* self.event(
            wait.execution_id,
            'signal.consumed',
            { signal: wait.signal_name, sequence: signal.sequence },
            wait.step_id
          )
          return {
            ...current,
            state: 'success' as const,
            result_json: signal.payload_json,
            wake_requested: 1
          }
        }
        if (current.deadline !== null && now >= current.deadline) {
          yield* self.sql`UPDATE better_workflows_waits SET state = 'timeout', wake_requested = 1
          WHERE execution_id = ${wait.execution_id} AND step_id = ${wait.step_id}`
          yield* self.event(
            wait.execution_id,
            'signal.timed-out',
            { signal: wait.signal_name },
            wait.step_id
          )
          return { ...current, state: 'timeout' as const, wake_requested: 1 }
        }
        yield* self.sql`UPDATE better_workflows_waits SET wake_requested = 0
        WHERE execution_id = ${wait.execution_id} AND step_id = ${wait.step_id} AND state = 'pending'`
        return { ...current, wake_requested: 0 }
      })
    )
  }

  delivered(wait: WaitRow) {
    return this.sql`UPDATE better_workflows_waits SET delivered = 1, wake_requested = 0
      WHERE execution_id = ${wait.execution_id} AND step_id = ${wait.step_id} AND state <> 'pending'`
  }

  claim(
    executionId: string,
    stepId: string,
    attempt: number,
    delivery: number,
    owner: string,
    lease: number
  ) {
    const self = this
    return self.sql.withTransaction(
      self.claimInTransaction(executionId, stepId, attempt, delivery, owner, lease)
    )
  }

  /** Claim an activity while the caller owns the surrounding transaction. */
  claimInTransaction(
    executionId: string,
    stepId: string,
    attempt: number,
    delivery: number,
    owner: string,
    lease: number
  ) {
    const self = this
    return Effect.gen(function* () {
      yield* self.lockRun(executionId)
      const now = yield* self.databaseNow()
      yield* self.sql`INSERT INTO better_workflows_claims
      (execution_id, step_id, attempt, delivery_attempt, owner_token, lease_until, state)
      VALUES (${executionId}, ${stepId}, ${attempt}, ${delivery}, ${owner}, ${now + lease}, 'running')
      ON CONFLICT DO NOTHING`
      yield* self.sql`UPDATE better_workflows_claims SET delivery_attempt = ${delivery}, owner_token = ${owner}, lease_until = ${now + lease}
      WHERE execution_id = ${executionId} AND step_id = ${stepId} AND attempt = ${attempt}
      AND state = 'running' AND delivery_attempt < ${delivery}`
      const [claim] = yield* self.sql<ClaimRow>`SELECT * FROM better_workflows_claims
      WHERE execution_id = ${executionId} AND step_id = ${stepId} AND attempt = ${attempt}`
      if (claim?.owner_token === owner && claim.state === 'running') {
        yield* self.event(executionId, 'activity.started', { attempt, delivery }, stepId)
      }
      return claim!
    })
  }

  renewClaim(claim: ClaimRow, lease: number) {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.databaseNow()
      const rows = yield* self.sql`UPDATE better_workflows_claims SET lease_until = ${now + lease}
        WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt}
        AND owner_token = ${claim.owner_token} AND state = 'running' AND lease_until > ${now}
        RETURNING owner_token`
      return rows.length > 0
    })
  }

  finishClaim(
    claim: ClaimRow,
    result: string | null,
    failure: Failure | null,
    retryDelay?: number
  ) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lockRun(claim.execution_id)
        const now = yield* self.databaseNow()
        const rows =
          yield* self.sql`UPDATE better_workflows_claims SET state = 'completed', result_json = ${result}, failure_json = ${failure ? encode(failure) : null}
        WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt}
        AND owner_token = ${claim.owner_token} AND state = 'running' AND lease_until > ${now} RETURNING owner_token`
        if (rows.length)
          yield* self.event(
            claim.execution_id,
            failure ? 'activity.failed' : 'activity.completed',
            failure
              ? { attempt: claim.attempt, code: failure.code, retryable: failure.retryable }
              : { attempt: claim.attempt },
            claim.step_id
          )
        if (rows.length && failure?.retryable && retryDelay !== undefined) {
          const retryAt = (yield* self.now()) + retryDelay
          yield* self.sql`INSERT INTO better_workflows_retries(execution_id, step_id, attempt, deadline)
          VALUES (${claim.execution_id}, ${claim.step_id}, ${claim.attempt}, ${retryAt}) ON CONFLICT DO NOTHING`
          yield* self.event(
            claim.execution_id,
            'activity.retry-scheduled',
            { attempt: claim.attempt + 1, at: new Date(retryAt).toISOString() },
            claim.step_id
          )
        }
        return rows.length > 0
      })
    )
  }

  pendingRetries() {
    const self = this
    return Effect.gen(function* () {
      const now = yield* self.now()
      return yield* self.sql<RetryRow>`SELECT t.* FROM better_workflows_retries t
        JOIN better_workflows_runs r ON r.execution_id = t.execution_id
        WHERE r.namespace = ${self.namespace} AND t.delivered = 0 AND t.deadline <= ${now}
        AND r.control <> 'cancel' AND r.state NOT IN ('continued', 'completed', 'failed', 'cancelled')
        ORDER BY t.deadline, t.execution_id LIMIT 100`
    })
  }

  retryDelivered(retry: RetryRow) {
    return this.sql`UPDATE better_workflows_retries SET delivered = 1
      WHERE execution_id = ${retry.execution_id} AND step_id = ${retry.step_id} AND attempt = ${retry.attempt}`
  }

  heartbeat(claim: ClaimRow, details: JsonValue) {
    const self = this
    return self.sql.withTransaction(
      Effect.gen(function* () {
        yield* self.lockRun(claim.execution_id)
        const now = yield* self.databaseNow()
        const rows = yield* self.sql`UPDATE better_workflows_claims SET lease_until = lease_until
        WHERE execution_id = ${claim.execution_id} AND step_id = ${claim.step_id} AND attempt = ${claim.attempt}
        AND owner_token = ${claim.owner_token} AND state = 'running' AND lease_until > ${now} RETURNING owner_token`
        if (!rows.length)
          return yield* fail('LEASE_LOST', 'This activity attempt no longer owns its lease')
        yield* self.event(claim.execution_id, 'activity.heartbeat', details, claim.step_id)
      })
    )
  }

  history(executionId: string, after = 0, limit = 100) {
    const self = this
    return Effect.gen(function* () {
      if (
        !Number.isSafeInteger(after) ||
        after < 0 ||
        !Number.isSafeInteger(limit) ||
        limit < 1 ||
        limit > 1000
      ) {
        return yield* fail(
          'INVALID_PAGINATION',
          'after must be >= 0; limit must be between 1 and 1000'
        )
      }
      const rows = yield* self.sql<{
        sequence: number
        at: number
        type: string
        step_id: string | null
        details_json: string
      }>`
        SELECT * FROM better_workflows_events WHERE execution_id = ${executionId} AND sequence > ${after}
        ORDER BY sequence LIMIT ${limit + 1}`
      const hasMore = rows.length > limit
      const events = rows.slice(0, limit).map((row) => {
        const event = {
          sequence: row.sequence,
          at: new Date(row.at).toISOString(),
          type: row.type,
          details: decode<JsonValue>(row.details_json)
        }
        return row.step_id === null ? event : { ...event, stepId: row.step_id }
      })
      const page: HistoryPage = hasMore
        ? { events, nextCursor: events.at(-1)!.sequence }
        : { events }
      return page
    })
  }
}
