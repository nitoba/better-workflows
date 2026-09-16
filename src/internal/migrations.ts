import { Effect } from 'effect'
import type { SqlClient } from 'effect/unstable/sql/SqlClient'

export const JOURNAL_VERSION = 6
export const ENGINE_VERSION = '4.0.0-rc.115'

/** All DDL is transactional on the supported PostgreSQL and SQLite adapters. */
export function migrateAdvanced(sql: SqlClient) {
  return Effect.gen(function* () {
    const versions = yield* sql<{ version: number }>`SELECT version FROM better_workflows_schema`
    if (versions.some((row) => row.version === JOURNAL_VERSION)) return
    if (!versions.some((row) => row.version === 3)) {
      if (!versions.some((row) => row.version === 2)) {
        // A table rebuild removes v1's global ordinal uniqueness while preserving its commands.
        yield* sql`CREATE TABLE better_workflows_commands_v2 (
          execution_id TEXT NOT NULL, step_id TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '',
          ordinal INTEGER NOT NULL, signature TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'scheduled',
          protocol INTEGER NOT NULL DEFAULT 2,
          PRIMARY KEY(execution_id, step_id), UNIQUE(execution_id, scope, ordinal)
        )`
        yield* sql`INSERT INTO better_workflows_commands_v2(execution_id, step_id, ordinal, signature, state, protocol)
          SELECT execution_id, step_id, ordinal, signature, state, 1 FROM better_workflows_commands`
        yield* sql`DROP TABLE better_workflows_commands`
        yield* sql`ALTER TABLE better_workflows_commands_v2 RENAME TO better_workflows_commands`
        yield* sql`CREATE TABLE better_workflows_branches (
          execution_id TEXT NOT NULL, group_id TEXT NOT NULL, branch_key TEXT NOT NULL, ordinal INTEGER NOT NULL,
          state TEXT NOT NULL DEFAULT 'pending', result_json TEXT, failure_json TEXT,
          PRIMARY KEY(execution_id, group_id, branch_key), UNIQUE(execution_id, group_id, ordinal)
        )`
        yield* sql`CREATE TABLE better_workflows_children (
          parent_id TEXT NOT NULL, step_id TEXT NOT NULL, child_id TEXT NOT NULL,
          close_policy TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, close_applied INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(parent_id, step_id), UNIQUE(child_id)
        )`
        yield* sql`CREATE INDEX better_workflows_child_outbox ON better_workflows_children(delivered, parent_id)`
        yield* sql`CREATE TABLE better_workflows_sagas (
          execution_id TEXT NOT NULL, saga_id TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'running',
          result_json TEXT, failure_json TEXT, PRIMARY KEY(execution_id, saga_id)
        )`
        yield* sql`CREATE TABLE better_workflows_compensations (
          execution_id TEXT NOT NULL, saga_id TEXT NOT NULL, step_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
          state TEXT NOT NULL DEFAULT 'registered', result_json TEXT NOT NULL, failure_json TEXT,
          PRIMARY KEY(execution_id, saga_id, step_id), UNIQUE(execution_id, saga_id, ordinal)
        )`
        yield* sql`CREATE TABLE better_workflows_timers (
          execution_id TEXT NOT NULL, step_id TEXT NOT NULL, deadline DOUBLE PRECISION NOT NULL,
          delivered INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(execution_id, step_id)
        )`
        yield* sql`CREATE INDEX better_workflows_timer_outbox ON better_workflows_timers(delivered, deadline)`
        yield* sql`CREATE TABLE better_workflows_limits (
          namespace TEXT NOT NULL, queue_name TEXT NOT NULL, global_limit INTEGER, key_limit INTEGER,
          PRIMARY KEY(namespace, queue_name)
        )`
        yield* sql`CREATE TABLE better_workflows_permits (
          namespace TEXT NOT NULL, queue_name TEXT NOT NULL, key_name TEXT NOT NULL,
          execution_id TEXT NOT NULL, step_id TEXT NOT NULL, attempt INTEGER NOT NULL,
          owner_token TEXT NOT NULL, lease_until DOUBLE PRECISION NOT NULL,
          PRIMARY KEY(execution_id, step_id, attempt)
        )`
        yield* sql`CREATE INDEX better_workflows_live_permits ON better_workflows_permits(namespace, queue_name, key_name, lease_until)`
        yield* sql`CREATE TABLE better_workflows_tombstones (
          execution_id TEXT PRIMARY KEY, namespace TEXT NOT NULL, workflow_name TEXT NOT NULL,
          version INTEGER NOT NULL, dedupe_key TEXT NOT NULL, input_hash TEXT NOT NULL,
          state TEXT NOT NULL, pruned_at DOUBLE PRECISION NOT NULL,
          UNIQUE(namespace, workflow_name, dedupe_key)
        )`
        yield* sql`INSERT INTO better_workflows_schema(version) VALUES (2)`
      }
      yield* sql`ALTER TABLE better_workflows_waits ADD COLUMN wake_requested INTEGER NOT NULL DEFAULT 0`
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_wait_wake_outbox ON better_workflows_waits(delivered, wake_requested, execution_id, step_id)`
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_wait_deadline_outbox ON better_workflows_waits(delivered, deadline, execution_id, step_id)`
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_wait_signal_wake ON better_workflows_waits(execution_id, signal_name, state, delivered, wake_requested, step_id)`
      // Preserve wake-ups that were already logically possible before this migration.
      yield* sql`UPDATE better_workflows_waits SET wake_requested = 1
        WHERE delivered = 0 AND (state <> 'pending' OR EXISTS (SELECT 1 FROM better_workflows_signals
          WHERE better_workflows_signals.execution_id = better_workflows_waits.execution_id
          AND better_workflows_signals.signal_name = better_workflows_waits.signal_name
          AND better_workflows_signals.consumed_by IS NULL))`
      yield* sql`INSERT INTO better_workflows_schema(version) VALUES (3)`
    }
    yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_reconciliations (
      execution_id TEXT PRIMARY KEY,
      namespace TEXT NOT NULL,
      result_json TEXT,
      failure_json TEXT,
      delivered INTEGER NOT NULL DEFAULT 0
    )`
    yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_reconciliation_outbox
      ON better_workflows_reconciliations(namespace, delivered, execution_id)`
    yield* sql`INSERT INTO better_workflows_schema(version) VALUES (4) ON CONFLICT DO NOTHING`
    if (!versions.some((row) => row.version === 5)) {
      const columns = yield* sql.onDialectOrElse({
        pg: () =>
          sql<{ name: string }>`SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=current_schema() AND table_name='better_workflows_runs'`,
        orElse: () => sql<{ name: string }>`PRAGMA table_info(better_workflows_runs)`
      })
      const existing = new Set(columns.map((column) => column.name))
      if (!existing.has('chain_id'))
        yield* sql`ALTER TABLE better_workflows_runs ADD COLUMN chain_id TEXT`
      if (!existing.has('generation'))
        yield* sql`ALTER TABLE better_workflows_runs ADD COLUMN generation INTEGER NOT NULL DEFAULT 0`
      if (!existing.has('continued_from'))
        yield* sql`ALTER TABLE better_workflows_runs ADD COLUMN continued_from TEXT`
      if (!existing.has('continued_to'))
        yield* sql`ALTER TABLE better_workflows_runs ADD COLUMN continued_to TEXT`
      yield* sql`UPDATE better_workflows_runs SET chain_id = execution_id WHERE chain_id IS NULL`
      yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS better_workflows_chain_generation
        ON better_workflows_runs(namespace, chain_id, generation)`
      yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS better_workflows_continued_to
        ON better_workflows_runs(namespace, continued_to) WHERE continued_to IS NOT NULL`
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_chain
        ON better_workflows_runs(namespace, chain_id, generation)`
      yield* sql`INSERT INTO better_workflows_schema(version) VALUES (5)`
    }
    if (!versions.some((row) => row.version === 6)) {
      yield* sql.onDialectOrElse({
        pg: () => sql`CREATE TABLE IF NOT EXISTS better_workflows_activity_deliveries (
          sequence BIGSERIAL PRIMARY KEY,
          namespace TEXT NOT NULL,
          queue_name TEXT NOT NULL,
          delivery_id TEXT NOT NULL,
          execution_id TEXT,
          payload_json TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          state TEXT NOT NULL DEFAULT 'pending',
          visible_at DOUBLE PRECISION NOT NULL,
          acquired_at DOUBLE PRECISION,
          acquired_by TEXT,
          last_failure TEXT,
          dead_letter_id TEXT,
          created_at DOUBLE PRECISION NOT NULL,
          updated_at DOUBLE PRECISION NOT NULL,
          UNIQUE(namespace, queue_name, delivery_id)
        )`,
        orElse: () => sql`CREATE TABLE IF NOT EXISTS better_workflows_activity_deliveries (
          sequence INTEGER PRIMARY KEY AUTOINCREMENT,
          namespace TEXT NOT NULL,
          queue_name TEXT NOT NULL,
          delivery_id TEXT NOT NULL,
          execution_id TEXT,
          payload_json TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          state TEXT NOT NULL DEFAULT 'pending',
          visible_at DOUBLE PRECISION NOT NULL,
          acquired_at DOUBLE PRECISION,
          acquired_by TEXT,
          last_failure TEXT,
          dead_letter_id TEXT,
          created_at DOUBLE PRECISION NOT NULL,
          updated_at DOUBLE PRECISION NOT NULL,
          UNIQUE(namespace, queue_name, delivery_id)
        )`
      })
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_activity_delivery_take
        ON better_workflows_activity_deliveries(namespace, queue_name, state, visible_at, sequence)`
      yield* sql`CREATE TABLE IF NOT EXISTS better_workflows_dead_letters (
        id TEXT PRIMARY KEY,
        namespace TEXT NOT NULL,
        queue_name TEXT NOT NULL,
        delivery_id TEXT NOT NULL,
        execution_id TEXT,
        step_id TEXT,
        activity_name TEXT,
        activity_version INTEGER,
        business_attempt INTEGER,
        delivery_attempt INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        reason_message TEXT NOT NULL,
        first_failed_at DOUBLE PRECISION NOT NULL,
        updated_at DOUBLE PRECISION NOT NULL,
        requeue_count INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        discard_reason TEXT,
        UNIQUE(namespace, delivery_id)
      )`
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_dead_letter_list
        ON better_workflows_dead_letters(namespace, state, id)`
      yield* sql`CREATE INDEX IF NOT EXISTS better_workflows_dead_letter_execution
        ON better_workflows_dead_letters(namespace, execution_id, state)`
      yield* sql`INSERT INTO better_workflows_schema(version) VALUES (6)`
    }
  })
}
