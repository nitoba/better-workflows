import { Effect } from 'effect'
import { SqlClient } from 'effect/unstable/sql'
import { ShardingConfig, SqlMessageStorage, SqlRunnerStorage } from 'effect/unstable/cluster'
import { PersistedQueue } from 'effect/unstable/persistence'
import type { Failure } from '../errors'
import type { MigrationStatus } from '../admin-types'
import { Journal } from './journal'
import { ENGINE_VERSION } from './migrations'

const required: Readonly<Record<string, readonly string[]>> = {
  better_workflows_schema: ['version'],
  better_workflows_runs: [
    'execution_id',
    'namespace',
    'workflow_name',
    'version',
    'dedupe_key',
    'input_json',
    'state',
    'control',
    'event_sequence'
  ],
  better_workflows_commands: [
    'execution_id',
    'step_id',
    'ordinal',
    'signature',
    'scope',
    'protocol'
  ],
  better_workflows_events: ['execution_id', 'sequence', 'at'],
  better_workflows_signals: ['execution_id', 'event_key', 'consumed_by'],
  better_workflows_waits: ['execution_id', 'step_id', 'deadline', 'delivered'],
  better_workflows_retries: ['execution_id', 'step_id', 'attempt', 'deadline'],
  better_workflows_claims: ['execution_id', 'step_id', 'owner_token', 'lease_until', 'state'],
  better_workflows_branches: ['execution_id', 'group_id', 'branch_key', 'ordinal', 'state'],
  better_workflows_children: ['parent_id', 'child_id', 'close_policy', 'delivered'],
  better_workflows_sagas: ['execution_id', 'saga_id', 'state'],
  better_workflows_compensations: ['execution_id', 'saga_id', 'step_id', 'ordinal', 'state'],
  better_workflows_timers: ['execution_id', 'step_id', 'deadline', 'delivered'],
  better_workflows_limits: ['namespace', 'queue_name', 'global_limit', 'key_limit'],
  better_workflows_permits: ['namespace', 'queue_name', 'key_name', 'owner_token', 'lease_until'],
  better_workflows_tombstones: [
    'execution_id',
    'namespace',
    'workflow_name',
    'dedupe_key',
    'input_hash'
  ],
  cluster_messages: ['id', 'entity_type', 'entity_id', 'processed'],
  cluster_replies: ['id', 'request_id'],
  cluster_runners: ['machine_id', 'address'],
  better_workflows_queue: ['sequence', 'id', 'queue_name', 'element', 'state', 'acquired_by'],
  cluster_migrations: ['migration_id'],
  better_workflows_queue_migrations: ['migration_id']
}
const fail = (code: string, message: string) =>
  Effect.fail<Failure>({ code, message, retryable: false })

export function migrationStatus(sql: SqlClient.SqlClient) {
  return Effect.gen(function* () {
    const tables = yield* sql.onDialectOrElse({
      pg: () =>
        sql<{
          name: string
        }>`SELECT tablename AS name FROM pg_tables WHERE schemaname = current_schema()`,
      orElse: () => sql<{ name: string }>`SELECT name FROM sqlite_master WHERE type='table'`
    })
    const names = new Set(tables.map((row) => row.name))
    const applied = (table: string, column: string) =>
      names.has(table)
        ? sql<{
            version: number
          }>`SELECT ${sql(column)} AS version FROM ${sql(table)} ORDER BY ${sql(column)}`.pipe(
            Effect.map((rows) => rows.map((row) => row.version))
          )
        : Effect.succeed<number[]>([])
    const journal = yield* applied('better_workflows_schema', 'version')
    const cluster = yield* applied('cluster_migrations', 'migration_id')
    const queue = yield* applied('better_workflows_queue_migrations', 'migration_id')
    for (const [name, versions, expected] of [
      ['journal', journal, 2],
      ['cluster', cluster, 3],
      ['queue', queue, 2]
    ] as const) {
      if (versions.some((version) => version > expected))
        return yield* fail('SCHEMA_TOO_NEW', `${name} schema is newer than this package`)
      if (versions.some((version, index) => version !== index + 1))
        return yield* fail('SCHEMA_CORRUPT', `${name} migration ledger has gaps`)
    }
    const missing: string[] = []
    for (const [table, columns] of Object.entries(required)) {
      if (!names.has(table)) {
        missing.push(table)
        continue
      }
      const found = yield* sql.onDialectOrElse({
        pg: () =>
          sql<{
            name: string
          }>`SELECT column_name AS name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=${table}`,
        orElse: () => sql<{ name: string }>`PRAGMA table_info(${sql(table)})`
      })
      const existing = new Set(found.map((row) => row.name))
      for (const column of columns) if (!existing.has(column)) missing.push(`${table}.${column}`)
    }
    const pending = (values: number[], count: number) =>
      Array.from({ length: count }, (_, i) => i + 1).filter((value) => !values.includes(value))
    const status: MigrationStatus = {
      engine: ENGINE_VERSION,
      journal: { applied: journal, pending: pending(journal, 2) },
      cluster: { applied: cluster, pending: pending(cluster, 3) },
      queue: { applied: queue, pending: pending(queue, 2) },
      missing,
      valid:
        journal.length === 2 && cluster.length === 3 && queue.length === 2 && missing.length === 0
    }
    return status
  })
}

export function validateMigrations(sql: SqlClient.SqlClient) {
  return Effect.gen(function* () {
    const status = yield* migrationStatus(sql)
    if (!status.valid)
      return yield* fail(
        'MIGRATIONS_REQUIRED',
        `Run migrations before starting workers; missing: ${status.missing.join(', ')}`
      )
    return status
  })
}

/** Runs journal and pinned engine migrations without starting any workflow/queue worker. */
export function migrateAll(namespace: string) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql.onDialectOrElse({
          pg: () => sql`SELECT pg_advisory_xact_lock(748023196)`,
          orElse: () => Effect.void
        })
        const before = yield* migrationStatus(sql)
        const v1Tables = new Set(
          ['schema', 'runs', 'commands', 'events', 'signals', 'waits', 'retries', 'claims'].map(
            (name) => `better_workflows_${name}`
          )
        )
        if (
          before.journal.applied.length === 1 &&
          before.missing.some(
            (entry) =>
              v1Tables.has(entry.split('.')[0]!) &&
              !['better_workflows_commands.scope', 'better_workflows_commands.protocol'].includes(
                entry
              )
          )
        )
          return yield* fail(
            'SCHEMA_CORRUPT',
            'Version 1 journal is missing existing tables or columns; restore a consistent backup'
          )
        if (
          before.journal.applied.length === 2 &&
          before.missing.some(
            (entry) =>
              entry.startsWith('better_workflows_') && !entry.startsWith('better_workflows_queue')
          )
        )
          return yield* fail(
            'SCHEMA_CORRUPT',
            'Current journal schema is missing tables or columns; restore a consistent backup'
          )
        // Precreate native ledgers: a missing-regclass probe inside a PostgreSQL transaction would poison it.
        for (const table of ['cluster_migrations', 'better_workflows_queue_migrations']) {
          yield* sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (migration_id INTEGER PRIMARY KEY, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, name TEXT NOT NULL)`
        }
        yield* SqlMessageStorage.makeEncoded()
        yield* SqlRunnerStorage.make({}).pipe(
          Effect.provideService(ShardingConfig.ShardingConfig, {
            ...ShardingConfig.defaults,
            shardLockDisableAdvisory: true
          }),
          Effect.scoped
        )
        yield* PersistedQueue.makeStoreSql({ tableName: 'better_workflows_queue' }).pipe(
          Effect.scoped
        )
        yield* new Journal(sql, namespace).migrate()
        return yield* validateMigrations(sql)
      })
    )
  })
}
