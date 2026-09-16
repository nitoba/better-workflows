import { Inject, Injectable } from '@nestjs/common'
import { queueName } from './queues'
import type { QueueReference } from './queues'
import { Cause, Effect, Exit, Layer, ManagedRuntime, Crypto } from 'effect'
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { SqlClient } from 'effect/unstable/sql'
import { makeDatabase } from './internal/infrastructure'
import { Journal } from './internal/journal'
import { SqlAdministration } from './internal/administration'
import { migrateAll, migrationStatus, validateMigrations } from './internal/schema-admin'
import { identifier } from './internal/values'
import { WorkflowError, toFailure } from './errors'
import type {
  AdminBackend,
  AdminOptions,
  RetentionPlan,
  RetentionOptions,
  DeadLetterListOptions,
  DiscardDeadLetterOptions
} from './admin-types'

/**
 * Nest backend token used internally to construct WorkflowsAdmin.
 * @internal
 */
export const WORKFLOWS_ADMIN_BACKEND = Symbol.for('better-workflows/admin/backend')

/**
 * Administrative service provided by the root WorkflowsModule.
 * Schema writes require a standalone connection before workers start. Retention is
 * destructive and always needs an inspected preview plus explicit confirmation.
 * This service adds no authentication: restrict access in application controllers.
 */
@Injectable()
export class WorkflowsAdmin {
  /**
   * Constructed by Nest or createWorkflowsAdmin.
   * @param backend - Library-owned administrative backend.
   * @internal
   */
  constructor(@Inject(WORKFLOWS_ADMIN_BACKEND) private readonly backend: AdminBackend) {}
  /**
   * Schema inspection/validation and offline migration operations.
   * These operations address shared tables, not just rows in one namespace.
   */
  readonly migrations = {
    /**
     * Inspect the pinned engine, applied/pending migrations and required schema elements.
     * Does not create missing application tables or run workers.
     * @returns MigrationStatus; valid can be false for a new/unprepared database.
     * @throws WorkflowError for inconsistent ledgers, schemas newer than the package, or SQL errors.
     */
    status: () => this.backend.migrationStatus(),
    /**
     * Apply known additive journal and pinned-engine schemas without starting workers.
     * Use only through createWorkflowsAdmin before starting application workers; the
     * injected runtime-backed service deliberately rejects this operation.
     * @returns Post-migration status.
     * @throws WorkflowError with MIGRATION_RUNTIME_ACTIVE on the injected service,
     * or a schema/storage error when migration cannot complete.
     */
    run: () => this.backend.migrate(),
    /**
     * Require existing schemas to match this package without applying migrations.
     * @returns Valid MigrationStatus when required schemas are present.
     * @throws WorkflowError with MIGRATIONS_REQUIRED, SCHEMA_TOO_NEW or SCHEMA_CORRUPT.
     */
    validate: () => this.backend.validateMigrations()
  }
  /**
   * Read-only preview and explicit transactional removal of eligible terminal history.
   */
  readonly retention = {
    /**
     * Inspect terminal runs older than a cutoff, reporting candidates and blockers.
     * No execution data is deleted. Scan size includes blocked rows; this is not an
     * unbounded automatic cleanup job.
     * @param options - UTC cutoff and optional scan limit (default 100, maximum 1000).
     * @returns An intact plan to review and later pass to prune.
     * @throws WorkflowError for an invalid/future cutoff, limit or unprepared storage.
     */
    preview: (options: RetentionOptions) => this.backend.previewRetention(options),
    /**
     * Delete eligible history after revalidating an intact preview in one transaction.
     * Checks updates, active relatives, live leases and outstanding deliveries again.
     * Retains idempotency tombstones: the old key cannot start a new execution and its
     * result/history are no longer available. External files are not deleted for you.
     * @param plan - Unmodified plan previously returned by preview for this namespace.
     * @param options - Explicit confirmation; confirm must be true.
     * @returns Deleted execution IDs/count and retained tombstone count.
     * @throws WorkflowError for CONFIRMATION_REQUIRED, INVALID_RETENTION_PLAN,
     * RETENTION_PLAN_STALE or storage errors; the transaction does not partially prune.
     * @example
     * ```ts
     * import type { WorkflowsAdmin } from 'better-workflows'
     * declare const admin: WorkflowsAdmin
     * const plan = await admin.retention.preview({ before: '2025-01-01T00:00:00.000Z', limit: 100 })
     * console.log(plan.candidates, plan.blocked)
     * // Only after an operator has reviewed and approved the plan:
     * await admin.retention.prune(plan, { confirm: true })
     * ```
     */
    prune: (
      plan: RetentionPlan,
      options: {
        /** Explicit acknowledgement that the inspected candidates may be permanently removed. */
        readonly confirm: true
      }
    ) => this.backend.pruneRetention(plan, options?.confirm === true)
  }
  /**
   * Offline operations for shared queue permit limits; local worker slots are configured in modules.
   */
  readonly queues = {
    /**
     * Replace both shared queue limits in a drained namespace.
     * Stop producers/workers and align deployment configuration before restarting them.
     * Unlike QueueSettings inheritance, an omitted global/per-key field removes that
     * shared limit here; this is replacement, not a partial patch. Local concurrency
     * is not changed. Setting a policy does not register an activity or start workers.
     * @param queue - Stable logical queue reference.
     * @param options - Positive global/per-key limits; omission means unlimited for that field.
     * @returns Resolves when the policy is persisted.
     * @throws WorkflowError with NAMESPACE_NOT_DRAINED, LIVE_PERMITS or invalid settings.
     * @example
     * ```ts
     * import { defineQueue } from 'better-workflows'
     * import type { WorkflowsAdmin } from 'better-workflows'
     * declare const admin: WorkflowsAdmin
     * // Run only after draining workflows and stopping producers/workers.
     * await admin.queues.setLimits(defineQueue('reports'), { globalConcurrency: 6, perKeyConcurrency: 1 })
     * ```
     */
    setLimits: (queue: QueueReference, options: Parameters<AdminBackend['setQueueLimits']>[1]) =>
      this.backend.setQueueLimits(queueName(queue), options)
  }

  /** Discover and operate on infrastructure dead letters without exposing payloads by default. */
  readonly deadLetters = {
    /**
     * List bounded dead-letter metadata without returning payloads.
     * @param options - Optional queue, execution, activity, state and cursor filters.
     * @returns One page of dead-letter metadata and an optional next cursor.
     */
    list: (options?: DeadLetterListOptions) => this.backend.listDeadLetters(options),
    /**
     * Read one dead-letter record.
     * @param id - Stable dead-letter identifier.
     * @param options - Set `includePayload` deliberately to expose the raw persisted payload.
     * @returns The requested dead-letter metadata, optionally including its payload.
     */
    get: (id: string, options?: { readonly includePayload?: boolean }) =>
      this.backend.getDeadLetter(id, options),
    /**
     * Requeue one dead-letter record transactionally and idempotently.
     * @param id - Stable dead-letter identifier.
     * @returns The dead-letter record after the requeue decision.
     */
    requeue: (id: string) => this.backend.requeueDeadLetter(id),
    /**
     * Discard one dead-letter record and terminate its owner administratively.
     * @param id - Stable dead-letter identifier.
     * @param options - Required operator reason for the discard.
     * @returns The discarded dead-letter metadata.
     */
    discard: (id: string, options: DiscardDeadLetterOptions) =>
      this.backend.discardDeadLetter(id, options)
  }

  /** Flat aliases for callers that prefer the operation names from the CLI. */
  /**
   * List bounded dead-letter metadata without returning payloads.
   * @param options - Optional queue, execution, activity, state and cursor filters.
   * @returns One page of dead-letter metadata and an optional next cursor.
   */
  listDeadLetters(options?: DeadLetterListOptions) {
    return this.deadLetters.list(options)
  }
  /**
   * Read one dead-letter record.
   * @param id - Stable dead-letter identifier.
   * @param options - Set `includePayload` deliberately to expose the raw persisted payload.
   * @returns The requested dead-letter metadata, optionally including its payload.
   */
  getDeadLetter(id: string, options?: { readonly includePayload?: boolean }) {
    return this.deadLetters.get(id, options)
  }
  /**
   * Requeue one dead-letter record transactionally and idempotently.
   * @param id - Stable dead-letter identifier.
   * @returns The dead-letter record after the requeue decision.
   */
  requeueDeadLetter(id: string) {
    return this.deadLetters.requeue(id)
  }
  /**
   * Discard one dead-letter record and terminate its owner administratively.
   * @param id - Stable dead-letter identifier.
   * @param options - Required operator reason for the discard.
   * @returns The discarded dead-letter metadata.
   */
  discardDeadLetter(id: string, options: DiscardDeadLetterOptions) {
    return this.deadLetters.discard(id, options)
  }
}
/**
 * Administrative service owning a separate connection scope and a close operation.
 * Obtain it from createWorkflowsAdmin; always close it in finally. No workers run here.
 */
export class StandaloneWorkflowsAdmin extends WorkflowsAdmin {
  /**
   * Constructed by createWorkflowsAdmin.
   * @param backend - Library-owned database operations.
   * @param close - Releases the owned runtime/connection scope.
   * @internal
   */
  constructor(
    backend: AdminBackend,
    /**
     * Release this administrative connection scope; call in a finally block.
     * @returns Resolves after owned resources are disposed; does not cancel workflows.
     */
    readonly close: () => Promise<void>
  ) {
    super(backend)
  }
}

/**
 * Open an administrative connection without starting workflow or activity workers.
 * Does not automatically migrate the schema. status/validate are read-only; run
 * applies known schemas. Standalone retention and limit changes validate storage first.
 * @param options - Workflow namespace and database adapter settings.
 * @returns Administrative service with an explicit close method.
 * @throws WorkflowError for invalid namespace or backend/schema errors; connection setup can also reject.
 * @example
 * ```ts
 * import { createWorkflowsAdmin } from 'better-workflows/admin'
 * import { sqlite } from 'better-workflows/sqlite'
 * const admin = await createWorkflowsAdmin({ namespace: 'reports', storage: sqlite({ filename: './data/workflows.sqlite' }) })
 * try {
 *   console.log(await admin.migrations.status())
 *   await admin.migrations.run()
 *   await admin.migrations.validate()
 * } finally {
 *   await admin.close()
 * }
 * ```
 */
export async function createWorkflowsAdmin(
  options: AdminOptions
): Promise<StandaloneWorkflowsAdmin> {
  identifier(options.namespace, 'Namespace')
  const runtime = ManagedRuntime.make(
    Layer.mergeAll(await makeDatabase(options.storage), NodeCrypto.layer)
  )
  try {
    const sql = await runtime.runPromise(SqlClient.SqlClient)
    const journal = new Journal(sql, options.namespace)
    const admin = new SqlAdministration(journal)
    const run = async <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient | Crypto.Crypto>) => {
      const exit = await runtime.runPromiseExit(effect)
      if (Exit.isFailure(exit)) {
        const error = toFailure(Cause.squash(exit.cause))
        throw new WorkflowError(error.code, error.message)
      }
      return exit.value
    }
    const backend: AdminBackend = {
      migrationStatus: () => run(migrationStatus(sql)),
      migrate: () => run(migrateAll(options.namespace)),
      validateMigrations: () => run(validateMigrations(sql)),
      previewRetention: async (settings) => {
        await run(validateMigrations(sql))
        return run(admin.preview(settings))
      },
      pruneRetention: async (plan, confirm) => {
        await run(validateMigrations(sql))
        return run(admin.prune(plan, confirm))
      },
      setQueueLimits: async (queue, settings) => {
        await run(validateMigrations(sql))
        await run(admin.setQueueLimits(queue, settings))
      },
      listDeadLetters: async (settings) => {
        await run(validateMigrations(sql))
        return run(admin.listDeadLetters(settings))
      },
      getDeadLetter: async (id, settings) => {
        await run(validateMigrations(sql))
        return run(admin.getDeadLetter(id, settings?.includePayload === true))
      },
      requeueDeadLetter: async (id) => {
        await run(validateMigrations(sql))
        return run(admin.requeueDeadLetter(id))
      },
      discardDeadLetter: async (id, settings) => {
        await run(validateMigrations(sql))
        return run(admin.discardDeadLetter(id, settings))
      }
    }
    return new StandaloneWorkflowsAdmin(backend, () => runtime.dispose())
  } catch (error) {
    await runtime.dispose()
    throw error
  }
}

export type {
  AdminOptions,
  DeadLetter,
  DeadLetterListOptions,
  DeadLetterPage,
  DeadLetterState,
  DiscardDeadLetterOptions,
  MigrationStatus,
  RetentionOptions,
  RetentionPlan,
  RetentionResult
} from './admin-types'
