import { mkdir, realpath } from 'node:fs/promises'
import { basename, dirname, resolve } from 'node:path'
import { Effect, Layer, ManagedRuntime, Option, Redacted } from 'effect'
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { ClusterWorkflowEngine, RunnerAddress, SingleRunner } from 'effect/unstable/cluster'
import { PersistedQueue } from 'effect/unstable/persistence'
import { SqlClient } from 'effect/unstable/sql'
import { migrateAll, validateMigrations } from './schema-admin'
import { WorkflowError } from '../errors'
import type { WorkflowsOptions } from '../types'
import { identifier, milliseconds, positiveInteger } from './values'
import { sharedSqlite } from './shared-sqlite'

export function validateOptions(options: WorkflowsOptions): void {
  identifier(options.namespace, 'Namespace')
  if (
    options.migrations !== undefined &&
    options.migrations !== 'run' &&
    options.migrations !== 'validate'
  )
    throw new WorkflowError('INVALID_CONFIGURATION', 'migrations must be run or validate')
  const poll = milliseconds(options.pollInterval ?? '100ms')
  positiveInteger(poll, 'pollInterval')
  positiveInteger(options.execution?.workflows?.concurrency ?? 20, 'Workflow concurrency')
  const lease = milliseconds(options.lease?.duration ?? '30s')
  const refresh = milliseconds(options.lease?.refreshInterval ?? '10s')
  if (refresh < 1 || lease < refresh * 3) {
    throw new WorkflowError(
      'INVALID_CONFIGURATION',
      'Lease duration must be at least three times its positive refresh interval'
    )
  }
  for (const [name, queue] of Object.entries(options.queues)) {
    identifier(name, 'Queue name')
    positiveInteger(queue.concurrency, `Queue ${name} concurrency`)
    if (queue.globalConcurrency !== undefined)
      positiveInteger(queue.globalConcurrency, `Queue ${name} global concurrency`)
    if (queue.perKeyConcurrency !== undefined)
      positiveInteger(queue.perKeyConcurrency, `Queue ${name} per-key concurrency`)
  }
  for (const queue of options.execution?.activities?.queues ?? []) {
    if (!options.queues[queue]) throw new WorkflowError('UNKNOWN_QUEUE', queue)
  }
  if (options.topology === 'distributed') {
    if (options.storage.driver !== 'postgres')
      throw new WorkflowError('INVALID_TOPOLOGY', 'Distributed execution requires PostgreSQL')
    if (options.execution?.workflows?.enabled !== false && !options.cluster) {
      throw new WorkflowError(
        'INVALID_TOPOLOGY',
        'A workflow runner needs an advertised cluster address'
      )
    }
    if (options.cluster) {
      positiveInteger(options.cluster.address.port, 'Cluster port')
      if (options.cluster.address.port > 65535)
        throw new WorkflowError('INVALID_CONFIGURATION', 'Invalid cluster port')
    }
  }
}

export async function makeInfrastructure(options: WorkflowsOptions) {
  validateOptions(options)
  const database = Layer.mergeAll(await makeDatabase(options.storage), NodeCrypto.layer)
  const preparedDatabase = Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      if (options.migrations === 'validate') yield* validateMigrations(sql)
      else yield* migrateAll(options.namespace)
    })
  ).pipe(Layer.provideMerge(database))
  const poll = milliseconds(options.pollInterval ?? '100ms')
  const sharding = {
    entityMessagePollInterval: poll,
    entityReplyPollInterval: poll,
    refreshAssignmentsInterval: poll,
    shardLockExpiration: milliseconds(options.lease?.duration ?? '30s'),
    shardLockRefreshInterval: milliseconds(options.lease?.refreshInterval ?? '10s')
  }
  const cluster =
    options.topology === 'distributed'
      ? (await import('@effect/platform-node/NodeClusterSocket')).layer({
          storage: 'sql',
          clientOnly: options.execution?.workflows?.enabled === false,
          shardingConfig: {
            ...sharding,
            runnerAddress: options.cluster
              ? Option.some(RunnerAddress.RunnerAddress.make(options.cluster.address))
              : Option.none(),
            runnerListenAddress: options.cluster?.listenAddress
              ? Option.some(RunnerAddress.RunnerAddress.make(options.cluster.listenAddress))
              : Option.none()
          }
        })
      : SingleRunner.layer({ runnerStorage: 'sql', shardingConfig: sharding })
  const queueStorage = PersistedQueue.layerStoreSql({
    tableName: 'better_workflows_queue',
    pollInterval: poll,
    lockExpiration: milliseconds(options.lease?.duration ?? '30s'),
    lockRefreshInterval: milliseconds(options.lease?.refreshInterval ?? '10s')
  })
  const layer = Layer.mergeAll(
    ClusterWorkflowEngine.layer.pipe(Layer.provide(cluster)),
    PersistedQueue.layer.pipe(Layer.provide(queueStorage))
  ).pipe(Layer.provideMerge(preparedDatabase))
  return ManagedRuntime.make(layer)
}

export type Infrastructure = Awaited<ReturnType<typeof makeInfrastructure>>

export async function makeDatabase(config: WorkflowsOptions['storage']) {
  if (config.driver === 'postgres')
    return (await import('@effect/sql-pg/PgClient')).layer({
      url: Redacted.make(config.connectionString),
      maxConnections: config.maxConnections
    })
  let filename = config.filename
  if (filename !== ':memory:') {
    await mkdir(dirname(resolve(filename)), { recursive: true })
    try {
      filename = await realpath(filename)
    } catch (error) {
      if (!(error instanceof Error) || !('code' in error) || error.code !== 'ENOENT') throw error
      filename = resolve(await realpath(dirname(resolve(filename))), basename(filename))
    }
  }
  const layer =
    config.runtime === 'bun' || (config.runtime === 'auto' && 'Bun' in globalThis)
      ? (await import('@effect/sql-sqlite-bun/SqliteClient')).layer({ filename })
      : (await import('@effect/sql-sqlite-node/SqliteClient')).layer({ filename })
  return sharedSqlite(filename, layer)
}
