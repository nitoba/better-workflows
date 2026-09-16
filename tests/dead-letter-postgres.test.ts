import { test, expect } from 'bun:test'
import { Effect, Layer, ManagedRuntime } from 'effect'
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { SqlClient } from 'effect/unstable/sql'
import { createWorkflowsAdmin } from '../src/admin'
import { postgres } from '../src/postgres'
import { makeDatabase } from '../src/internal/infrastructure'

const connectionString = process.env.WORKFLOWS_TEST_POSTGRES_URL

test.skipIf(!connectionString)(
  'PostgreSQL concurrent admins requeue one dead-letter delivery',
  async () => {
    if (!connectionString) return
    const namespace = `dead-letter-pg-${process.pid}-${Date.now()}`
    const storage = postgres({ connectionString, maxConnections: 4 })
    const first = await createWorkflowsAdmin({ namespace, storage })
    const second = await createWorkflowsAdmin({ namespace, storage })
    const runtime = ManagedRuntime.make(
      Layer.mergeAll(await makeDatabase(storage), NodeCrypto.layer)
    )
    try {
      await first.migrations.run()
      const sql = await runtime.runPromise(SqlClient.SqlClient)
      await runtime.runPromise(
        sql`INSERT INTO better_workflows_dead_letters
        (id, namespace, queue_name, delivery_id, execution_id, step_id, activity_name,
         activity_version, business_attempt, delivery_attempt, reason_code, reason_message,
         first_failed_at, updated_at, requeue_count, state, payload_json)
       VALUES ('pg-dead-letter', ${namespace}, 'work', 'pg-delivery', NULL, 'step',
         'missing.activity', 1, 1, 2, 'UNKNOWN_ACTIVITY', 'missing activity',
         ${Date.now()}, ${Date.now()}, 0, 'open', '{}')`
      )
      const results = await Promise.all([
        first.requeueDeadLetter('pg-dead-letter'),
        second.requeueDeadLetter('pg-dead-letter')
      ])
      expect(results.map((result) => result.requeueCount)).toEqual([1, 1])
      const deliveries = await runtime.runPromise(
        sql<{ delivery_id: string }>`SELECT delivery_id FROM better_workflows_activity_deliveries
        WHERE namespace=${namespace} ORDER BY delivery_id`
      )
      expect(deliveries).toEqual([{ delivery_id: 'pg-dead-letter:r1' }])
    } finally {
      const sql = await runtime.runPromise(SqlClient.SqlClient)
      await runtime.runPromise(
        Effect.gen(function* () {
          yield* sql`DELETE FROM better_workflows_activity_deliveries WHERE namespace=${namespace}`
          yield* sql`DELETE FROM better_workflows_dead_letters WHERE namespace=${namespace}`
        })
      )
      await runtime.dispose()
      await first.close()
      await second.close()
    }
  }
)
