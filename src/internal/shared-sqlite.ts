import { Effect, Exit, Layer, ManagedRuntime } from 'effect'
import { SqlClient } from 'effect/unstable/sql'
import type { SqlError } from 'effect/unstable/sql/SqlError'

interface SharedConnection {
  references: number
  readonly runtime: ManagedRuntime.ManagedRuntime<SqlClient.SqlClient, SqlError>
}

const connections = new Map<string, SharedConnection>()

/** Sync SQLite busy waits block the event loop: share its async transaction semaphore across local owners. */
export function sharedSqlite(
  filename: string,
  layer: Layer.Layer<SqlClient.SqlClient, SqlError>
): Layer.Layer<SqlClient.SqlClient, SqlError> {
  if (filename === ':memory:') return layer
  return Layer.effect(
    SqlClient.SqlClient,
    Effect.acquireRelease(
      Effect.sync(() => {
        let connection = connections.get(filename)
        if (!connection) {
          connection = { references: 0, runtime: ManagedRuntime.make(layer) }
          connections.set(filename, connection)
        }
        connection.references++
        return connection
      }),
      (connection) =>
        Effect.promise(async () => {
          connection.references--
          if (connection.references === 0) {
            connections.delete(filename)
            await connection.runtime.dispose()
          }
        })
    ).pipe(
      Effect.flatMap((connection) =>
        Effect.promise(() => connection.runtime.runPromiseExit(SqlClient.SqlClient))
      ),
      Effect.flatMap((exit) =>
        Exit.isSuccess(exit) ? Effect.succeed(exit.value) : Effect.failCause(exit.cause)
      )
    )
  )
}
