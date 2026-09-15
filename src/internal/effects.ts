import { Effect } from 'effect'
import { SqlError } from 'effect/unstable/sql/SqlError'
import { toFailure } from '../errors'
import type { Failure } from '../errors'

/** Infrastructure failure must never trigger a user's business catch/compensation. */
export function durable<A, R>(
  operation: Effect.Effect<A, Failure | SqlError, R>
): Effect.Effect<A, Failure, R> {
  return operation.pipe(
    Effect.catchIf((error): error is SqlError => error instanceof SqlError, Effect.die)
  )
}

export function promised<A>(
  operation: (signal: AbortSignal) => Promise<A>
): Effect.Effect<A, Failure> {
  return Effect.tryPromise({ try: operation, catch: toFailure })
}
