import { Effect } from 'effect'
import { ActivityError, toFailure } from '../errors'
import type { Failure } from '../errors'
import { runAsyncRound } from './async-round'
import type { RoundControl, RoundResult } from './async-round'

export interface Dispatcher<R> {
  <A>(operation: Effect.Effect<A, Failure, R>): Promise<A>
}

/**
 * Interpret durable commands on the SAME Effect fiber as the workflow. A
 * DurableDeferred suspension interrupts that fiber, not a JavaScript Promise.
 * User catch/finally therefore only see business outcomes, never suspension.
 * Commands submitted together are processed in submission order.
 */
export function interpretAsync<A, R>(
  execute: (dispatch: Dispatcher<R>) => Promise<A>
): Effect.Effect<A, Failure, R> {
  return Effect.suspend(() => {
    const abort = new AbortController()
    const commands: Effect.Effect<void, Failure, R>[] = []
    let control: RoundControl | undefined
    let outcome: RoundResult<A> | undefined
    let pending = 0
    let wake = (): void => {}

    const dispatch: Dispatcher<R> = <T>(operation: Effect.Effect<T, Failure, R>): Promise<T> => {
      if (!control?.active) return new Promise<T>(() => {})
      pending++
      return new Promise<T>((resolve, reject) => {
        commands.push(
          Effect.matchEffect(operation, {
            onSuccess: (value) =>
              Effect.sync(() => {
                pending--
                if (control?.active) resolve(value)
              }),
            onFailure: (failure) =>
              failure.code === 'WORKFLOW_CANCELLED'
                ? Effect.interrupt
                : failure.code === 'NON_DETERMINISTIC_WORKFLOW'
                  ? Effect.fail(failure)
                  : Effect.sync(() => {
                      pending--
                      if (control?.active) reject(new ActivityError(failure))
                    })
          })
        )
        wake()
      })
    }

    const program = Effect.gen(function* () {
      void runAsyncRound((round) => {
        control = round
        return execute(dispatch)
      }, abort.signal).then((result) => {
        outcome = result
        wake()
      })

      while (true) {
        if (outcome) {
          switch (outcome.status) {
            case 'success':
              if (pending !== 0) {
                return yield* Effect.fail({
                  code: 'UNAWAITED_COMMAND',
                  message: 'Every durable command must be awaited',
                  retryable: false
                })
              }
              return outcome.value
            case 'failure':
              return yield* Effect.fail(toFailure(outcome.error))
            case 'closed':
            case 'suspended':
              return yield* Effect.interrupt
          }
        }
        const command = commands.shift()
        if (command) {
          // A handler returning without awaiting a command must fail before that
          // command can suspend the interpreter and hide the early return.
          yield* Effect.yieldNow
          if (outcome) {
            commands.unshift(command)
            continue
          }
          yield* command
          // Give async continuations a chance to submit their next command.
          yield* Effect.yieldNow
          continue
        }
        yield* Effect.callback<void>((resume) => {
          wake = () => resume(Effect.void)
          if (commands.length > 0 || outcome) wake()
          return Effect.sync(() => {
            wake = () => {}
          })
        })
      }
    })

    return program.pipe(
      Effect.onExit(() =>
        Effect.sync(() => {
          abort.abort()
          commands.length = 0
          wake = () => {}
        })
      )
    )
  })
}
