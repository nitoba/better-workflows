import { Clock, Duration, Effect } from 'effect'

export const WORKFLOWS_TEST_CLOCK = Symbol.for('better-workflows/testing/clock')

export interface BusinessClock {
  now(): number
  schedule(delay: number, resume: () => void): () => void
}

export function effectClock(clock: BusinessClock): Clock.Clock {
  const nanos = () => BigInt(clock.now()) * 1_000_000n
  return {
    currentTimeMillisUnsafe: () => clock.now(),
    currentTimeMillis: Effect.sync(() => clock.now()),
    currentTimeNanosUnsafe: nanos,
    currentTimeNanos: Effect.sync(nanos),
    monotonicTimeNanosUnsafe: nanos,
    monotonicTimeNanos: Effect.sync(nanos),
    sleep: (duration) =>
      Effect.callback<void>((resume) => {
        const cancel = clock.schedule(Duration.toMillis(duration), () => resume(Effect.void))
        return Effect.sync(cancel)
      })
  }
}
