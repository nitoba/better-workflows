/** A suspended async stack is deliberately abandoned, never rejected or resumed. */
export interface RoundControl {
  readonly active: boolean
  park<T>(): Promise<T>
}

export type RoundResult<A> =
  | { readonly status: 'success'; readonly value: A }
  | { readonly status: 'failure'; readonly error: Error }
  | { readonly status: 'suspended' }
  | { readonly status: 'closed' }

/**
 * Separates interpreter suspension from application exceptions. Each parked
 * promise is unrooted after the interpreter releases the round, so its async
 * stack can be collected. Never replace it with a shared, permanently rooted
 * "never" promise: that would retain every await continuation.
 */
export function runAsyncRound<A>(
  execute: (control: RoundControl) => Promise<A>,
  signal: AbortSignal
): Promise<RoundResult<A>> {
  return new Promise((resolve) => {
    let active = true
    const finish = (result: RoundResult<A>): void => {
      if (!active) return
      active = false
      signal.removeEventListener('abort', close)
      resolve(result)
    }
    const close = (): void => finish({ status: 'closed' })
    const control: RoundControl = {
      get active() {
        return active
      },
      park<T>(): Promise<T> {
        finish({ status: 'suspended' })
        return new Promise<T>(() => {})
      }
    }
    if (signal.aborted) {
      close()
      return
    }
    signal.addEventListener('abort', close, { once: true })
    // A callback can throw before producing a Promise. This handles both paths.
    Promise.resolve()
      .then(() => (active ? execute(control) : control.park<A>()))
      .then(
        (value) => finish({ status: 'success', value }),
        (error) =>
          finish({
            status: 'failure',
            error: error instanceof Error ? error : new Error(String(error))
          })
      )
  })
}
