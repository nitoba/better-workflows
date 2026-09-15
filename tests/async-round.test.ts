import { expect, test } from 'bun:test'
import { runAsyncRound } from '../src/internal/async-round'

test('suspension does not enter user catch or finally blocks', async () => {
  const events: string[] = []
  const result = await runAsyncRound(async control => {
    try {
      await control.park()
      events.push('continued')
    } catch {
      events.push('caught')
    } finally {
      events.push('finally')
    }
  }, new AbortController().signal)
  await Promise.resolve()
  expect(result).toEqual({ status: 'suspended' })
  expect(events).toEqual([])
})

test('ordinary business exceptions can still be caught', async () => {
  const result = await runAsyncRound(async () => {
    try { throw new Error('business error') }
    catch { return 'recovered' }
  }, new AbortController().signal)
  expect(result).toEqual({ status: 'success', value: 'recovered' })
})

test('uncaught errors become a failure outcome', async () => {
  const error = new Error('failed')
  expect(await runAsyncRound(async () => { throw error }, new AbortController().signal))
    .toEqual({ status: 'failure', error })
})

test('closing a round invalidates late continuations without throwing into them', async () => {
  const abort = new AbortController()
  const barrier = Promise.withResolvers<void>()
  const events: string[] = []
  const result = runAsyncRound(async control => {
    await barrier.promise
    if (!control.active) return control.park()
    events.push('late commit')
  }, abort.signal)
  await Promise.resolve()
  abort.abort()
  expect(await result).toEqual({ status: 'closed' })
  barrier.resolve()
  await Promise.resolve()
  await Promise.resolve()
  expect(events).toEqual([])
})

test('an already closed round never invokes the application', async () => {
  let invoked = false
  const result = await runAsyncRound(async () => { invoked = true }, AbortSignal.abort())
  expect(result).toEqual({ status: 'closed' })
  expect(invoked).toBe(false)
})
