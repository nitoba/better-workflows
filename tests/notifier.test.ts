import { expect, test } from 'bun:test'
import {
  ExecutionNotifier,
  executionNotificationChannel,
  executionNotificationPayload,
  publishLocalExecutionChange
} from '../src/internal/notifier'

const sleep = (duration: number) => new Promise((resolve) => setTimeout(resolve, duration))

test('local execution notifications wake waiters without polling every 100ms', async () => {
  const key = `notifier-${crypto.randomUUID()}`
  let revision = 1
  let reads = 0
  const notifier = new ExecutionNotifier(key, async () => {
    reads++
    return revision
  })
  try {
    const waiting = notifier.wait('execution-a', revision)
    await sleep(150)
    expect(reads).toBe(1)
    expect(notifier.waiterCount).toBe(1)

    revision = 2
    publishLocalExecutionChange(key, 'execution-a', revision)
    await waiting
    expect(notifier.waiterCount).toBe(0)

    const reconnecting = notifier.wait('execution-reconnect', revision)
    expect(notifier.waiterCount).toBe(1)
    notifier.reconnected()
    await reconnecting
    expect(notifier.waiterCount).toBe(0)
  } finally {
    notifier.shutdown()
  }
})

test('wait registration closes the read/notification race and cleans up aborts', async () => {
  const notifier = new ExecutionNotifier(`notifier-${crypto.randomUUID()}`, async () => 2, 1_000)
  try {
    await notifier.wait('execution-race', 1)
    expect(notifier.waiterCount).toBe(0)

    const controller = new AbortController()
    const waiting = notifier.wait('execution-abort', 2, { signal: controller.signal })
    expect(notifier.waiterCount).toBe(1)
    controller.abort()
    await expect(waiting).rejects.toMatchObject({ code: 'WAIT_ABORTED' })
    expect(notifier.waiterCount).toBe(0)
  } finally {
    notifier.shutdown()
  }
})

test('shutdown rejects outstanding waits and malformed PostgreSQL notifications are ignored', async () => {
  const notifier = new ExecutionNotifier(`notifier-${crypto.randomUUID()}`, async () => 1)
  const waiting = notifier.wait('execution-shutdown', 1)
  notifier.shutdown()
  await expect(waiting).rejects.toMatchObject({ code: 'RUNTIME_SHUTDOWN' })
  expect(notifier.waiterCount).toBe(0)

  expect(executionNotificationPayload('{"executionId":"x","revision":2}')).toEqual({
    executionId: 'x',
    revision: 2
  })
  expect(executionNotificationPayload('{"executionId":"x","revision":-1}')).toBeUndefined()
  expect(executionNotificationPayload('not-json')).toBeUndefined()
  expect(executionNotificationChannel('namespace')).toHaveLength(59)
})
