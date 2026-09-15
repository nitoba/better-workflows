import { test, expect } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Effect, ManagedRuntime } from 'effect'
import { SqlClient } from 'effect/unstable/sql'
import { sqlite } from '../src/sqlite'
import { makeDatabase } from '../src/internal/infrastructure'

test('SQLite administration and runtime share transaction serialization until the last owner closes', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bw-shared-'))
  const storage = sqlite({ filename: join(dir, 'data.sqlite') })
  const first = ManagedRuntime.make(await makeDatabase(storage))
  const second = ManagedRuntime.make(await makeDatabase(storage))
  try {
    const a = await first.runPromise(SqlClient.SqlClient)
    const b = await second.runPromise(SqlClient.SqlClient)
    expect(a).toBe(b)
    await first.runPromise(a`CREATE TABLE shared_test(value INTEGER)`)
    await Promise.all([
      first.runPromise(
        a.withTransaction(
          Effect.gen(function* () {
            yield* a`INSERT INTO shared_test VALUES(1)`
            yield* Effect.sleep('30 millis')
          })
        )
      ),
      second.runPromise(b.withTransaction(b`INSERT INTO shared_test VALUES(2)`))
    ])
    await first.dispose()
    expect(await second.runPromise(b`SELECT value FROM shared_test ORDER BY value`)).toEqual([
      { value: 1 },
      { value: 2 }
    ])
    await second.dispose()
    const third = ManagedRuntime.make(await makeDatabase(storage))
    try {
      const c = await third.runPromise(SqlClient.SqlClient)
      expect(c).not.toBe(a)
      expect(await third.runPromise(c`SELECT value FROM shared_test ORDER BY value`)).toEqual([
        { value: 1 },
        { value: 2 }
      ])
    } finally {
      await third.dispose()
    }
  } finally {
    await first.dispose()
    await second.dispose()
    await rm(dir, { recursive: true, force: true })
  }
})

test('separate memory testing stores never share state', async () => {
  const a = ManagedRuntime.make(await makeDatabase(sqlite({ filename: ':memory:' })))
  const b = ManagedRuntime.make(await makeDatabase(sqlite({ filename: ':memory:' })))
  try {
    const one = await a.runPromise(SqlClient.SqlClient)
    const two = await b.runPromise(SqlClient.SqlClient)
    expect(one).not.toBe(two)
    await a.runPromise(one`CREATE TABLE private_table(value INTEGER)`)
    expect(
      await b.runPromise(two`SELECT name FROM sqlite_master WHERE name='private_table'`)
    ).toEqual([])
  } finally {
    await a.dispose()
    await b.dispose()
  }
})
