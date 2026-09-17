import { existsSync } from 'node:fs'
import { spawn } from 'node:child_process'
import { join } from 'node:path'
import { PostgreSqlContainer } from '@testcontainers/postgresql'

const userId = process.getuid?.()
const runtimeDirectory =
  process.env.XDG_RUNTIME_DIR ?? (userId === undefined ? undefined : `/run/user/${userId}`)
const podmanSocket =
  runtimeDirectory === undefined ? undefined : join(runtimeDirectory, 'podman/podman.sock')
if (!process.env.DOCKER_HOST && podmanSocket && existsSync(podmanSocket))
  process.env.DOCKER_HOST = `unix://${podmanSocket}`
if (process.env.DOCKER_HOST?.includes('/podman/'))
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= 'true'

const container = await new PostgreSqlContainer('postgres:16-alpine').start()
const connectionString = container.getConnectionUri()
const testProcess = spawn(
  process.env.BUN_BIN ?? 'bun',
  ['test', 'tests/dead-letter-postgres.test.ts', 'tests/observability.test.ts'],
  {
    cwd: process.cwd(),
    env: { ...process.env, WORKFLOWS_TEST_POSTGRES_URL: connectionString },
    stdio: 'inherit'
  }
)

let exitCode = 1
try {
  exitCode = await new Promise((resolve, reject) => {
    testProcess.once('error', reject)
    testProcess.once('close', (code) => resolve(code ?? 1))
  })
} finally {
  await container.stop()
}

process.exitCode = exitCode
