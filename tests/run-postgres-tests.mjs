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
const bun = process.env.BUN_BIN ?? 'bun'
const environment = { ...process.env, WORKFLOWS_TEST_POSTGRES_URL: connectionString }

async function run(args, env = environment) {
  const child = spawn(bun, args, {
    cwd: process.cwd(),
    env,
    stdio: 'inherit'
  })
  return new Promise((resolve, reject) => {
    child.once('error', reject)
    child.once('close', (code) => resolve(code ?? 1))
  })
}

let exitCode = 1
try {
  for (const args of [['run', 'build'], ['test'], ['run', 'test:node']]) {
    const env = args[1] === 'test:node' ? { ...environment } : environment
    if (args[1] === 'test:node') delete env.WORKFLOWS_TEST_POSTGRES_URL
    exitCode = await run(args, env)
    if (exitCode !== 0) break
  }
} finally {
  await container.stop()
}

process.exitCode = exitCode
