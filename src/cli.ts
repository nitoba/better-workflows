#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises'
import { createWorkflowsAdmin } from './admin'
import type { RetentionPlan } from './admin'
import { sqlite } from './sqlite'
import { postgres } from './postgres'
import { WorkflowError } from './errors'

const usage = `better-workflows migrations status|run|validate
better-workflows retention preview --before <ISO date> --plan <file.json> [--limit 100]
better-workflows retention prune --plan <file.json> --confirm

Set WORKFLOWS_NAMESPACE and exactly one of WORKFLOWS_SQLITE_FILE or WORKFLOWS_DATABASE_URL.
Migration run is an offline operation. Retention prune requires an unchanged preview file.
`

async function main(): Promise<void> {
  const [group, command, ...args] = process.argv.slice(2)
  if (!group || group === '--help') {
    console.log(usage)
    return
  }
  const allowed = new Set(
    group === 'retention' && command === 'preview'
      ? ['--before', '--plan', '--limit']
      : group === 'retention' && command === 'prune'
        ? ['--plan', '--confirm']
        : []
  )
  const flags = new Map<string, string>()
  for (let i = 0; i < args.length; i++) {
    const key = args[i]!
    if (!allowed.has(key) || flags.has(key))
      throw new WorkflowError('INVALID_ARGUMENT', `Unknown or duplicate argument: ${key}`)
    if (key === '--confirm') flags.set(key, 'true')
    else {
      const value = args[++i]
      if (!value || value.startsWith('--'))
        throw new WorkflowError('INVALID_ARGUMENT', `Missing value for ${key}`)
      flags.set(key, value)
    }
  }
  const namespace = process.env.WORKFLOWS_NAMESPACE
  const filename = process.env.WORKFLOWS_SQLITE_FILE
  const connectionString = process.env.WORKFLOWS_DATABASE_URL
  if (!namespace || Boolean(filename) === Boolean(connectionString))
    throw new WorkflowError(
      'INVALID_CONFIGURATION',
      'Set namespace and exactly one database location'
    )
  const storage = filename
    ? sqlite({ filename })
    : postgres({ connectionString: connectionString! })
  const admin = await createWorkflowsAdmin({ namespace, storage })
  try {
    if (group === 'migrations') {
      if (command === 'status')
        console.log(JSON.stringify(await admin.migrations.status(), null, 2))
      else if (command === 'run') console.log(JSON.stringify(await admin.migrations.run(), null, 2))
      else if (command === 'validate')
        console.log(JSON.stringify(await admin.migrations.validate(), null, 2))
      else throw new WorkflowError('INVALID_ARGUMENT', usage)
    } else if (group === 'retention' && command === 'preview') {
      const before = flags.get('--before'),
        path = flags.get('--plan')
      if (!before || !path)
        throw new WorkflowError('INVALID_ARGUMENT', 'Preview requires --before and --plan')
      const plan = await admin.retention.preview({
        before,
        limit: Number(flags.get('--limit') ?? 100)
      })
      await writeFile(path, JSON.stringify(plan, null, 2) + '\n', { flag: 'wx', mode: 0o600 })
      console.log(JSON.stringify(plan, null, 2))
    } else if (group === 'retention' && command === 'prune') {
      const path = flags.get('--plan')
      if (!path || flags.get('--confirm') !== 'true')
        throw new WorkflowError('CONFIRMATION_REQUIRED', 'Prune requires --plan and --confirm')
      // SAFETY: the retention API validates the plan's namespace, integrity, identifiers and live state before any deletion.
      const plan = JSON.parse(await readFile(path, 'utf8')) as RetentionPlan
      console.log(JSON.stringify(await admin.retention.prune(plan, { confirm: true }), null, 2))
    } else throw new WorkflowError('INVALID_ARGUMENT', usage)
  } finally {
    await admin.close()
  }
}
main().catch((error: Error) => {
  console.error(error.message)
  process.exitCode = 1
})
