#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises'
import { createWorkflowsAdmin } from './admin'
import type { RetentionPlan } from './admin'
import type { DeadLetterListOptions } from './admin-types'
import { sqlite } from './sqlite'
import { postgres } from './postgres'
import { WorkflowError } from './errors'

const usage = `better-workflows migrations status|run|validate
better-workflows retention preview --before <ISO date> --plan <file.json> [--limit 100]
better-workflows retention prune --plan <file.json> --confirm
better-workflows dead-letters list [--queue <name>] [--execution-id <id>] [--activity <name>] [--state <state>] [--cursor <id>] [--limit 100]
better-workflows dead-letters show <id> [--payload]
better-workflows dead-letters requeue <id>
better-workflows dead-letters discard <id> --reason "..."

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
        : group === 'dead-letters' && command === 'list'
          ? ['--queue', '--execution-id', '--activity', '--state', '--cursor', '--limit']
          : group === 'dead-letters' && command === 'show'
            ? ['--payload']
            : group === 'dead-letters' && command === 'discard'
              ? ['--reason']
              : []
  )
  const flags = new Map<string, string>()
  const positional: string[] = []
  for (let i = 0; i < args.length; i++) {
    const key = args[i]!
    if (!key.startsWith('--')) {
      positional.push(key)
      continue
    }
    if (!allowed.has(key) || flags.has(key))
      throw new WorkflowError('INVALID_ARGUMENT', `Unknown or duplicate argument: ${key}`)
    if (key === '--confirm' || key === '--payload') flags.set(key, 'true')
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
    if (
      group === 'dead-letters' &&
      ((command === 'list' && positional.length > 0) ||
        (command !== 'list' && positional.length !== 1))
    )
      throw new WorkflowError('INVALID_ARGUMENT', usage)
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
    } else if (group === 'dead-letters' && command === 'list') {
      let listOptions: DeadLetterListOptions = {}
      const queue = flags.get('--queue')
      const executionId = flags.get('--execution-id')
      const activity = flags.get('--activity')
      const cursor = flags.get('--cursor')
      const stateValues = ['open', 'requeued', 'resolved', 'discarded'] as const
      const state = stateValues.find((value) => value === flags.get('--state'))
      if (flags.has('--state') && !state)
        throw new WorkflowError(
          'INVALID_ARGUMENT',
          `Unknown dead-letter state: ${flags.get('--state')}`
        )
      if (queue !== undefined) listOptions = { ...listOptions, queue }
      if (executionId !== undefined) listOptions = { ...listOptions, executionId }
      if (activity !== undefined) listOptions = { ...listOptions, activity }
      if (state !== undefined) listOptions = { ...listOptions, state }
      if (cursor !== undefined) listOptions = { ...listOptions, cursor }
      if (flags.has('--limit'))
        listOptions = { ...listOptions, limit: Number(flags.get('--limit')) }
      console.log(JSON.stringify(await admin.listDeadLetters(listOptions), null, 2))
    } else if (group === 'dead-letters' && command === 'show') {
      console.log(
        JSON.stringify(
          await admin.getDeadLetter(positional[0]!, { includePayload: flags.has('--payload') }),
          null,
          2
        )
      )
    } else if (group === 'dead-letters' && command === 'requeue') {
      console.log(JSON.stringify(await admin.requeueDeadLetter(positional[0]!), null, 2))
    } else if (group === 'dead-letters' && command === 'discard') {
      const reason = flags.get('--reason')
      if (!reason) throw new WorkflowError('INVALID_REASON', 'Discard requires --reason')
      console.log(
        JSON.stringify(await admin.discardDeadLetter(positional[0]!, { reason }), null, 2)
      )
    } else throw new WorkflowError('INVALID_ARGUMENT', usage)
  } finally {
    await admin.close()
  }
}
main().catch((error: Error) => {
  console.error(error.message)
  process.exitCode = 1
})
