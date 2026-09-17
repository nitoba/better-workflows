#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises'
import { createWorkflowsAdmin } from './admin'
import type { RetentionPlan, WorkflowsStats } from './admin'
import type {
  DeadLetterListOptions,
  ScheduleDefinitionUpdate,
  ScheduleListOptions,
  ScheduleOccurrenceListOptions
} from './admin-types'
import type { JsonValue } from './types'
import { sqlite } from './sqlite'
import { postgres } from './postgres'
import { WorkflowError } from './errors'

const usage = `better-workflows stats
better-workflows status
better-workflows migrations status|run|validate
better-workflows retention preview --before <ISO date> --plan <file.json> [--limit 100]
better-workflows retention prune --plan <file.json> --confirm
better-workflows dead-letters list [--queue <name>] [--execution-id <id>] [--activity <name>] [--state <state>] [--cursor <id>] [--limit 100]
better-workflows dead-letters show <id> [--payload]
better-workflows dead-letters requeue <id>
better-workflows dead-letters discard <id> --reason "..."
better-workflows schedules list [--status <active|paused|orphaned>] [--cursor <name>] [--limit 100]
better-workflows schedules show <name>
better-workflows schedules occurrences <name> [--state <started|skipped|failed>] [--cursor <n>] [--limit 100]
better-workflows schedules pause <name>
better-workflows schedules resume <name>
better-workflows schedules remove <name> --confirm
better-workflows schedules trigger <name> [--idempotency-key <key>]
better-workflows schedules reconcile <name> --confirm --from now --workflow <name> --version <n> --type <cron|interval> [--expression <cron>] [--timezone <iana-zone>] [--interval-ms <n>] [--misfire <policy>] [--overlap <policy>] [--max-catch-up <n>] [--input-json <json>|--resolver]

Set WORKFLOWS_NAMESPACE and exactly one of WORKFLOWS_SQLITE_FILE or WORKFLOWS_DATABASE_URL.
Migration run is an offline operation. Retention prune requires an unchanged preview file.
`

const formatAge = (ageMs: number): string => {
  if (ageMs < 1_000) return `${Math.round(ageMs)}ms`
  const seconds = Math.floor(ageMs / 1_000)
  if (seconds < 60) return `${Number((ageMs / 1_000).toFixed(1))}s`
  const minutes = Math.floor(seconds / 60)
  const remainder = seconds % 60
  return `${minutes}m${remainder === 0 ? '' : `${remainder}s`}`
}

const formatStats = (namespace: string, stats: WorkflowsStats): string => {
  const queues = stats.queues.length
    ? stats.queues.map(
        (queue) =>
          `  ${queue.name} pending=${queue.pending} processing=${queue.processing} oldest=${formatAge(queue.oldestPendingAgeMs)}`
      )
    : ['  (none)']
  return [
    `Namespace: ${namespace}`,
    '',
    'Executions',
    `  Accepted:  ${stats.executions.accepted}`,
    `  Running:   ${stats.executions.running}`,
    `  Waiting:   ${stats.executions.waiting}`,
    `  Blocked:   ${stats.executions.blocked}`,
    `  Paused:    ${stats.executions.paused}`,
    `  Cancelling:${stats.executions.cancelling}`,
    '',
    'Queues',
    ...queues,
    '',
    'Dead letters',
    `  open=${stats.deadLetters.open} requeued=${stats.deadLetters.requeued} oldest=${formatAge(stats.deadLetters.oldestOpenAgeMs)}`,
    '',
    'Deadlines',
    `  timers=${stats.deadlines.dueTimers} retries=${stats.deadlines.overdueRetries} oldest=${formatAge(stats.deadlines.oldestLagMs)}`,
    '',
    'Schedules',
    `  active=${stats.schedules.active} paused=${stats.schedules.paused} overdue=${stats.schedules.overdue} oldest=${formatAge(stats.schedules.oldestLagMs)}`
  ].join('\n')
}

async function main(): Promise<void> {
  const [group, command, ...args] = process.argv.slice(2)
  if (!group || group === '--help') {
    console.log(usage)
    return
  }
  const statsCommand = group === 'stats' || group === 'status'
  const allowed = new Set(
    statsCommand
      ? []
      : group === 'retention' && command === 'preview'
        ? ['--before', '--plan', '--limit']
        : group === 'retention' && command === 'prune'
          ? ['--plan', '--confirm']
          : group === 'dead-letters' && command === 'list'
            ? ['--queue', '--execution-id', '--activity', '--state', '--cursor', '--limit']
            : group === 'dead-letters' && command === 'show'
              ? ['--payload']
              : group === 'dead-letters' && command === 'discard'
                ? ['--reason']
                : group === 'schedules' && command === 'list'
                  ? ['--status', '--cursor', '--limit']
                  : group === 'schedules' && command === 'occurrences'
                    ? ['--state', '--cursor', '--limit']
                    : group === 'schedules' && command === 'trigger'
                      ? ['--idempotency-key']
                      : group === 'schedules' && command === 'remove'
                        ? ['--confirm']
                        : group === 'schedules' && command === 'reconcile'
                          ? [
                              '--confirm',
                              '--from',
                              '--workflow',
                              '--version',
                              '--type',
                              '--expression',
                              '--timezone',
                              '--interval-ms',
                              '--misfire',
                              '--overlap',
                              '--max-catch-up',
                              '--input-json',
                              '--resolver'
                            ]
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
    if (key === '--confirm' || key === '--payload' || key === '--resolver') flags.set(key, 'true')
    else {
      const value = args[++i]
      if (!value || value.startsWith('--'))
        throw new WorkflowError('INVALID_ARGUMENT', `Missing value for ${key}`)
      flags.set(key, value)
    }
  }
  if (statsCommand && (command !== undefined || positional.length > 0 || flags.size > 0))
    throw new WorkflowError('INVALID_ARGUMENT', usage)
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
    if (
      group === 'schedules' &&
      ((command === 'list' && positional.length > 0) ||
        (command !== 'list' && positional.length !== 1))
    )
      throw new WorkflowError('INVALID_ARGUMENT', usage)
    if (statsCommand) console.log(formatStats(namespace, await admin.stats()))
    else if (group === 'migrations') {
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
    } else if (group === 'schedules' && command === 'list') {
      const statusValues = ['active', 'paused', 'orphaned'] as const
      const status = statusValues.find((value) => value === flags.get('--status'))
      if (flags.has('--status') && !status)
        throw new WorkflowError(
          'INVALID_ARGUMENT',
          `Unknown schedule status: ${flags.get('--status')}`
        )
      let listOptions: ScheduleListOptions = {}
      const cursor = flags.get('--cursor')
      if (status !== undefined) listOptions = { ...listOptions, status }
      if (cursor !== undefined) listOptions = { ...listOptions, cursor }
      if (flags.has('--limit'))
        listOptions = { ...listOptions, limit: Number(flags.get('--limit')) }
      console.log(JSON.stringify(await admin.listSchedules(listOptions), null, 2))
    } else if (group === 'schedules' && command === 'show') {
      console.log(JSON.stringify(await admin.getSchedule(positional[0]!), null, 2))
    } else if (group === 'schedules' && command === 'occurrences') {
      const stateValues = ['started', 'skipped', 'failed'] as const
      const state = stateValues.find((value) => value === flags.get('--state'))
      if (flags.has('--state') && !state)
        throw new WorkflowError(
          'INVALID_ARGUMENT',
          `Unknown schedule occurrence state: ${flags.get('--state')}`
        )
      let occurrenceOptions: ScheduleOccurrenceListOptions = {}
      const cursor = flags.get('--cursor')
      if (state !== undefined) occurrenceOptions = { ...occurrenceOptions, state }
      if (cursor !== undefined) occurrenceOptions = { ...occurrenceOptions, after: Number(cursor) }
      if (flags.has('--limit'))
        occurrenceOptions = { ...occurrenceOptions, limit: Number(flags.get('--limit')) }
      console.log(
        JSON.stringify(
          await admin.listScheduleOccurrences(positional[0]!, occurrenceOptions),
          null,
          2
        )
      )
    } else if (group === 'schedules' && command === 'pause') {
      console.log(JSON.stringify(await admin.pauseSchedule(positional[0]!), null, 2))
    } else if (group === 'schedules' && command === 'resume') {
      console.log(JSON.stringify(await admin.resumeSchedule(positional[0]!), null, 2))
    } else if (group === 'schedules' && command === 'remove') {
      if (flags.get('--confirm') !== 'true')
        throw new WorkflowError('CONFIRMATION_REQUIRED', 'Schedule removal requires --confirm')
      await admin.removeSchedule(positional[0]!, { confirm: true })
      console.log(JSON.stringify({ removed: positional[0] }, null, 2))
    } else if (group === 'schedules' && command === 'trigger') {
      const idempotencyKey = flags.get('--idempotency-key')
      console.log(
        JSON.stringify(
          await admin.triggerSchedule(
            positional[0]!,
            idempotencyKey === undefined ? undefined : { idempotencyKey }
          ),
          null,
          2
        )
      )
    } else if (group === 'schedules' && command === 'reconcile') {
      const workflow = flags.get('--workflow')
      const version = Number(flags.get('--version'))
      const type = flags.get('--type')
      const from = flags.get('--from')
      const misfireValue = flags.get('--misfire') ?? 'latest'
      const overlapValue = flags.get('--overlap') ?? 'allow'
      const maxCatchUp = Number(flags.get('--max-catch-up') ?? 100)
      if (
        flags.get('--confirm') !== 'true' ||
        from !== 'now' ||
        !workflow ||
        !Number.isSafeInteger(version) ||
        (type !== 'cron' && type !== 'interval') ||
        (misfireValue !== 'skip' && misfireValue !== 'latest' && misfireValue !== 'catch-up') ||
        (overlapValue !== 'allow' && overlapValue !== 'skip') ||
        !Number.isSafeInteger(maxCatchUp)
      )
        throw new WorkflowError('INVALID_ARGUMENT', usage)
      if (flags.has('--resolver') && flags.has('--input-json'))
        throw new WorkflowError('INVALID_ARGUMENT', 'Choose --resolver or --input-json, not both')
      if (type === 'cron' && !flags.get('--expression'))
        throw new WorkflowError('INVALID_ARGUMENT', 'Cron reconciliation requires --expression')
      if (type === 'interval' && !flags.has('--interval-ms'))
        throw new WorkflowError(
          'INVALID_ARGUMENT',
          'Interval reconciliation requires --interval-ms'
        )
      const misfire: ScheduleDefinitionUpdate['misfire'] =
        misfireValue === 'skip' ? 'skip' : misfireValue === 'catch-up' ? 'catch-up' : 'latest'
      const overlap: ScheduleDefinitionUpdate['overlap'] =
        overlapValue === 'skip' ? 'skip' : 'allow'
      let input: JsonValue = null
      if (flags.has('--input-json')) {
        try {
          input = JSON.parse(flags.get('--input-json')!)
        } catch {
          throw new WorkflowError('INVALID_ARGUMENT', 'Invalid --input-json value')
        }
      }
      const inputDefinition = flags.has('--resolver')
        ? { inputMode: 'resolver' as const }
        : flags.has('--input-json')
          ? { inputMode: 'static' as const, input }
          : { inputMode: 'none' as const }
      const commonDefinition = {
        workflow,
        workflowVersion: version,
        misfire,
        overlap,
        maxCatchUp,
        ...inputDefinition
      }
      const timezone = flags.get('--timezone')
      let definition: ScheduleDefinitionUpdate =
        type === 'cron'
          ? { ...commonDefinition, type, expression: flags.get('--expression')! }
          : { ...commonDefinition, type, intervalMs: Number(flags.get('--interval-ms')) }
      if (timezone !== undefined) definition = { ...definition, timezone }
      console.log(
        JSON.stringify(
          await admin.updateScheduleDefinition(positional[0]!, {
            confirm: true,
            from: 'now',
            definition
          }),
          null,
          2
        )
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
