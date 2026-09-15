import { createHash } from 'node:crypto'
import { mkdir, rename, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { Inject, Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  Activities,
  Activity,
  Workflow,
  WorkflowsModule,
  InjectWorkflow,
  WorkflowClient,
  defineQueue
} from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { sqlite } from '../src/sqlite'

// Identity-only references: neither import starts a worker or opens storage.
const ReportQueue = defineQueue('reports.render')
const AuditQueue = defineQueue('audit.write')
const directory = resolve(process.env.WORKFLOWS_MODULAR_DIR ?? './.demo-modular')
const ReportInput = z.object({ id: z.string(), values: z.array(z.number()).min(1) })
const Summary = z.object({ id: z.string(), total: z.number() })
const BatchInput = z.array(ReportInput)

@Injectable()
class FileStore {
  async put(contents: string, context: ActivityContext): Promise<string> {
    const key = `${createHash('sha256').update(context.idempotencyKey).digest('hex')}.json`
    const output = join(directory, 'objects')
    await mkdir(output, { recursive: true })
    const temporary = join(output, `${key}.${process.pid}.tmp`)
    await writeFile(temporary, contents, { signal: context.signal })
    await rename(temporary, join(output, key))
    return key
  }
}
@Module({ providers: [FileStore], exports: [FileStore] })
class StorageModule {}

@Activities({ queue: AuditQueue })
class AuditActivities {
  constructor(@Inject(FileStore) private readonly files: FileStore) {}

  @Activity({ name: 'audit.record-report', version: 1, input: Summary, output: z.string() })
  record(summary: z.infer<typeof Summary>, context: ActivityContext): Promise<string> {
    // Real local audit artifact, not a mock email integration.
    return this.files.put(JSON.stringify({ type: 'report-created', summary }), context)
  }
}
@Module({
  imports: [
    WorkflowsModule.forFeature({
      name: 'audit',
      imports: [StorageModule],
      activities: [AuditActivities],
      queues: [{ queue: AuditQueue, concurrency: 1 }],
      defaults: { activities: { timeout: '10s' } },
      exports: { activities: [AuditActivities] }
      // AuditQueue stays PRIVATE; consumers import the durable activity contract instead.
    })
  ],
  exports: [WorkflowsModule]
})
class AuditModule {}

@Injectable()
class ReportsSettings {
  readonly concurrency = 2
}
@Module({ providers: [ReportsSettings], exports: [ReportsSettings] })
class SettingsModule {}

@Activities({ queue: ReportQueue })
class ReportActivities {
  constructor(@Inject(FileStore) private readonly files: FileStore) {}

  @Activity({ name: 'reports.render', version: 1, input: ReportInput, output: Summary })
  async render(input: z.infer<typeof ReportInput>, context: ActivityContext) {
    const summary = { id: input.id, total: input.values.reduce((sum, value) => sum + value, 0) }
    await this.files.put(JSON.stringify({ input, summary }), context)
    return summary
  }
}
@Workflow({ name: 'reports.generate', version: 1, input: ReportInput, output: Summary })
class GenerateReport {
  async run(input: z.infer<typeof ReportInput>, ctx: WorkflowContext) {
    const summary = await ctx.activities(ReportActivities).render(input, { stepId: 'render' })
    await ctx.activities(AuditActivities).record(summary, { stepId: 'audit' })
    return summary
  }
}
@Workflow({ name: 'reports.batch', version: 1, input: BatchInput, output: z.array(Summary) })
class GenerateBatch {
  async run(input: z.infer<typeof BatchInput>, ctx: WorkflowContext) {
    return ctx.map(
      'reports',
      input,
      { key: (report) => report.id, concurrency: 3 },
      (report, branch) => branch.child('generate', GenerateReport, report)
    )
  }
}
@Module({
  imports: [
    WorkflowsModule.forFeatureAsync({
      name: 'reports',
      imports: [StorageModule, SettingsModule, AuditModule],
      inject: [ReportsSettings],
      workflows: [GenerateReport, GenerateBatch],
      activities: [ReportActivities],
      useFactory: (settings: ReportsSettings) => ({
        queues: [{ queue: ReportQueue, concurrency: settings.concurrency }],
        defaults: { activities: { timeout: '30s' } }
      })
    })
  ],
  exports: [WorkflowsModule]
})
class ReportsModule {}

@Injectable()
class Demo {
  constructor(
    @InjectWorkflow(GenerateBatch) readonly batches: WorkflowClient<typeof GenerateBatch>
  ) {}
}
@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'modular-demo',
      storage: sqlite({ filename: join(directory, 'workflows.sqlite') }),
      defaults: { queues: { concurrency: 4 } }
      // Root has NO queue list and knows nothing about the feature queues.
    }),
    ReportsModule
  ],
  providers: [Demo]
})
class AppModule {}

const app = await NestFactory.createApplicationContext(AppModule, {
  logger: false,
  abortOnError: false
})
app.enableShutdownHooks()
try {
  const handle = await app.get(Demo).batches.start([
    { id: 'first', values: [10, 20] },
    { id: 'second', values: [4, 5, 6] },
    { id: 'third', values: [7, 8] }
  ])
  console.log('Modular reports:', await handle.result({ timeout: '30s' }))
  console.log('Execution:', handle.executionId)
  console.log('JSON reports and audit records:', join(directory, 'objects'))
} finally {
  await app.close()
}
