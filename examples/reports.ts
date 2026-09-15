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
  WorkflowClient,
  InjectWorkflow,
  defineSignal
} from '../src'
import type { ActivityContext, WorkflowContext } from '../src'
import { sqlite } from '../src/sqlite'

const Input = z.object({ reportId: z.string().min(1), values: z.array(z.number()).min(1) })
const Output = z.object({ objectKey: z.string(), sum: z.number(), average: z.number() })
const Approval = defineSignal('reports.approval', z.object({ approved: z.boolean() }))
type ReportInput = z.infer<typeof Input>
type ReportOutput = z.infer<typeof Output>
const directory = resolve(process.env.WORKFLOWS_DEMO_DIR ?? './.demo')

@Injectable()
class ReportsService {
  async generate(input: ReportInput, context: ActivityContext): Promise<ReportOutput> {
    const sum = input.values.reduce((total, value) => total + value, 0)
    const average = sum / input.values.length
    const objectKey = `${createHash('sha256').update(context.idempotencyKey).digest('hex')}.json`
    const output = join(directory, 'reports')
    await mkdir(output, { recursive: true })
    // This writes a real JSON report, not a simulated PDF or email integration.
    // Repeated deliveries publish the same deterministic bytes at the same key.
    const temporary = join(output, `${objectKey}.${process.pid}.${context.attempt}.tmp`)
    await writeFile(temporary, JSON.stringify({ ...input, sum, average }, null, 2), {
      signal: context.signal
    })
    await rename(temporary, join(output, objectKey))
    await context.heartbeat({ objectKey })
    return { objectKey, sum, average }
  }
}

@Activities()
class ReportActivities {
  constructor(@Inject(ReportsService) private readonly reports: ReportsService) {}

  @Activity({
    name: 'reports.generate',
    version: 1,
    queue: 'reports',
    input: Input,
    output: Output,
    timeout: '30s'
  })
  generate(input: ReportInput, context: ActivityContext): Promise<ReportOutput> {
    return this.reports.generate(input, context)
  }
}

@Workflow({
  name: 'reports',
  version: 1,
  input: Input,
  output: Output,
  signals: [Approval],
  idempotencyKey: (input) => input.reportId
})
class GenerateReport {
  async run(input: ReportInput, ctx: WorkflowContext): Promise<ReportOutput> {
    const report = await ctx
      .activities(ReportActivities)
      .generate(input, { stepId: 'generate-report' })
    await ctx.sleep('review-delay', '1s')
    const decision = await ctx.waitForSignal('approval', Approval, { timeout: '7d' })
    if (!decision.approved) throw new Error('Report was rejected')
    return report
  }
}

@Injectable()
class ReportsDemo {
  constructor(
    @InjectWorkflow(GenerateReport) readonly reports: WorkflowClient<typeof GenerateReport>
  ) {}
}

@Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: 'reports-demo',
      storage: sqlite({ filename: join(directory, 'workflows.sqlite') }),
      queues: { reports: { concurrency: 2 } }
    }),
    WorkflowsModule.forFeature([GenerateReport])
  ],
  providers: [GenerateReport, ReportActivities, ReportsService, ReportsDemo]
})
class AppModule {}

const app = await NestFactory.createApplicationContext(AppModule, { logger: false })
app.enableShutdownHooks()
try {
  const client = app.get(ReportsDemo).reports
  const id = process.argv[2] ?? 'report-001'
  const handle = await client.start({ reportId: id, values: [10, 20, 30] })
  console.log('Execution:', handle.executionId, 'created:', handle.created)
  // Run without --approve to leave a durable approval wait in the database.
  // Run again with the same report ID and --approve: completed work is replayed.
  if (process.argv.includes('--approve')) {
    await handle.signal(Approval, { approved: true }, { idempotencyKey: `approval:${id}` })
    console.log('Result:', await handle.result({ timeout: '30s' }))
  } else {
    await new Promise((resolve) => setTimeout(resolve, 1500))
    console.log(await handle.describe())
    console.log(`Approve with: bun run example ${id} --approve`)
  }
} finally {
  await app.close()
}
