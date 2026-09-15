import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { WorkflowsModule } from 'better-workflows'
import { postgres } from 'better-workflows/postgres'
import { Even, Odd, queues } from './advanced-contracts.mjs'

class App {}
Module({
  imports: [
    WorkflowsModule.forRoot({
      namespace: process.env.WORKFLOW_NAMESPACE,
      storage: postgres({ connectionString: process.env.WORKFLOWS_TEST_POSTGRES_URL }),
      topology: 'distributed',
      migrations: 'validate',
      queues,
      pollInterval: '20ms',
      lease: { duration: '3s', refreshInterval: '800ms' },
      execution: { workflows: { enabled: false } }
    })
  ],
  providers: [process.env.WORKFLOW_WORKER === 'even' ? Even : Odd]
})(App)
let app
try {
  app = await NestFactory.createApplicationContext(App, { logger: false, abortOnError: false })
  process.on('message', async (value) => {
    if (value === 'stop') {
      await app.close()
      process.disconnect?.()
    }
  })
  process.send?.({ event: 'ready' })
} catch (error) {
  console.error(error)
  process.exitCode = 1
  await app?.close()
  process.disconnect?.()
}
