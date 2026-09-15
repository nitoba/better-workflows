import { Module } from '@nestjs/common';
import { join } from 'node:path';
import { WorkflowsModule } from 'better-workflows';
import { sqlite } from 'better-workflows/sqlite';
import { SettingsModule } from './config/settings.module.js';
import { AppSettings } from './config/settings.js';
import { ResearchModule } from './research/research.module.js';
import { HealthController } from './http/health.controller.js';
@Module({
  imports: [SettingsModule, WorkflowsModule.forRootAsync({ imports: [SettingsModule], inject: [AppSettings],
    useFactory: (settings: AppSettings) => ({ namespace: 'arxiv-research-app',
      storage: sqlite({ filename: join(settings.dataDir, 'workflows.sqlite'), runtime: 'bun' }),
      topology: 'single-node', execution: { workflows: { concurrency: 20 } },
      defaults: { queues: { concurrency: 2 } }, pollInterval: '100ms' }) }), ResearchModule],
  controllers: [HealthController],
})
export class AppModule {}
