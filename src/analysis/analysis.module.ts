import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { StorageModule } from '../storage/storage.module.js';
import { AppSettings } from '../config/settings.js';
import { AnalysisActivities, LlmQueue } from './analysis.activities.js';
import { GoogleGateway, LLM_GATEWAY } from './llm.gateway.js';
@Module({ providers: [GoogleGateway, { provide: LLM_GATEWAY, useExisting: GoogleGateway }], exports: [LLM_GATEWAY] })
export class GoogleModule {}
@Module({
  imports: [WorkflowsModule.forFeatureAsync({
    name: 'analysis', imports: [StorageModule, GoogleModule], inject: [AppSettings],
    activities: [AnalysisActivities],
    useFactory: (settings: AppSettings) => ({ queues: [{ queue: LlmQueue, concurrency: settings.env.LLM_CONCURRENCY,
      globalConcurrency: settings.env.LLM_CONCURRENCY, perKeyConcurrency: settings.env.LLM_CONCURRENCY }] }),
    exports: { activities: [AnalysisActivities] },
  })],
  exports: [WorkflowsModule],
})
export class AnalysisModule {}
