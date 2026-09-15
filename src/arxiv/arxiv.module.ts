import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { StorageModule } from '../storage/storage.module.js';
import { ArxivHttpService, ARXIV_FETCH } from './arxiv-http.service.js';
import { ArxivActivities, ArxivQueue } from './arxiv.activities.js';
@Module({ imports: [StorageModule], providers: [ArxivHttpService, { provide: ARXIV_FETCH, useValue: (url: string, init: RequestInit) => fetch(url, init) }], exports: [ArxivHttpService] })
export class ArxivInfrastructureModule {}
@Module({ imports: [WorkflowsModule.forFeature({ name: 'arxiv', imports: [StorageModule, ArxivInfrastructureModule],
  activities: [ArxivActivities], queues: [{ queue: ArxivQueue, concurrency: 1, globalConcurrency: 1 }],
  exports: { activities: [ArxivActivities] } })], exports: [WorkflowsModule] })
export class ArxivModule {}
