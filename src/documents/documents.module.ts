import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { StorageModule } from '../storage/storage.module.js';
import { LiteparseService } from './liteparse.service.js';
import { DocumentActivities, ParseQueue } from './document.activities.js';
@Module({ imports: [StorageModule], providers: [LiteparseService], exports: [LiteparseService] })
export class DocumentsInfrastructureModule {}
@Module({ imports: [WorkflowsModule.forFeature({ name: 'documents', imports: [StorageModule, DocumentsInfrastructureModule],
  activities: [DocumentActivities], queues: [{ queue: ParseQueue, concurrency: 1 }],
  exports: { activities: [DocumentActivities] } })], exports: [WorkflowsModule] })
export class DocumentsModule {}
