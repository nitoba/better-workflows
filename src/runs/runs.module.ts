import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { RunStoreModule } from './run-store.module.js';
import { ControlQueue, RunActivities } from './run.activities.js';
@Module({ imports: [WorkflowsModule.forFeature({ name: 'research-control', imports: [RunStoreModule], activities: [RunActivities],
  queues: [{ queue: ControlQueue, concurrency: 4 }], exports: { activities: [RunActivities] } })], exports: [WorkflowsModule] })
export class RunsModule {}
