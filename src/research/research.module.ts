import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { StorageModule } from '../storage/storage.module.js';
import { RunStoreModule } from '../runs/run-store.module.js';
import { RunsModule } from '../runs/runs.module.js';
import { ArxivModule } from '../arxiv/arxiv.module.js';
import { DeliveryModule } from '../delivery/delivery.module.js';
import { PapersModule } from './papers.module.js';
import { ResearchWorkflow } from './workflows/research.workflow.js';
import { PaperWorkflow } from './workflows/paper.workflow.js';
import { ResearchService } from './research.service.js';
import { ResearchController } from './research.controller.js';
import { ControlReconciler } from './control-reconciler.service.js';
import { ApiTokenGuard } from '../http/api-token.guard.js';
@Module({
  imports: [StorageModule, RunStoreModule,
    WorkflowsModule.forFeature({ name: 'research-workflows', imports: [ArxivModule, DeliveryModule, RunsModule, PapersModule],
      workflows: [ResearchWorkflow], clients: [PaperWorkflow] })],
  providers: [ResearchService, ControlReconciler, ApiTokenGuard], controllers: [ResearchController],
  exports: [ResearchService],
})
export class ResearchModule {}
