import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { ArxivModule } from '../arxiv/arxiv.module.js';
import { DocumentsModule } from '../documents/documents.module.js';
import { AnalysisModule } from '../analysis/analysis.module.js';
import { RunsModule } from '../runs/runs.module.js';
import { PaperWorkflow } from './workflows/paper.workflow.js';
@Module({ imports: [WorkflowsModule.forFeature({ name: 'paper-workflows',
  imports: [ArxivModule, DocumentsModule, AnalysisModule, RunsModule], workflows: [PaperWorkflow] })], exports: [WorkflowsModule] })
export class PapersModule {}
