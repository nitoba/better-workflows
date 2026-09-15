import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { SettingsModule } from '../src/config/settings.module.js';
import { ArxivInfrastructureModule } from '../src/arxiv/arxiv.module.js';
import { DocumentsInfrastructureModule } from '../src/documents/documents.module.js';
import { ArxivHttpService } from '../src/arxiv/arxiv-http.service.js';
import { LiteparseService } from '../src/documents/liteparse.service.js';
import { ArxivIdSchema } from '../src/arxiv/arxiv.schemas.js';
import { parseAtom } from '../src/arxiv/atom.js';
import { AppSettings } from '../src/config/settings.js';
import { RunStoreModule } from '../src/runs/run-store.module.js';
import { RunRepository } from '../src/runs/run.repository.js';
import { StorageModule } from '../src/storage/storage.module.js';
import { ArtifactsService } from '../src/storage/artifacts.service.js';
@Module({ imports: [SettingsModule, ArxivInfrastructureModule, DocumentsInfrastructureModule, RunStoreModule, StorageModule] })
class SourceCheckModule {}
const app = await NestFactory.createApplicationContext(SourceCheckModule, { logger: ['error'], abortOnError: false });
try {
  const id = ArxivIdSchema.parse(process.argv[2] ?? '1706.03762');
  const settings = app.get(AppSettings); const http = app.get(ArxivHttpService); const files = app.get(ArtifactsService);
  const runId = crypto.randomUUID();
  app.get(RunRepository).create({ requestId: runId, recipient: 'source-check@local.test', query: null, arxivIds: [id], maxResults: 1,
    language: 'en', maxReviewRounds: 1, analysisModel: settings.env.ANALYSIS_MODEL, judgeModel: settings.env.JUDGE_MODEL, paperConcurrency: 1 });
  // This is a standalone source diagnostic, not a queued research intent.
  app.get(RunRepository).markTerminal(runId);
  const signal = AbortSignal.timeout(240_000);
  const xml = await http.get(new URL(`https://export.arxiv.org/api/query?id_list=${encodeURIComponent(id)}&max_results=1`), signal, 2_000_000);
  const paper = parseAtom(xml.data.toString('utf8'))[0];
  if (!paper) throw new Error('No paper returned by arXiv');
  const pdf = await http.get(new URL(paper.pdfUrl), signal, settings.env.PDF_MAX_BYTES);
  if (pdf.data.subarray(0, 1024).indexOf('%PDF-') < 0) throw new Error('arXiv did not return PDF bytes');
  const file = await files.put(runId, `${paper.id}.pdf`, pdf.data, 'application/pdf');
  const result = await app.get(LiteparseService).parse(file, signal);
  const markdown = await files.put(runId, `${paper.id}.md`, result.markdown, 'text/markdown');
  console.log(JSON.stringify({ paperId: paper.id, title: paper.title, totalPages: result.totalPages, characters: result.markdown.length,
    pdf: file.key, markdown: markdown.key, directory: files.directory }, null, 2));
} finally { await app.close(); }
