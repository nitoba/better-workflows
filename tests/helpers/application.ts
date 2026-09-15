import 'reflect-metadata';
import { Test } from '@nestjs/testing';
import { FastifyAdapter } from '@nestjs/platform-fastify';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { AppModule } from '../../src/app.module.js';
import { AppSettings } from '../../src/config/settings.js';
import { configureHttp } from '../../src/bootstrap.js';
import { ARXIV_FETCH } from '../../src/arxiv/arxiv-http.service.js';
import { LLM_GATEWAY } from '../../src/analysis/llm.gateway.js';
import { atomFixture, ScriptedLlm } from './fixtures.js';
export const TOKEN = 'test-integration-token-123456789';
export const headers = { authorization: `Bearer ${TOKEN}` };
export async function application(directory: string, smtpPort: number, llm = new ScriptedLlm(), ids = ['2609.00001v1'], badIds: string[] = []) {
  const defaults = new AppSettings();
  const settings = { ...defaults, dataDir: directory, projectDir: resolve(import.meta.dir, '../..'), arxivIntervalMs: 0,
    env: { ...defaults.env, DATA_DIR: directory, API_TOKEN: TOKEN, GOOGLE_GENERATIVE_AI_API_KEY: 'test-only-no-real-network',
      SMTP_HOST: '127.0.0.1', SMTP_PORT: smtpPort, PAPER_CONCURRENCY: 2, LLM_CONCURRENCY: 2 } };
  const requests: string[] = [];
  const pdf = await readFile(resolve(import.meta.dir, '../fixtures/sample-paper.pdf'));
  const module = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(AppSettings).useValue(settings)
    .overrideProvider(LLM_GATEWAY).useValue(llm)
    .overrideProvider(ARXIV_FETCH).useValue(async (url: string) => {
      requests.push(url);
      if (url.includes('/api/query')) return new Response(atomFixture(ids), { headers: { 'content-type': 'application/atom+xml' } });
      const bad = badIds.some(id => url.endsWith(id));
      return new Response(bad ? Buffer.from('not a PDF') : pdf, { headers: { 'content-type': 'application/pdf' } });
    }).compile();
  const app = module.createNestApplication<NestFastifyApplication>(new FastifyAdapter(), { logger: false });
  configureHttp(app); await app.init(); await app.getHttpAdapter().getInstance().ready();
  return { app, llm, requests, settings };
}
export async function eventually<T>(read: () => Promise<T>, valid: (value: T) => boolean, timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T;
  do { last = await read(); if (valid(last)) return last; await Bun.sleep(50); } while (Date.now() < deadline);
  throw new Error(`Timed out waiting for test condition: ${JSON.stringify(last)}`);
}
