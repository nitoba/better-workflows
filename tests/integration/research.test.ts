import { afterAll, beforeAll, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import { application, eventually, headers } from '../helpers/application.js';
import { ScriptedLlm } from '../helpers/fixtures.js';
import { smtpFixture } from '../helpers/smtp.js';
import { ResearchService } from '../../src/research/research.service.js';
import { AppSettings } from '../../src/config/settings.js';
import type { ResearchResult } from '../../src/research/research.schemas.js';
let fixture: Awaited<ReturnType<typeof smtpFixture>> | undefined;
let smtpPort = Number(process.env.SMTP_PORT ?? 1025);
beforeAll(async () => {
  if (!process.env.MAILPIT_API_URL) { fixture = await smtpFixture(); smtpPort = fixture.port; }
});
afterAll(async () => { await fixture?.close(); });
async function complete(app: NestFastifyApplication, runId: string): Promise<ResearchResult> {
  const response = await eventually(() => app.inject({ method: 'GET', url: `/api/research-runs/${runId}/result`, headers }),
    response => response.json().ready === true || ['failed', 'cancelled'].includes(response.json().status));
  const body = response.json(); if (!body.ready) throw new Error(JSON.stringify(body));
  return body.result;
}
async function start(app: NestFastifyApplication, overrides = {}) {
  const request = { requestId: crypto.randomUUID(), recipient: 'research-tests@local.test', arxivIds: ['2609.00001v1'], maxResults: 2, ...overrides };
  const response = await app.inject({ method: 'POST', url: '/api/research-runs', headers, payload: request });
  expect(response.statusCode).toBe(202);
  return { request, body: response.json() };
}
test('real Nest+GitHub dependency+SQLite+LiteParse+SMTP, author/review loop and HTTP idempotency', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'research-e2e-'));
  const setup = await application(dir, smtpPort);
  try {
    expect((await setup.app.inject({ method: 'GET', url: '/api/research-runs' })).statusCode).toBe(401);
    expect((await setup.app.inject({ method: 'POST', url: '/api/research-runs', headers, payload: {} })).statusCode).toBe(400);
    const { request, body } = await start(setup.app);
    const result = await complete(setup.app, body.runId);
    expect(result.approved).toBe(1); expect(result.papers[0]?.rounds).toBe(2);
    expect(setup.llm.calls.map(call => call.role)).toEqual(['extract', 'author', 'judge', 'author', 'judge']);
    const refs = (await setup.app.inject({ method: 'GET', url: `/api/research-runs/${body.runId}/artifacts`, headers })).json();
    expect(refs.some((r: { label: string }) => r.label.endsWith('/document.md'))).toBe(true);
    expect(refs.some((r: { label: string }) => r.label.endsWith('/round-2/quality.json'))).toBe(true);
    const download = await setup.app.inject({ method: 'GET', url: `/api/research-runs/${body.runId}/artifacts/${result.digest.key}`, headers });
    expect(download.statusCode).toBe(200); expect(download.body).toContain('APROVADO PELO REVISOR');
    const duplicate = await setup.app.inject({ method: 'POST', url: '/api/research-runs', headers, payload: request });
    expect(duplicate.statusCode).toBe(202); expect(duplicate.json().created).toBe(false);
    expect(duplicate.json().executionId).toBe(body.executionId);
    expect((await setup.app.inject({ method: 'POST', url: '/api/research-runs', headers, payload: { ...request, recipient: 'other@local.test' } })).statusCode).toBe(409);
    expect(setup.llm.calls.length).toBe(5);
    if (process.env.MAILPIT_API_URL) {
      const listing = await fetch(`${process.env.MAILPIT_API_URL}/api/v1/messages`).then(r => r.json());
      const message = listing.messages.find((m: { Subject: string }) => m.Subject.includes(body.runId));
      expect(message).toBeDefined();
      const detail = await fetch(`${process.env.MAILPIT_API_URL}/api/v1/message/${message.ID}`).then(r => r.json());
      expect(detail.Text).toContain('APROVADO PELO REVISOR');
    } else expect(fixture!.messages.some(m => m.includes(body.runId))).toBe(true);
  } finally { await setup.app.close(); await rm(dir, { recursive: true, force: true }); }
}, 30_000);
test('pausing the run also gates children, and a paused workflow survives application restart', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'research-pause-'));
  const llm = new ScriptedLlm(); llm.holdExtract = true;
  let setup = await application(dir, smtpPort, llm);
  try {
    const { body } = await start(setup.app);
    await eventually(async () => llm.calls.length, count => count > 0);
    expect((await setup.app.inject({ method: 'POST', url: `/api/research-runs/${body.runId}/pause`, headers })).statusCode).toBe(202);
    llm.release();
    await eventually(() => setup.app.get(ResearchService).status(body.runId), status => status.papers.length === 1 && status.papers.every(p => p.workflow.status === 'paused'));
    await Bun.sleep(400);
    expect(llm.calls.filter(c => c.role === 'author')).toHaveLength(0);
    await setup.app.close();
    setup = await application(dir, smtpPort, llm);
    expect((await setup.app.get(ResearchService).status(body.runId)).desiredState).toBe('paused');
    expect((await setup.app.inject({ method: 'POST', url: `/api/research-runs/${body.runId}/resume`, headers })).statusCode).toBe(202);
    const result = await complete(setup.app, body.runId);
    expect(result.approved).toBe(1); expect(llm.calls.filter(c => c.role === 'extract')).toHaveLength(1);
  } finally { llm.release(); await setup.app.close(); await rm(dir, { recursive: true, force: true }); }
}, 45_000);
test('interrupt cancels parent and children and cannot be resumed as a paused run', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'research-interrupt-')); const llm = new ScriptedLlm(); llm.holdExtract = true;
  const { app } = await application(dir, smtpPort, llm);
  try {
    const { body } = await start(app);
    await eventually(async () => llm.calls.length, count => count > 0);
    expect((await app.inject({ method: 'POST', url: `/api/research-runs/${body.runId}/interrupt`, headers })).statusCode).toBe(202);
    await eventually(() => app.get(ResearchService).status(body.runId), s => s.workflow?.status === 'cancelled' && s.papers.every(p => p.workflow.status === 'cancelled'));
    expect((await app.inject({ method: 'POST', url: `/api/research-runs/${body.runId}/resume`, headers })).statusCode).toBe(409);
    expect(llm.calls.filter(c => c.role === 'author')).toHaveLength(0);
  } finally { llm.release(); await app.close(); await rm(dir, { recursive: true, force: true }); }
}, 30_000);
test('a broken PDF is visible in the digest while other children still complete', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'research-partial-'));
  const { app } = await application(dir, smtpPort, new ScriptedLlm(), ['2609.00001v1', '2609.00002v1'], ['2609.00002v1']);
  try {
    const { body } = await start(app, { arxivIds: ['2609.00001v1', '2609.00002v1'] });
    const result = await complete(app, body.runId);
    expect(result.approved).toBe(1); expect(result.failed).toBe(1);
    expect(result.papers.find(p => p.status === 'failed')?.reason).toContain('ARXIV_NOT_PDF');
  } finally { await app.close(); await rm(dir, { recursive: true, force: true }); }
}, 30_000);
test('review exhaustion is a rejected result, not approval or an infinite loop', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'research-rejected-')); const llm = new ScriptedLlm(); llm.decision = 'always-revise';
  const { app } = await application(dir, smtpPort, llm);
  try {
    const { body } = await start(app, { maxReviewRounds: 2 });
    const result = await complete(app, body.runId);
    expect(result.approved).toBe(0); expect(result.rejected).toBe(1); expect(result.papers[0]?.rounds).toBe(2);
    expect(llm.calls.filter(c => c.role === 'judge')).toHaveLength(2);
  } finally { await app.close(); await rm(dir, { recursive: true, force: true }); }
}, 30_000);
test('a missing Google key fails explicitly at submission, never synthesizes a fake summary', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'research-key-')); const { app } = await application(dir, smtpPort);
  try {
    app.get(AppSettings).env.GOOGLE_GENERATIVE_AI_API_KEY = '';
    const reply = await app.inject({ method: 'POST', url: '/api/research-runs', headers,
      payload: { requestId: crypto.randomUUID(), recipient: 'test@local.test', query: 'all:workflow' } });
    expect(reply.statusCode).toBe(503);
  } finally { await app.close(); await rm(dir, { recursive: true, force: true }); }
});
