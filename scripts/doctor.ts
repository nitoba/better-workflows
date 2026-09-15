import 'reflect-metadata';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, mkdtemp, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { AppSettings } from '../src/config/settings.js';
import { MailService } from '../src/delivery/mail.service.js';
const settings = new AppSettings();
const exec = promisify(execFile);
let failed = false;
async function check(name: string, work: () => Promise<void>): Promise<void> {
  try { await work(); console.log(`OK   ${name}`); }
  catch (error) { failed = true; console.error(`FAIL ${name}: ${error instanceof Error ? error.message : 'check failed'}`); }
}
await check('GitHub dependency entry points and declarations', async () => {
  const { WorkflowsModule } = await import('better-workflows');
  if (!WorkflowsModule) throw new Error('WorkflowsModule missing');
  await readFile(join(settings.projectDir, 'node_modules/better-workflows/dist/index.d.mts'));
});
await check('Native LiteParse produces Markdown using Node', async () => {
  await mkdir(settings.dataDir, { recursive: true });
  const dir = await mkdtemp(join(settings.dataDir, 'doctor-'));
  try {
    const output = join(dir, 'parsed.json');
    await exec(settings.env.NODE_BINARY, [join(settings.projectDir, 'scripts/liteparse-worker.mjs'),
      join(settings.projectDir, 'tests/fixtures/sample-paper.pdf'), output, '10', 'false'], { timeout: 15_000 });
    const parsed = JSON.parse(await readFile(output, 'utf8'));
    if (!parsed.markdown.includes('Synthetic Research Fixture')) throw new Error('Unexpected Markdown result');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
await check('Mailpit SMTP connection', () => new MailService(settings).verify());
for (const model of new Set([settings.env.ANALYSIS_MODEL, settings.env.JUDGE_MODEL])) {
  await check(`Google model metadata: ${model}`, async () => {
    if (!settings.env.GOOGLE_GENERATIVE_AI_API_KEY) throw new Error('GOOGLE_GENERATIVE_AI_API_KEY is empty');
    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}`, {
      headers: { 'x-goog-api-key': settings.env.GOOGLE_GENERATIVE_AI_API_KEY }, signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) throw new Error(`Google metadata returned HTTP ${response.status}. Check the key and model access.`);
    const metadata = await response.json();
    if (!metadata.supportedGenerationMethods?.includes('generateContent')) throw new Error('Model does not report generateContent support');
  });
}
console.log('Doctor does not generate text or certify LLM quality. Use smoke:live for a real, billable end-to-end run.');
process.exitCode = failed ? 1 : 0;
