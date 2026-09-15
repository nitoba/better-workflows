import { AppSettings } from '../src/config/settings.js';
if (!process.argv.includes('--confirm-llm-usage')) {
  throw new Error('This run calls the real Google API and can incur charges. Add --confirm-llm-usage to proceed.');
}
const settings = new AppSettings();
const base = process.env.API_URL ?? `http://127.0.0.1:${settings.env.PORT}`;
const runId = process.env.SMOKE_RUN_ID ?? crypto.randomUUID();
const headers = { authorization: `Bearer ${settings.env.API_TOKEN}`, 'content-type': 'application/json' };
const response = await fetch(`${base}/api/research-runs`, { method: 'POST', headers, body: JSON.stringify({
  requestId: runId, recipient: process.env.SMOKE_RECIPIENT ?? 'reader@local.test',
  arxivIds: [process.env.SMOKE_ARXIV_ID ?? '1706.03762'], maxResults: 1, maxReviewRounds: 2, language: 'pt-BR',
}) });
if (!response.ok) throw new Error(`Submission failed: HTTP ${response.status} ${await response.text()}`);
console.log(await response.json());
const deadline = Date.now() + 30 * 60_000;
while (Date.now() < deadline) {
  const statusResponse = await fetch(`${base}/api/research-runs/${runId}/result`, { headers });
  if (!statusResponse.ok) throw new Error(`Polling failed: HTTP ${statusResponse.status}`);
  const result = await statusResponse.json();
  if (result.ready) { console.log(JSON.stringify(result, null, 2)); process.exit(0); }
  if (['failed', 'cancelled'].includes(result.status)) throw new Error(JSON.stringify(result));
  console.log(`Run ${runId}: ${result.status}`);
  await Bun.sleep(2000);
}
throw new Error(`Stopped waiting for ${runId}. The workflow continues; inspect the HTTP status endpoint.`);
