import { expect, test } from 'bun:test';
import { parseAtom, arxivIdFromUrl } from '../../src/arxiv/atom.js';
import { assertArxivUrl, readLimited } from '../../src/arxiv/arxiv-http.service.js';
import { splitMarkdown } from '../../src/documents/chunker.js';
import { evaluateAnalysis } from '../../src/analysis/quality.js';
import { renderDigest } from '../../src/delivery/render-digest.js';
import { CreateResearchSchema } from '../../src/research/research.schemas.js';
import { atomFixture, analysisFixture, reviewFixture, QUOTE, SECOND_QUOTE } from '../helpers/fixtures.js';

test('arXiv Atom parses stable versions and rejects XML entities and remote identifiers', () => {
  const papers = parseAtom(atomFixture(['2609.00001v2', 'hep-th/9901001v1']));
  expect(papers.map(p => p.id)).toEqual(['2609.00001v2', 'hep-th/9901001v1']);
  expect(papers[0]?.authors).toEqual(['Test Author']);
  expect(() => parseAtom('<!DOCTYPE feed [<!ENTITY x SYSTEM "file:///etc/passwd">]><feed/>')).toThrow();
  expect(() => arxivIdFromUrl('https://evil.example/abs/2609.00001')).toThrow();
  expect(() => parseAtom('<feed><entry><id>http://arxiv.org/api/errors#incorrect_id_format</id></entry></feed>')).toThrow();
});
test('PDF redirects cannot leave the arXiv allowlist, and streaming sizes are bounded', async () => {
  for (const url of ['http://arxiv.org/pdf/1', 'https://arxiv.org.evil.test/pdf/1', 'https://arxiv.org@127.0.0.1/pdf/1', 'https://arxiv.org/admin']) {
    expect(() => assertArxivUrl(new URL(url))).toThrow();
  }
  assertArxivUrl(new URL('https://arxiv.org/pdf/2609.00001v1'));
  await expect(readLimited(new Response('123456'), 5)).rejects.toThrow('Download exceeds');
  expect((await readLimited(new Response('12345'), 5)).toString()).toBe('12345');
});
test('Markdown chunking is lossless and does not split surrogate pairs', () => {
  const input = `${'a'.repeat(99)}😀\n\n${'Evidence and detail. '.repeat(80)}`;
  const chunks = splitMarkdown(input, 100);
  expect(chunks.map(c => c.text).join('')).toBe(input);
  expect(chunks.every(c => c.text.length <= 102)).toBe(true);
  for (const chunk of chunks) { expect(input.slice(chunk.start, chunk.end)).toBe(chunk.text); expect(chunk.text.isWellFormed()).toBe(true); }
});
test('judge acceptance requires verified evidence, thresholds and no major issues', () => {
  const source = [{ id: 'chunk-0001', text: `${QUOTE}\n${SECOND_QUOTE}` }];
  expect(evaluateAnalysis(analysisFixture(), reviewFixture(), source).approved).toBe(true);
  expect(evaluateAnalysis(analysisFixture(), reviewFixture(), [{ id: 'chunk-0001', text: 'invented' }]).approved).toBe(false);
  expect(evaluateAnalysis(analysisFixture(), { ...reviewFixture(), fidelity: 70 }, source).approved).toBe(false);
  expect(evaluateAnalysis(analysisFixture(), { ...reviewFixture(), issues: [{ severity: 'critical', description: 'Unsupported data' }] }, source).approved).toBe(false);
  expect(evaluateAnalysis(analysisFixture(), reviewFixture('reject'), source).terminal).toBe(true);
});
test('HTTP request rejects arbitrary URLs and unbounded workloads', () => {
  const input = { requestId: crypto.randomUUID(), recipient: 'reader@local.test', arxivIds: ['2609.00001v1'] };
  expect(CreateResearchSchema.parse(input).maxReviewRounds).toBe(3);
  expect(CreateResearchSchema.safeParse({ ...input, query: 'test' }).success).toBe(false);
  expect(CreateResearchSchema.safeParse({ ...input, maxReviewRounds: 100 }).success).toBe(false);
  expect(CreateResearchSchema.safeParse({ ...input, arxivIds: ['https://localhost/secret'] }).success).toBe(false);
});
test('untrusted model text cannot inject HTML or be presented as approved when rejected', () => {
  const draft = analysisFixture(); draft.summary = '<script>alert(1)</script>';
  const { html, text } = renderDigest('test', [{ analysis: draft, outcome: {
    paperId: '2609.00001v1', title: '<img src=x onerror=alert(1)>', executionId: 'test', status: 'rejected', analysis: null, review: null, rounds: 2, reason: 'Rejected'
  } }], 'pt-BR');
  expect(html).not.toContain('<script>'); expect(html).toContain('&lt;script&gt;'); expect(text).toContain('NÃO APROVADO');
});
