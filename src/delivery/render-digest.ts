import type { PaperAnalysis } from '../analysis/analysis.schemas.js';
import type { PaperOutcome } from '../research/research.schemas.js';
export interface DigestPaper { outcome: PaperOutcome; analysis: PaperAnalysis | null }
export function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]!);
}
export function renderDigest(runId: string, papers: readonly DigestPaper[], language: string): { text: string; html: string } {
  const pt = language === 'pt-BR';
  const title = pt ? 'Resumo de pesquisa no arXiv' : 'arXiv research digest';
  const labels = pt ? { approved: 'APROVADO PELO REVISOR AUTOMÁTICO', rejected: 'NÃO APROVADO — RASCUNHO', failed: 'FALHA NO PROCESSAMENTO' }
    : { approved: 'APPROVED BY AUTOMATED REVIEW', rejected: 'NOT APPROVED — DRAFT', failed: 'PROCESSING FAILED' };
  const lines = [`# ${title}`, `Research run: ${runId}`, '',
    pt ? 'Resumos gerados por IA. Aprovação automática não substitui a leitura crítica do artigo. Figuras e fórmulas podem não ter sido extraídas corretamente.'
      : 'AI-generated summaries. Automated approval does not replace critical reading. Figures and equations may not be extracted correctly.', ''];
  if (!papers.length) lines.push(pt ? 'Nenhum artigo encontrado para a consulta.' : 'No articles matched the query.');
  for (const { outcome: o, analysis: a } of papers) {
    lines.push(`## ${o.title}`, `${labels[o.status]} | ${o.rounds} ${pt ? 'rodadas' : 'rounds'}`,
      `https://arxiv.org/abs/${o.paperId}`, o.reason, '');
    if (!a) continue;
    lines.push(a.summary, '', `### ${pt ? 'Pergunta de pesquisa' : 'Research question'}`, a.researchQuestion,
      `### ${pt ? 'Metodologia' : 'Methodology'}`, a.methodology, `### ${pt ? 'Principais pontos' : 'Key points'}`);
    for (const point of a.keyPoints) lines.push(`- ${point.title}: ${point.detail}`);
    lines.push(`### ${pt ? 'Resultados' : 'Results'}`, ...a.results.map(v => `- ${v}`),
      `### ${pt ? 'Limitações' : 'Limitations'}`, ...a.limitations.map(v => `- ${v}`),
      `### ${pt ? 'Ressalvas da extração' : 'Extraction caveats'}`, ...a.extractionCaveats.map(v => `- ${v}`), '');
  }
  const text = lines.join('\n');
  // All model-supplied content is text. Never interpolate it as HTML or remote images.
  const html = `<!doctype html><html><body><h1>${escapeHtml(title)}</h1><pre style="white-space:pre-wrap;font:15px/1.6 sans-serif">${escapeHtml(text)}</pre></body></html>`;
  return { text, html };
}
