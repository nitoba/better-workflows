import { readFile, writeFile } from 'node:fs/promises';
import { LiteParse } from '@llamaindex/liteparse';

const [inputPath, outputPath, maxPagesText, ocrText] = process.argv.slice(2);
if (!inputPath || !outputPath || !maxPagesText) throw new Error('Missing parser worker arguments');
const maxPages = Number(maxPagesText);
const parser = new LiteParse({
  outputFormat: 'markdown', imageMode: 'placeholder', extractLinks: true,
  ocrEnabled: ocrText === 'true', maxPages: maxPages + 1,
  quiet: true, continueOnPageError: false,
});
try {
  const result = await parser.parse(await readFile(inputPath));
  if (result.totalPages > maxPages) throw new Error(`PAGE_LIMIT: document has ${result.totalPages} pages; configured limit is ${maxPages}`);
  if (result.pages.length !== result.totalPages || (result.pageErrors?.length ?? 0) > 0) throw new Error('PARTIAL_EXTRACTION: parser did not return every page');
  if (!result.text || result.text.trim().length < 100) throw new Error('INSUFFICIENT_TEXT: PDF may require OCR');
  const emptyPages = result.pages.filter(page => page.textItems.length === 0).map(page => page.pageNum);
  const warnings = ['Markdown extraction does not interpret figure/image contents; do not infer unobserved visual results.'];
  if (emptyPages.length) warnings.push(`Pages without text items: ${emptyPages.join(', ')}`);
  await writeFile(outputPath, JSON.stringify({ markdown: result.text, totalPages: result.totalPages, warnings }), { mode: 0o600 });
} finally { parser.close(); }
