export interface TextChunk { id: string; index: number; start: number; end: number; text: string }
/** Lossless, deterministic chunks. Offsets refer to Markdown, not PDF page numbers. */
export function splitMarkdown(markdown: string, size: number): TextChunk[] {
  if (!Number.isInteger(size) || size < 100) throw new Error('Chunk size must be at least 100');
  const chunks: TextChunk[] = [];
  let start = 0;
  while (start < markdown.length) {
    let end = Math.min(markdown.length, start + size);
    if (end < markdown.length) {
      const paragraph = markdown.lastIndexOf('\n\n', end);
      if (paragraph > start + size / 2) end = paragraph + 2;
      const code = markdown.charCodeAt(end - 1);
      if (code >= 0xd800 && code <= 0xdbff) end--;
    }
    const index = chunks.length;
    chunks.push({ id: `chunk-${String(index + 1).padStart(4, '0')}`, index, start, end, text: markdown.slice(start, end) });
    start = end;
  }
  return chunks;
}
