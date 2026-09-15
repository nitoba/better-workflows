import { XMLParser, XMLValidator } from 'fast-xml-parser';
import { z } from 'zod';
import { ArxivIdSchema, PaperSchema } from './arxiv.schemas.js';
import type { Paper } from './arxiv.schemas.js';
import { IntegrationError } from '../common/failure.js';

const EntrySchema = z.object({
  id: z.string(), title: z.string(), summary: z.string().optional(),
  published: z.string(), updated: z.string(),
  author: z.array(z.object({ name: z.string() })).default([]),
  category: z.array(z.object({ '@_term': z.string() })).default([]),
});
const normalize = (s: string) => s.replace(/\s+/g, ' ').trim();

export function arxivIdFromUrl(raw: string): string {
  const url = new URL(raw);
  if (!['http:', 'https:'].includes(url.protocol) || !['arxiv.org', 'export.arxiv.org'].includes(url.hostname)
    || url.username || url.password || url.port || !url.pathname.startsWith('/abs/')) {
    throw new IntegrationError('ARXIV_INVALID_ENTRY', 'Unexpected article identifier returned by arXiv');
  }
  return ArxivIdSchema.parse(decodeURIComponent(url.pathname.slice('/abs/'.length)));
}

export function parseAtom(xml: string): Paper[] {
  if (/<!DOCTYPE|<!ENTITY/i.test(xml) || XMLValidator.validate(xml) !== true) {
    throw new IntegrationError('ARXIV_INVALID_XML', 'arXiv returned invalid or unsupported XML');
  }
  const parsed: unknown = new XMLParser({
    ignoreAttributes: false, removeNSPrefix: true, parseTagValue: false,
    isArray: name => ['entry', 'author', 'link', 'category'].includes(name),
  }).parse(xml);
  const feed = z.object({ feed: z.object({ entry: z.array(z.unknown()).default([]) }) }).parse(parsed).feed;
  return feed.entry.map(value => {
    const errorEntry = z.object({ id: z.string() }).safeParse(value);
    if (errorEntry.success && errorEntry.data.id.includes('/api/errors')) {
      throw new IntegrationError('ARXIV_QUERY_ERROR', 'arXiv rejected the query or article identifier');
    }
    const entry = EntrySchema.parse(value);
    const id = arxivIdFromUrl(entry.id);
    return PaperSchema.parse({
      id, title: normalize(entry.title), abstract: normalize(entry.summary ?? ''),
      authors: entry.author.map(author => normalize(author.name)),
      categories: entry.category.map(category => category['@_term']),
      published: entry.published, updated: entry.updated,
      abstractUrl: `https://arxiv.org/abs/${id}`, pdfUrl: `https://arxiv.org/pdf/${id}`,
    });
  });
}
