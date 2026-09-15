import { z } from 'zod';
export const ArxivIdSchema = z.string().regex(/^(?:\d{4}\.\d{4,5}|[a-z][a-z.-]*(?:\.[A-Z]{2})?\/\d{7})(?:v[1-9]\d*)?$/, 'Invalid arXiv identifier');
export const PaperSchema = z.object({
  id: ArxivIdSchema, title: z.string().min(1).max(2000),
  authors: z.array(z.string()).max(1000), abstract: z.string().max(100_000),
  published: z.string(), updated: z.string(), categories: z.array(z.string()),
  abstractUrl: z.string().url(), pdfUrl: z.string().url(),
});
export type Paper = z.infer<typeof PaperSchema>;
export const SearchRequestSchema = z.object({
  query: z.string().max(500).nullable(), arxivIds: z.array(ArxivIdSchema).max(10),
  maxResults: z.number().int().min(1).max(10),
});
export type SearchRequest = z.infer<typeof SearchRequestSchema>;
