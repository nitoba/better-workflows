import { Activities, Activity, defineQueue } from 'better-workflows';
import type { ActivityContext } from 'better-workflows';
import { z } from 'zod';
import { SearchRequestSchema, PaperSchema } from './arxiv.schemas.js';
import type { SearchRequest, Paper } from './arxiv.schemas.js';
import { parseAtom } from './atom.js';
import { ArxivHttpService } from './arxiv-http.service.js';
import { AppSettings } from '../config/settings.js';
import { ArtifactRefSchema } from '../storage/storage.schemas.js';
import { ArtifactsService } from '../storage/artifacts.service.js';
import { AppDatabase } from '../storage/database.js';
import { executeActivity, IntegrationError } from '../common/failure.js';

export const ArxivQueue = defineQueue('arxiv.downloads');
const SearchInput = SearchRequestSchema.extend({ runId: z.string().uuid() });
const DownloadInput = z.object({ runId: z.string().uuid(), paper: PaperSchema });

@Activities({ queue: ArxivQueue, timeout: '3m', retry: { maxAttempts: 3, backoff: 'exponential', initialDelay: '5s', maxDelay: '1m' } })
export class ArxivActivities {
  constructor(private readonly http: ArxivHttpService, private readonly settings: AppSettings,
    private readonly files: ArtifactsService, private readonly database: AppDatabase) {}

  @Activity({ name: 'arxiv.search', version: 1, input: SearchInput, output: z.array(PaperSchema) })
  search(input: SearchRequest & { runId: string }, ctx: ActivityContext): Promise<Paper[]> {
    return executeActivity(ctx, async () => {
      const cached = this.database.getReceipt(ctx.idempotencyKey);
      if (cached) return z.array(PaperSchema).parse(cached);
      const url = new URL('https://export.arxiv.org/api/query');
      if (input.arxivIds.length) url.searchParams.set('id_list', input.arxivIds.join(','));
      else url.searchParams.set('search_query', input.query!);
      url.searchParams.set('start', '0'); url.searchParams.set('max_results', String(input.maxResults));
      if (!input.arxivIds.length) {
        url.searchParams.set('sortBy', 'submittedDate'); url.searchParams.set('sortOrder', 'descending');
      }
      const urlKey = url.toString();
      const cachedSearch = this.database.db.query<{ papers_json: string; expires_at: number }, [string]>(
        'SELECT papers_json,expires_at FROM arxiv_search_cache WHERE cache_key=?').get(urlKey);
      let papers: Paper[];
      if (cachedSearch && cachedSearch.expires_at > Date.now()) papers = z.array(PaperSchema).parse(JSON.parse(cachedSearch.papers_json));
      else {
        const response = await this.http.get(url, ctx.signal, 2_000_000);
        papers = parseAtom(response.data.toString('utf8')).slice(0, input.maxResults);
        const expiresAt = Math.floor(Date.now() / 86_400_000) * 86_400_000 + 86_400_000;
        this.database.db.query('INSERT INTO arxiv_search_cache(cache_key,papers_json,expires_at) VALUES (?,?,?) ON CONFLICT(cache_key) DO UPDATE SET papers_json=excluded.papers_json,expires_at=excluded.expires_at')
          .run(urlKey, JSON.stringify(papers), expiresAt);
      }
      await this.files.put(input.runId, 'arxiv-search.json', JSON.stringify({ url: url.toString(), papers }, null, 2), 'application/json');
      this.database.putReceipt(ctx.idempotencyKey, papers);
      return papers;
    });
  }

  @Activity({ name: 'arxiv.download-pdf', version: 1, input: DownloadInput, output: ArtifactRefSchema })
  download(input: z.infer<typeof DownloadInput>, ctx: ActivityContext) {
    return executeActivity(ctx, async () => {
      const cached = this.database.getReceipt(ctx.idempotencyKey);
      if (cached) { const ref = ArtifactRefSchema.parse(cached); await this.files.read(ref); return ref; }
      // Never use an arbitrary caller/model supplied download URL.
      const response = await this.http.get(new URL(`https://arxiv.org/pdf/${input.paper.id}`), ctx.signal, this.settings.env.PDF_MAX_BYTES);
      const signature = response.data.subarray(0, 1024).indexOf('%PDF-');
      if (signature < 0 || response.contentType.includes('text/html')) {
        throw new IntegrationError('ARXIV_NOT_PDF', 'arXiv returned something other than a PDF');
      }
      const ref = await this.files.put(input.runId, `${input.paper.id}.pdf`, response.data, 'application/pdf');
      this.database.putReceipt(ctx.idempotencyKey, ref);
      return ref;
    });
  }
}
