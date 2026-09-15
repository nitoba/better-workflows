import { Inject, Injectable } from '@nestjs/common';
import { setTimeout as delay } from 'node:timers/promises';
import { AppSettings } from '../config/settings.js';
import { AppDatabase } from '../storage/database.js';
import { IntegrationError } from '../common/failure.js';
import { Mutex } from '../common/mutex.js';
export const ARXIV_FETCH = Symbol('arxiv.fetch');
export type FetchPort = (url: string, init: RequestInit) => Promise<Response>;

export function assertArxivUrl(url: URL): void {
  if (url.protocol !== 'https:' || !['arxiv.org', 'export.arxiv.org'].includes(url.hostname) ||
      url.username || url.password || url.port ||
      !(url.pathname.startsWith('/pdf/') || (url.hostname === 'export.arxiv.org' && url.pathname === '/api/query'))) {
    throw new IntegrationError('UNSAFE_ARXIV_URL', 'Refusing a non-arXiv URL or unexpected endpoint');
  }
}

export async function readLimited(response: Response, maxBytes: number): Promise<Buffer> {
  const declared = Number(response.headers.get('content-length'));
  if (declared > maxBytes) { await response.body?.cancel(); throw new IntegrationError('DOWNLOAD_TOO_LARGE', `Download exceeds ${maxBytes} bytes`); }
  if (!response.body) throw new IntegrationError('EMPTY_RESPONSE', 'Upstream response has no body', true);
  const reader = response.body.getReader();
  const buffers: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const item = await reader.read();
      if (item.done) break;
      size += item.value.byteLength;
      if (size > maxBytes) throw new IntegrationError('DOWNLOAD_TOO_LARGE', `Download exceeds ${maxBytes} bytes`);
      buffers.push(item.value);
    }
  } finally { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
  return Buffer.concat(buffers, size);
}

@Injectable()
export class ArxivHttpService {
  private readonly gate = new Mutex();
  constructor(private readonly settings: AppSettings, private readonly database: AppDatabase,
    @Inject(ARXIV_FETCH) private readonly request: FetchPort) {}

  async get(url: URL, signal: AbortSignal, limit: number): Promise<{ data: Buffer; contentType: string }> {
    return this.gate.use(async () => {
      let current = url;
      for (let redirects = 0; redirects <= 4; redirects++) {
        assertArxivUrl(current);
        signal.throwIfAborted();
        const last = this.database.db.query<{ last_at: number }, []>("SELECT last_at FROM network_gates WHERE name='arxiv'").get()?.last_at ?? 0;
        const wait = last + this.settings.arxivIntervalMs - Date.now();
        if (wait > 0) await delay(wait, undefined, { signal });
        this.mark();
        let response: Response;
        try {
          response = await this.request(current.toString(), {
            redirect: 'manual', signal: AbortSignal.any([signal, AbortSignal.timeout(90_000)]),
            headers: { 'User-Agent': `ArxivResearchWorkflows/1.0 (mailto:${this.settings.env.ARXIV_CONTACT_EMAIL})`, Accept: 'application/atom+xml, application/pdf' },
          });
          if ([301, 302, 303, 307, 308].includes(response.status)) {
            const location = response.headers.get('location');
            await response.body?.cancel();
            if (!location) throw new IntegrationError('ARXIV_REDIRECT', 'Redirect without destination');
            current = new URL(location, current);
            continue;
          }
          if (!response.ok) {
            const retryAfter = response.headers.get('retry-after');
            if (retryAfter) {
              const seconds = Number(retryAfter);
              const deadline = Number.isFinite(seconds) ? Date.now() + seconds * 1000 : Date.parse(retryAfter);
              if (Number.isFinite(deadline)) this.mark(Math.min(deadline, Date.now() + 3_600_000));
            }
            await response.body?.cancel();
            throw new IntegrationError(`ARXIV_HTTP_${response.status}`, `arXiv returned HTTP ${response.status}`, response.status === 429 || response.status >= 500);
          }
          return { data: await readLimited(response, limit), contentType: response.headers.get('content-type') ?? '' };
        } catch (error) {
          if (signal.aborted || error instanceof IntegrationError) throw error;
          throw new IntegrationError('ARXIV_NETWORK', 'arXiv request timed out or could not connect', true);
        } finally { this.mark(); }
      }
      throw new IntegrationError('ARXIV_REDIRECT_LIMIT', 'Too many redirects');
    });
  }
  private mark(time = Date.now()): void {
    this.database.db.query("INSERT INTO network_gates(name,last_at) VALUES ('arxiv',?) ON CONFLICT(name) DO UPDATE SET last_at=MAX(last_at, excluded.last_at)").run(time);
  }
}
