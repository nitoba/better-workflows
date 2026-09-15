import { Injectable, Logger } from '@nestjs/common';
import type { OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { RunRepository } from '../runs/run.repository.js';
import { ResearchService } from './research.service.js';
@Injectable()
export class ControlReconciler implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(ControlReconciler.name);
  private timer?: ReturnType<typeof setInterval>;
  private inFlight?: Promise<void>;
  private cursor = '';
  constructor(private readonly runs: RunRepository, private readonly research: ResearchService) {}
  onApplicationBootstrap(): void {
    this.timer = setInterval(() => {
      if (!this.inFlight) this.inFlight = this.tick().finally(() => { this.inFlight = undefined; });
    }, 1000);
    this.timer.unref();
  }
  async tick(): Promise<void> {
    const rows = this.runs.active(this.cursor);
    this.cursor = rows.at(-1)?.id ?? '';
    for (const row of rows) {
      try { await this.research.reconcile(row.id); }
      catch { this.logger.warn(`Will retry dispatch/control reconciliation for ${row.id}`); }
    }
  }
  async onModuleDestroy(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.inFlight;
  }
}
