import { Injectable } from '@nestjs/common';
import nodemailer from 'nodemailer';
import type { Transporter } from 'nodemailer';
import { AppSettings } from '../config/settings.js';
import { IntegrationError } from '../common/failure.js';
export interface OutgoingMessage { to: string; subject: string; text: string; html: string; messageId: string }
@Injectable()
export class MailService {
  constructor(private readonly settings: AppSettings) {}
  private transport(): Transporter {
    return nodemailer.createTransport({
      host: this.settings.env.SMTP_HOST, port: this.settings.env.SMTP_PORT,
      secure: false, ignoreTLS: true, // Mailpit, deliberately local-only. Not a production SMTP configuration.
      connectionTimeout: 10_000, greetingTimeout: 10_000, socketTimeout: 30_000,
      disableFileAccess: true, disableUrlAccess: true,
    });
  }
  async send(message: OutgoingMessage, signal: AbortSignal): Promise<string> {
    signal.throwIfAborted();
    const transport = this.transport();
    const abort = () => transport.close();
    signal.addEventListener('abort', abort, { once: true });
    try {
      const info = await transport.sendMail({ ...message, from: this.settings.env.MAIL_FROM });
      signal.throwIfAborted();
      if (!info.accepted?.length || info.rejected?.length) {
        throw new IntegrationError('SMTP_REJECTED', 'SMTP did not accept the recipient');
      }
      return String(info.messageId);
    } catch (error) {
      signal.throwIfAborted();
      if (error instanceof IntegrationError) throw error;
      throw new IntegrationError('SMTP_UNAVAILABLE', 'Could not deliver to local Mailpit SMTP', true);
    } finally { signal.removeEventListener('abort', abort); transport.close(); }
  }
  async verify(): Promise<void> { const transport = this.transport(); try { await transport.verify(); } finally { transport.close(); } }
}
