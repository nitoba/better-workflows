import { Injectable, UnauthorizedException } from '@nestjs/common';
import type { CanActivate, ExecutionContext } from '@nestjs/common';
import { timingSafeEqual } from 'node:crypto';
import { AppSettings } from '../config/settings.js';
@Injectable()
export class ApiTokenGuard implements CanActivate {
  constructor(private readonly settings: AppSettings) {}
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<{ headers: { authorization?: string } }>();
    const token = request.headers.authorization;
    if (!token?.startsWith('Bearer ')) throw new UnauthorizedException('Bearer token required');
    const given = Buffer.from(token.slice(7));
    const expected = Buffer.from(this.settings.env.API_TOKEN);
    if (given.length !== expected.length || !timingSafeEqual(given, expected)) throw new UnauthorizedException('Invalid bearer token');
    return true;
  }
}
