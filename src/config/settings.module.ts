import { Global, Module } from '@nestjs/common';
import { AppSettings } from './settings.js';
@Global()
@Module({ providers: [AppSettings], exports: [AppSettings] })
export class SettingsModule {}
