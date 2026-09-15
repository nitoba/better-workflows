import { Module } from '@nestjs/common';
import { StorageModule } from '../storage/storage.module.js';
import { RunRepository } from './run.repository.js';
@Module({ imports: [StorageModule], providers: [RunRepository], exports: [RunRepository] })
export class RunStoreModule {}
