import { Module } from '@nestjs/common';
import { AppDatabase } from './database.js';
import { ArtifactsService } from './artifacts.service.js';
@Module({ providers: [AppDatabase, ArtifactsService], exports: [AppDatabase, ArtifactsService] })
export class StorageModule {}
