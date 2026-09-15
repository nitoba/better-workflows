import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { StandardSchemaValidationPipe } from '@nestjs/common';
import { FastifyAdapter } from '@nestjs/platform-fastify';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import { AppModule } from './app.module.js';
import { WorkflowExceptionFilter } from './http/workflow-exception.filter.js';
export function configureHttp(app: NestFastifyApplication): void {
  app.setGlobalPrefix('api');
  app.useGlobalPipes(new StandardSchemaValidationPipe());
  app.useGlobalFilters(new WorkflowExceptionFilter());
  app.enableShutdownHooks();
}
export async function createApplication(): Promise<NestFastifyApplication> {
  const app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter({ bodyLimit: 16_384 }), {
    abortOnError: false, logger: ['log', 'warn', 'error'],
  });
  configureHttp(app);
  return app;
}
