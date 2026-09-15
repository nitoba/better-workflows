import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { WorkflowsModule, Workflow, WorkflowClient, getWorkflowToken } from 'better-workflows';
import { sqlite } from 'better-workflows/sqlite';
import { z } from 'zod';
class Echo { async run(value: number): Promise<number> { return value + 1; } }
Workflow({name:'ci.echo',version:1,input:z.number(),output:z.number()})(Echo);
class Root {}
Module({ imports: [WorkflowsModule.forRoot({namespace:'github-dependency-check',storage:sqlite({filename:':memory:'})}), WorkflowsModule.forFeature({name:'ci',workflows:[Echo]})] })(Root);
const app = await NestFactory.createApplicationContext(Root,{logger:false});
try {
 const client = app.get<WorkflowClient<typeof Echo>>(getWorkflowToken(Echo));
 const run = await client.start(3);
 const value: number = await run.result({timeout:'10s'});
 if(value!==4)throw new Error('Wrong workflow result');
 console.log('Fresh GitHub dependency: Nest DI, actual SQLite workflow execution and typed result passed');
} finally {await app.close();}
