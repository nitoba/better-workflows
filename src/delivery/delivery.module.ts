import { Module } from '@nestjs/common';
import { WorkflowsModule } from 'better-workflows';
import { StorageModule } from '../storage/storage.module.js';
import { DeliveryActivities, EmailQueue } from './delivery.activities.js';
import { MailService } from './mail.service.js';
@Module({ providers: [MailService], exports: [MailService] })
export class MailModule {}
@Module({ imports: [WorkflowsModule.forFeature({ name: 'delivery', imports: [StorageModule, MailModule],
  activities: [DeliveryActivities], queues: [{ queue: EmailQueue, concurrency: 2 }], exports: { activities: [DeliveryActivities] } })],
  exports: [WorkflowsModule] })
export class DeliveryModule {}
