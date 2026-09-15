import { Catch } from '@nestjs/common';
import type { ArgumentsHost, ExceptionFilter } from '@nestjs/common';
import type { FastifyReply } from 'fastify';
import { WorkflowError } from 'better-workflows';
@Catch(WorkflowError)
export class WorkflowExceptionFilter implements ExceptionFilter<WorkflowError> {
  catch(error: WorkflowError, host: ArgumentsHost): void {
    const code = error.code;
    const status = code.includes('NOT_FOUND') ? 404 :
      code.includes('CONFLICT') || code.includes('TERMINAL') || code.includes('CANCELLED') ? 409 :
      code.includes('NOT_READY') ? 503 : code.includes('INVALID') ? 400 : 500;
    host.switchToHttp().getResponse<FastifyReply>().status(status).send({ statusCode: status, code,
      message: status === 500 ? 'Workflow operation failed; inspect application logs and history' : error.message });
  }
}
