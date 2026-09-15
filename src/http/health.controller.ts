import { Controller, Get } from '@nestjs/common';
@Controller('health')
export class HealthController {
  @Get() live() { return { status: 'ok', application: 'arxiv-research-workflows' }; }
}
