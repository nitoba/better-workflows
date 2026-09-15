import { createApplication } from './bootstrap.js';
import { AppSettings } from './config/settings.js';
const app = await createApplication();
const settings = app.get(AppSettings);
await app.listen(settings.env.PORT, settings.env.HOST);
