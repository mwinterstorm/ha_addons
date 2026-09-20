import { loadConfig } from './config.js';
import { createApp } from './app.js';
try {
  const config = loadConfig();
  const { app } = createApp(config, { dataDir: process.env.DATA_DIR || '/data' });
  const server = app.listen(8099, '0.0.0.0', () => console.log('Nightscout read-only MCP listening on port 8099'));
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 5000).unref();
  });
} catch {
  console.error('Startup failed. Check the documented required options, HTTPS URL, independent 32-character secrets, and data directory permissions. Configuration values are not logged.');
  process.exit(1);
}
