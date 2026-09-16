'use strict';
const fs = require('node:fs');
const { spawn } = require('node:child_process');
const { environment } = require('./options.cjs');
let child;
let stopping = false;
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => {
  stopping = true;
  if (child) child.kill(signal);
  else process.exit(0);
});
async function main() {
  let options;
  try { options = JSON.parse(fs.readFileSync('/data/options.json', 'utf8')); }
  catch { throw new Error('Cannot read /data/options.json'); }
  const env = environment(options);
  fs.mkdirSync('/run/nightscout', { recursive: true, mode: 0o700 });
  fs.chownSync('/run/nightscout', 1000, 1000);
  fs.writeFileSync(env.API_SECRET_FILE, options.api_secret, { mode: 0o600 });
  fs.chownSync(env.API_SECRET_FILE, 1000, 1000);
  // Do not pass Supervisor tokens or arbitrary inherited environment to upstream.
  const cleanEnv = { PATH: process.env.PATH, HOME: '/home/node', TZ: process.env.TZ || 'Pacific/Auckland', ...env };
  process.setgroups([]);
  process.setgid(1000);
  process.setuid(1000);
  const { MongoClient } = require('/opt/app/node_modules/mongodb');
  for (let attempt = 0; attempt < 60; attempt++) {
    const client = new MongoClient(env.MONGO_CONNECTION, { serverSelectionTimeoutMS: 2000 });
    try {
      await client.connect();
      await client.db().command({ ping: 1 });
      await client.close();
      break;
    } catch {
      await client.close().catch(() => {});
      if (attempt === 59) throw new Error('Database unavailable: check host, credentials and MongoDB logs');
      if (attempt % 10 === 0) console.log('Waiting for MongoDB (connection details withheld)…');
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  if (stopping) return;
  child = spawn(process.execPath, ['lib/server/server.js'], { cwd: '/opt/app', env: cleanEnv, stdio: 'inherit' });
  child.on('error', () => { console.error('Could not launch Nightscout'); process.exit(1); });
  child.on('exit', (code, signal) => process.exit(stopping ? 0 : (code ?? (signal ? 1 : 0))));
}
main().catch(error => { console.error(error.message); process.exit(1); });
