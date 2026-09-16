'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require.resolve('../nightscout/rootfs/start.cjs'), 'utf8');
async function run(failures) {
  let attempts = 0;
  const events = [], handlers = {}, childHandlers = {}, messages = [];
  const child = { kill: signal => events.push(['kill', signal]), on: (key, fn) => { childHandlers[key] = fn; } };
  const options = { api_secret: 'secret-not-for-logs', mongo_host: 'db', mongo_password: 'password-not-for-logs',
    display_units: 'mmol/L', auth_default_roles: 'status-only', extra_env: [] };
  const fakeFs = {
    readFileSync: () => JSON.stringify(options),
    mkdirSync: (...args) => events.push(['mkdir', ...args]),
    chownSync: (...args) => events.push(['chown', ...args]),
    writeFileSync: (...args) => events.push(['write', ...args]),
  };
  const context = {
    require: name => {
      if (name === 'node:fs') return fakeFs;
      if (name === 'node:child_process') return { spawn: (...args) => { events.push(['spawn', ...args]); return child; } };
      if (name === './options.cjs') return require('../nightscout/rootfs/options.cjs');
      if (name === '/opt/app/node_modules/mongodb') return { MongoClient: class {
        async connect() { attempts++; if (attempts <= failures) throw new Error('password-not-for-logs'); }
        db() { return { command: async () => {} }; }
        async close() {}
      } };
      throw new Error(name);
    },
    process: { env: { PATH: '/usr/bin', TZ: 'Pacific/Auckland', SUPERVISOR_TOKEN: 'private-token' },
      execPath: '/usr/bin/node', on: (key, fn) => { handlers[key] = fn; },
      setgroups: x => events.push(['groups', x]), setgid: x => events.push(['gid', x]),
      setuid: x => events.push(['uid', x]), exit: code => events.push(['exit', code]) },
    console: { log: x => messages.push(x), error: x => messages.push(x) },
    setTimeout: fn => queueMicrotask(fn),
  };
  await vm.runInNewContext(source, context);
  return { attempts, events, handlers, childHandlers, messages };
}
test('waits for DB, drops privileges, starts upstream with clean environment, forwards shutdown', async () => {
  const r = await run(2);
  assert.equal(r.attempts, 3);
  const spawn = r.events.find(x => x[0] === 'spawn');
  assert.ok(spawn);
  assert.ok(r.events.findIndex(x => x[0] === 'uid') < r.events.indexOf(spawn));
  assert.equal(spawn[3].env.SUPERVISOR_TOKEN, undefined);
  assert.equal(spawn[3].env.API_SECRET, undefined);
  assert.equal(r.events.find(x => x[0] === 'write')[3].mode, 0o600);
  r.handlers.SIGTERM();
  assert.ok(r.events.some(x => x[0] === 'kill' && x[1] === 'SIGTERM'));
  r.childHandlers.exit(null, 'SIGTERM');
  assert.ok(r.events.some(x => x[0] === 'exit' && x[1] === 0));
  assert.ok(!r.messages.join(' ').includes('password-not-for-logs'));
});
test('exits after bounded database retries without exposing connection errors', async () => {
  const r = await run(Infinity);
  assert.equal(r.attempts, 60);
  assert.ok(!r.events.some(x => x[0] === 'spawn'));
  assert.ok(r.events.some(x => x[0] === 'exit' && x[1] === 1));
  assert.ok(!r.messages.join(' ').includes('password-not-for-logs'));
});
