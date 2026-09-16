'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { environment } = require('../nightscout/rootfs/options.cjs');
const valid = () => ({ api_secret: 'test-secret-123456789', mongo_host: 'local-nightscout-mongodb',
  mongo_password: 'a:b@c/d?e#f%g+long', display_units: 'mmol/L', auth_default_roles: 'status-only', extra_env: [] });
test('credentials are encoded, private by default, and API secret is file based', () => {
  const o = valid(); const e = environment(o);
  const u = new URL(e.MONGO_CONNECTION);
  assert.equal(decodeURIComponent(u.password), o.mongo_password);
  assert.equal(u.hostname, o.mongo_host);
  assert.equal(u.searchParams.get('authSource'), 'nightscout');
  assert.equal(e.AUTH_DEFAULT_ROLES, 'status-only');
  assert.equal(e.API_SECRET, undefined);
  assert.equal(e.API_SECRET_FILE, '/run/nightscout/api-secret');
});
test('rejects short secrets without printing their values', () => {
  assert.throws(() => environment({ ...valid(), api_secret: 'short' }), /at least 12/);
  assert.throws(() => environment({ ...valid(), mongo_password: 'short' }), /at least 16/);
});
test('external SRV URI bypasses companion credentials', () => {
  const e = environment({ ...valid(), mongo_host: '', mongo_password: '', mongo_uri: 'mongodb+srv://u:p@example.com/nightscout' });
  assert.equal(e.MONGO_CONNECTION, 'mongodb+srv://u:p@example.com/nightscout');
});
test('prevents environment overrides and shell-like host input', () => {
  for (const name of ['NODE_OPTIONS', 'API_SECRET', 'MONGODB_URI', 'PORT', 'API_SECRET_FILE', 'SSL_KEY'])
    assert.throws(() => environment({ ...valid(), extra_env: [{ name, value: 'bad' }] }), /reserved/);
  assert.throws(() => environment({ ...valid(), mongo_host: 'host;touch /tmp/x' }), /mongo_host/);
});
test('passes ordinary plugin options literally and rejects duplicate names', () => {
  const extra_env = [{ name: 'CUSTOM_TITLE', value: 'literal $(no execution)' }];
  assert.equal(environment({ ...valid(), extra_env }).CUSTOM_TITLE, extra_env[0].value);
  assert.throws(() => environment({ ...valid(), extra_env: [...extra_env, ...extra_env] }), /reserved/);
});
