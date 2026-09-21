import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { request as httpRequest } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { createApp } from '../server/app.js';
import { validateConfig } from '../server/config.js';
import { Nightscout, glucose, windowFor } from '../server/nightscout.js';
import { OwnerOAuth } from '../server/auth.js';

const bearer = 'test-only-bearer-token-012345678901234567890';
const password = 'test-only-owner-password-01234567890123456789';
const publicUrl = 'https://mcp.example.test';
const callback = 'https://chatgpt.com/connector/oauth/test-connection';
const base = { nightscout_url: 'http://nightscout.test:1337', nightscout_token: 'test-read-only-token', auth_mode: 'bearer', bearer_token: bearer, allowed_hosts: ['127.0.0.1'], max_records: 10 };
const oauthConfig = () => validateConfig({ ...base, auth_mode: 'oauth', public_url: publicUrl, owner_password: password });
async function running(t, config, options = {}) {
  const result = createApp(config, options);
  const listener = result.app.listen(0, '127.0.0.1'); await once(listener, 'listening');
  const url = `http://127.0.0.1:${listener.address().port}`;
  config.allowed_hosts.push(new URL(url).host);
  t.after(() => new Promise(resolve => { listener.closeAllConnections(); listener.close(resolve); }));
  return { ...result, url };
}
const response = data => new Response(JSON.stringify(data), { headers: { 'content-type': 'application/json' } });
const request = async (url, path, body, headers = {}) => fetch(url + path, {
  method: 'POST', redirect: 'manual', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body),
});

test('configuration fails closed on missing auth, URL credentials, non-HTTPS OAuth, and secret reuse', () => {
  assert.throws(() => validateConfig({ ...base, bearer_token: '' }));
  assert.throws(() => validateConfig({ ...base, nightscout_url: 'http://user:secret@host' }));
  assert.throws(() => validateConfig({ ...base, auth_mode: 'oauth', public_url: 'http://mcp.example.test', owner_password: password }));
  assert.throws(() => validateConfig({ ...base, nightscout_token: bearer }));
});

test('MCP handshake, all eight read-only tools, upstream GET-only, filtering and credential isolation', async t => {
  const seen = [];
  const config = validateConfig(base);
  const now = Date.now();
  const fixtures = {
    'entries/sgv.json': [{ sgv: 108, date: now - 300000, direction: 'Flat' }],
    'treatments.json': [{ created_at: new Date(now).toISOString(), carbs: 12, notes: 'private note' }, { created_at: new Date(now).toISOString(), eventType: 'Temp Basal', absolute: 0.5 }],
    'devicestatus.json': [{ created_at: new Date(now).toISOString(), pump: { reservoir: 100, serial: 'private' }, loop: { iob: 1.2, recommendedBolus: 42 }, openaps: { suggested: { insulinReq: 42 } } }],
    'profile.json': [{ defaultProfile: 'Default', secret: 'private', store: { Default: { units: 'mmol', basal: [{ value: 0.5, time: '00:00' }] } } }],
    'status.json': { status: 'ok', version: '15.0.8', settings: { units: 'mmol', api_secret: 'private' } },
  };
  const { url } = await running(t, config, { fetcher: async (u, init) => { seen.push([u, init]); return response(fixtures[u.pathname.replace('/api/v1/', '')]); } });
  const client = new Client({ name: 'test', version: '1' });
  await client.connect(new StreamableHTTPClientTransport(new URL(url + '/mcp'), { requestInit: { headers: { Authorization: `Bearer ${bearer}` } } }));
  t.after(() => client.close());
  const { tools } = await client.listTools();
  assert.equal(tools.length, 8);
  for (const tool of tools) {
    assert.equal(tool.annotations.readOnlyHint, true); assert.equal(tool.annotations.destructiveHint, false);
    const result = await client.callTool({ name: tool.name, arguments: {} });
    assert.equal(result.isError, undefined, JSON.stringify(result));
    assert.ok(!JSON.stringify(result).includes('private'));
    assert.ok(!JSON.stringify(result).includes('recommendedBolus'));
  }
  assert.equal(seen.length, 8);
  for (const [u, init] of seen) {
    assert.equal(init.method, 'GET'); assert.equal(init.redirect, 'error');
    assert.equal(init.headers['api-secret'], base.nightscout_token);
    assert.equal(u.searchParams.has('token'), false); assert.equal(init.headers.Authorization, undefined);
  }
  const before = seen.length;
  assert.equal((await client.callTool({ name: 'get_glucose_history', arguments: { limit: 100000 } })).isError, true);
  assert.equal((await client.callTool({ name: 'get_glucose_history', arguments: { start: '2020-01-01T00:00:00Z', end: '2026-01-01T00:00:00Z' } })).isError, true);
  assert.equal((await client.callTool({ name: 'dose_insulin', arguments: {} })).isError, true);
  assert.equal(seen.length, before);
});

test('unauthenticated calls, hostile Host/Origin, query credentials and write verbs cannot reach Nightscout', async t => {
  let calls = 0;
  const { url } = await running(t, validateConfig(base), { fetcher: () => { calls++; throw new Error(); } });
  for (const path of ['/mcp', `/mcp?token=${bearer}`]) assert.equal((await request(url, path, {})).status, 401);
  assert.equal((await request(url, '/mcp', {}, { Authorization: `Bearer ${bearer}`, Origin: 'https://evil.test' })).status, 403);
  assert.equal(await new Promise(resolve => { const r = httpRequest(url + '/mcp', { method: 'POST', headers: { Host: 'evil.test' } }, res => { res.resume(); resolve(res.statusCode); }); r.end(); }), 403);
  assert.equal((await fetch(url + '/mcp', { method: 'DELETE', headers: { Authorization: `Bearer ${bearer}` } })).status, 405);
  assert.equal(calls, 0);
});

test('freshness, invalid sensor values, offset windows, and safe upstream errors', async () => {
  const config = validateConfig(base), now = Date.now();
  assert.equal(glucose({ sgv: 108, date: now - 20 * 60000 }, config, now).stale, true);
  assert.equal(glucose({ sgv: 108, date: now }, config, now).glucose_mmol_l, 6);
  assert.equal(glucose({ sgv: 5, date: now }, config, now).valid, false);
  assert.equal(glucose({ sgv: 108, date: now + 3600000 }, config, now).valid, false);
  assert.equal(windowFor({ start: '2026-01-01T12:00:00+13:00', end: '2026-01-01T13:00:00+13:00' }, config).start, '2025-12-31T23:00:00.000Z');
  for (const fetcher of [async () => new Response('secret', { status: 401 }), async () => new Response('secret'), async () => response({ invalid: true }), async () => { throw new Error('private credential'); }]) {
    await assert.rejects(new Nightscout(config, fetcher).get('treatments.json'), error => !/secret|credential/.test(error.message));
  }
  await assert.rejects(new Nightscout(config).get('../write'));
});

test('history reports truncation; no data is null, not zero', async t => {
  const config = validateConfig(base);
  const { url } = await running(t, config, { fetcher: async u => response(u.pathname.includes('entries') ? [] : Array.from({ length: 4 }, () => ({ carbs: 12 }))) });
  const client = new Client({ name: 'test', version: '1' });
  await client.connect(new StreamableHTTPClientTransport(new URL(url + '/mcp'), { requestInit: { headers: { Authorization: `Bearer ${bearer}` } } }));
  t.after(() => client.close());
  const history = await client.callTool({ name: 'get_carbs_history', arguments: { limit: 2 } });
  assert.equal(history.structuredContent.truncated, true); assert.equal(history.structuredContent.records.length, 2);
  assert.equal((await client.callTool({ name: 'get_current_glucose', arguments: {} })).structuredContent.reading, null);
});

async function register(url, redirects = [callback]) {
  return request(url, '/register', { client_name: 'ChatGPT test', redirect_uris: redirects, token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] });
}
async function consent(url, client, overrides = {}) {
  const verifier = randomBytes(32).toString('base64url');
  const challenge = createHash('sha256').update(verifier).digest('base64url');
  const query = new URLSearchParams({ client_id: client.client_id, redirect_uri: callback, response_type: 'code', state: 'test-state', scope: 'nightscout:read', code_challenge: challenge, code_challenge_method: 'S256', resource: publicUrl + '/mcp', ...overrides });
  const auth = await fetch(url + '/authorize?' + query, { redirect: 'manual' });
  const html = await auth.text();
  return { verifier, auth, html, id: /name="request_id" value="([^"]+)"/.exec(html)?.[1], cookie: auth.headers.get('set-cookie')?.split(';')[0] };
}
async function approve(url, flow, options = {}) {
  return request(url, '/approve', { request_id: flow.id, decision: 'allow', password, ...options.body }, { Origin: publicUrl, Cookie: flow.cookie, ...options.headers });
}
async function token(url, body) {
  return fetch(url + '/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(body) });
}

test('OAuth discovery, DCR, owner consent, PKCE, resource binding, refresh rotation and revocation end-to-end', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'ns-mcp-auth-')); t.after(() => rmSync(dir, { recursive: true, force: true }));
  const config = oauthConfig();
  const { url, oauth } = await running(t, config, { dataDir: dir });
  const unauth = await request(url, '/mcp', {}); assert.equal(unauth.status, 401);
  assert.match(unauth.headers.get('www-authenticate'), /oauth-protected-resource\/mcp/);
  const resource = await (await fetch(url + '/.well-known/oauth-protected-resource/mcp')).json();
  assert.equal(resource.resource, publicUrl + '/mcp');
  const metadata = await (await fetch(url + '/.well-known/oauth-authorization-server')).json();
  assert.deepEqual(metadata.code_challenge_methods_supported, ['S256']);
  assert.equal((await register(url, ['https://evil.test/callback'])).status, 400);
  const reg = await register(url); assert.equal(reg.status, 201); const client = await reg.json();
  assert.equal(statSync(join(dir, 'oauth-clients.json')).mode & 0o777, 0o600);
  assert.ok(new OwnerOAuth(config, dir).clientsStore.getClient(client.client_id));
  const flow = await consent(url, client); assert.equal(flow.auth.status, 200);
  assert.match(flow.auth.headers.get('set-cookie'), /Secure/); assert.match(flow.auth.headers.get('set-cookie'), /HttpOnly/);
  assert.match(flow.auth.headers.get('content-security-policy'), /form-action 'self' https:\/\/chatgpt\.com/);
  const approval = await approve(url, flow, { headers: { Origin: 'null' } }); assert.equal(approval.status, 303);
  const location = new URL(approval.headers.get('location'));
  assert.equal(location.searchParams.get('state'), 'test-state');
  const code = location.searchParams.get('code');
  const args = { grant_type: 'authorization_code', client_id: client.client_id, code, code_verifier: flow.verifier, redirect_uri: callback, resource: publicUrl + '/mcp' };
  assert.equal((await token(url, { ...args, code_verifier: 'wrong' })).status, 400);
  assert.equal((await token(url, { ...args, resource: 'https://evil.test/mcp' })).status, 400);
  assert.equal((await token(url, { ...args, redirect_uri: 'https://evil.test' })).status, 400);
  const issued = await token(url, args); assert.equal(issued.status, 200); const tokens = await issued.json();
  assert.equal((await token(url, args)).status, 400);
  assert.equal((await oauth.verifyAccessToken(tokens.access_token)).resource.href, publicUrl + '/mcp');
  const listed = await request(url, '/mcp', { jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }, { Authorization: `Bearer ${tokens.access_token}`, Accept: 'application/json, text/event-stream' });
  assert.equal(listed.status, 200);
  assert.deepEqual((await listed.json()).result.tools[0]._meta.securitySchemes, [{ type: 'oauth2', scopes: ['nightscout:read'] }]);
  const refreshed = await token(url, { grant_type: 'refresh_token', client_id: client.client_id, refresh_token: tokens.refresh_token, resource: publicUrl + '/mcp' });
  assert.equal(refreshed.status, 200); const newer = await refreshed.json();
  assert.equal((await token(url, { grant_type: 'refresh_token', client_id: client.client_id, refresh_token: tokens.refresh_token })).status, 400);
  await assert.rejects(oauth.verifyAccessToken(newer.access_token));
  const second = await consent(url, client), accepted = await approve(url, second);
  const secondCode = new URL(accepted.headers.get('location')).searchParams.get('code');
  const secondTokens = await (await token(url, { ...args, code: secondCode, code_verifier: second.verifier })).json();
  await oauth.revokeToken(client, { token: secondTokens.access_token });
  await assert.rejects(oauth.verifyAccessToken(secondTokens.access_token));
});

test('OAuth browser flow allows GET /authorize and Origin null approval only with its matching secure pending login', async t => {
  const { url, oauth } = await running(t, oauthConfig());
  const client = await (await register(url)).json();
  const first = await consent(url, client); assert.equal(first.auth.status, 200);
  assert.equal((await approve(url, first, { headers: { Origin: 'null', Cookie: '' } })).status, 400);
  assert.equal((await approve(url, first, { headers: { Origin: 'null' } })).status, 303);
  const second = await consent(url, client), third = await consent(url, client);
  assert.equal((await approve(url, second, { headers: { Origin: 'null', Cookie: third.cookie } })).status, 400);
  assert.equal((await approve(url, second, { headers: { Origin: 'null' } })).status, 303);
  assert.equal((await approve(url, third, { headers: { Origin: 'null' }, body: { request_id: 'missing' } })).status, 400);
  assert.equal((await approve(url, third, { headers: { Origin: 'null' } })).status, 303);
  await assert.rejects(oauth.verifyAccessToken('missing'));
});

test('OAuth rejects wrong owner secret, CSRF, foreign client codes and expanded scopes', async t => {
  const { url, oauth } = await running(t, oauthConfig());
  const client = await (await register(url)).json();
  const flow = await consent(url, client);
  assert.equal((await approve(url, flow, { body: { password: 'wrong' } })).status, 403);
  assert.equal((await approve(url, flow)).status, 400);
  const scope = await consent(url, client, { scope: 'nightscout:write' }); assert.equal(scope.auth.status, 302);
  const noResource = await consent(url, client, { resource: 'https://other.test' }); assert.equal(noResource.auth.status, 302);
  await assert.rejects(oauth.challengeForAuthorizationCode({ client_id: 'other' }, 'missing'));
  await assert.rejects(oauth.verifyAccessToken('missing'));
});

test('expired access, code and refresh tokens fail closed; cross-client grants and scope escalation fail', async () => {
  const oauth = new OwnerOAuth(oauthConfig());
  const client = { client_id: 'owner-client' };
  const tokens = oauth.issue(client.client_id, 'family');
  await assert.rejects(oauth.exchangeRefreshToken({ client_id: 'other' }, tokens.refresh_token));
  await assert.rejects(oauth.exchangeRefreshToken(client, tokens.refresh_token, ['nightscout:write']));
  await assert.rejects(oauth.exchangeRefreshToken(client, tokens.refresh_token, undefined, new URL('https://wrong.test')));
  for (const record of oauth.access.values()) record.expires = Date.now() - 1;
  await assert.rejects(oauth.verifyAccessToken(tokens.access_token));
  for (const record of oauth.refresh.values()) record.expires = Date.now() - 1;
  await assert.rejects(oauth.exchangeRefreshToken(client, tokens.refresh_token));
  const hash = value => createHash('sha256').update(value).digest('hex');
  oauth.codes.set(hash('test-code'), { clientId: client.client_id, expires: Date.now() + 60000, params: { codeChallenge: 'challenge' } });
  await assert.rejects(oauth.challengeForAuthorizationCode({ client_id: 'other' }, 'test-code'));
  oauth.codes.get(hash('test-code')).expires = Date.now() - 1;
  await assert.rejects(oauth.challengeForAuthorizationCode(client, 'test-code'));
});

test('oversized upstream and request bodies are rejected without returning their contents', async t => {
  const ns = new Nightscout(validateConfig(base), async () => response([{ notes: 'x'.repeat(4 * 1024 * 1024) }]));
  await assert.rejects(ns.get('treatments.json'), /too large/);
  const { url } = await running(t, validateConfig(base));
  const oversized = await request(url, '/mcp', { value: 'x'.repeat(33 * 1024) }, { Authorization: `Bearer ${bearer}` });
  assert.equal(oversized.status, 413);
  assert.deepEqual(await oversized.json(), { error: 'Invalid request' });
});
