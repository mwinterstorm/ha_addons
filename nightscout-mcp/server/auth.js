import { randomBytes, createHash, timingSafeEqual } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync, renameSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { InvalidClientMetadataError, InvalidGrantError, InvalidScopeError, InvalidTargetError, InvalidTokenError, TooManyRequestsError } from '@modelcontextprotocol/sdk/server/auth/errors.js';

export const SCOPE = 'nightscout:read';
const random = () => randomBytes(32).toString('base64url');
const hash = value => createHash('sha256').update(String(value)).digest();
const key = value => hash(value).toString('hex');
export const equalSecret = (a, b) => typeof a === 'string' && typeof b === 'string' && timingSafeEqual(hash(a), hash(b));
const escape = value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const validScope = scopes => !scopes?.length || scopes.every(s => s === SCOPE);

export class OwnerOAuth {
  constructor(config, dataDir) {
    this.config = config;
    this.resource = new URL('/mcp', config.public_url).href;
    this.pending = new Map(); this.codes = new Map(); this.access = new Map(); this.refresh = new Map();
    this.file = dataDir ? join(dataDir, 'oauth-clients.json') : null;
    if (dataDir) mkdirSync(dataDir, { recursive: true, mode: 0o700 });
    this.clients = new Map(this.file && existsSync(this.file) ? JSON.parse(readFileSync(this.file, 'utf8')) : []);
    this.clientsStore = {
      getClient: id => this.clients.get(id),
      registerClient: client => this.register(client),
    };
  }
  allowedRedirect(uri) {
    if (this.config.extra_redirect_uris.includes(uri)) return true;
    const u = new URL(uri);
    return u.origin === 'https://chatgpt.com' && !u.search && !u.hash && !u.username && !u.password &&
      (u.pathname === '/connector_platform_oauth_redirect' || /^\/connector\/oauth\/[A-Za-z0-9_-]+$/.test(u.pathname));
  }
  register(client) {
    if (!client.redirect_uris?.length || client.redirect_uris.length > 5 || !client.redirect_uris.every(u => this.allowedRedirect(u)))
      throw new InvalidClientMetadataError('Only ChatGPT callbacks or explicitly configured exact redirect URIs are allowed');
    if (!validScope(client.scope?.split(' ')) || client.grant_types?.some(t => !['authorization_code', 'refresh_token'].includes(t)) || client.response_types?.some(t => t !== 'code') || !['none', 'client_secret_post'].includes(client.token_endpoint_auth_method || 'client_secret_post'))
      throw new InvalidClientMetadataError('Unsupported OAuth client configuration');
    if (this.clients.size >= 100) throw new TooManyRequestsError('Client registration limit reached; ask the owner to reset registrations');
    const stored = { ...client, client_name: String(client.client_name || 'MCP client').slice(0, 100), scope: SCOPE, grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] };
    this.clients.set(stored.client_id, stored);
    if (this.file) {
      try {
        writeFileSync(`${this.file}.tmp`, JSON.stringify([...this.clients]), { mode: 0o600 });
        renameSync(`${this.file}.tmp`, this.file);
      } catch (e) { this.clients.delete(stored.client_id); throw e; }
    }
    return stored;
  }
  prune() {
    const now = Date.now();
    for (const map of [this.pending, this.codes, this.access, this.refresh])
      for (const [id, value] of map) if (value.expires <= now) map.delete(id);
  }
  requireResource(resource, optional = false) {
    if ((resource === undefined && optional) || resource?.href === this.resource) return;
    throw new InvalidTargetError('Resource must match this MCP endpoint');
  }
  async authorize(client, params, res) {
    this.prune();
    this.requireResource(params.resource);
    if (!validScope(params.scopes)) throw new InvalidScopeError('Only nightscout:read is available');
    if (!/^[A-Za-z0-9_-]{43}$/.test(params.codeChallenge)) throw new InvalidGrantError('Invalid S256 challenge');
    // Exact matching also applies to optional local test callbacks.
    if (!client.redirect_uris.includes(params.redirectUri)) throw new InvalidGrantError('Redirect mismatch');
    if (this.pending.size >= 100) throw new TooManyRequestsError('Too many pending logins');
    const id = random(), cookie = random();
    this.pending.set(id, { clientId: client.client_id, params, cookieHash: key(cookie), expires: Date.now() + 300000 });
    res.cookie('__Host-ns_mcp_login', cookie, { secure: true, httpOnly: true, sameSite: 'lax', path: '/', maxAge: 300000 });
    res.type('html').send(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Connect Nightscout</title><main><h1>Connect Nightscout</h1><p>Allow <strong>${escape(client.client_name)}</strong> to read glucose, treatment history, profiles and device status?</p><p>Requested app names are supplied by the client. Continue only if you started this connection in ChatGPT.</p><p>Return address: ${escape(params.redirectUri)}</p><p>No treatment changes or dosing actions are available.</p><form method="post" action="/approve"><input type="hidden" name="request_id" value="${id}"><label>MCP owner secret <input type="password" name="password" required maxlength="512" autocomplete="current-password"></label><p>Use the owner secret from this add-on's configuration, not your Nightscout API secret.</p><button type="submit" name="decision" value="allow">Allow read-only access</button> <button type="submit" name="decision" value="deny" formnovalidate>Cancel</button></form></main></html>`);
  }
  approve(req, res) {
    this.prune();
    const id = req.body?.request_id;
    const pending = this.pending.get(id);
    const cookie = req.headers.cookie?.split(';').map(x => x.trim()).find(x => x.startsWith('__Host-ns_mcp_login='))?.split('=')[1];
    if (req.headers.origin !== this.config.public_url || !pending || !cookie || key(cookie) !== pending.cookieHash)
      return res.status(400).send('Login expired or invalid. Restart the connection in ChatGPT.');
    this.pending.delete(id); // one attempt per consent form
    res.clearCookie('__Host-ns_mcp_login', { secure: true, httpOnly: true, sameSite: 'lax', path: '/' });
    const redirect = new URL(pending.params.redirectUri);
    if (pending.params.state) redirect.searchParams.set('state', pending.params.state);
    if (req.body.decision !== 'allow') {
      redirect.searchParams.set('error', 'access_denied');
      return res.redirect(303, redirect.href);
    }
    if (!equalSecret(req.body.password, this.config.owner_password)) return res.status(403).send('Access denied. Restart the connection to try again.');
    const code = random();
    this.codes.set(key(code), { ...pending, expires: Date.now() + 60000 });
    redirect.searchParams.set('code', code);
    return res.redirect(303, redirect.href);
  }
  codeFor(client, code) {
    this.prune();
    const record = this.codes.get(key(code));
    if (!record || record.clientId !== client.client_id) throw new InvalidGrantError('Invalid or expired code');
    return record;
  }
  async challengeForAuthorizationCode(client, code) { return this.codeFor(client, code).params.codeChallenge; }
  async exchangeAuthorizationCode(client, code, _verifier, redirectUri, resource) {
    const record = this.codeFor(client, code);
    this.requireResource(resource);
    if (redirectUri !== record.params.redirectUri) throw new InvalidGrantError('Redirect mismatch');
    this.codes.delete(key(code));
    return this.issue(client.client_id, random());
  }
  issue(clientId, family) {
    this.prune();
    if (this.access.size + this.refresh.size >= 10000) throw new TooManyRequestsError('Token limit reached');
    const access = random(), refresh = random();
    const base = { clientId, family, resource: this.resource, scopes: [SCOPE] };
    this.access.set(key(access), { ...base, expires: Date.now() + 3600000 });
    this.refresh.set(key(refresh), { ...base, expires: Date.now() + 7 * 86400000, used: false });
    return { access_token: access, token_type: 'Bearer', expires_in: 3600, refresh_token: refresh, scope: SCOPE };
  }
  async exchangeRefreshToken(client, token, scopes, resource) {
    this.prune(); this.requireResource(resource, true);
    const record = this.refresh.get(key(token));
    if (!record || record.clientId !== client.client_id) throw new InvalidGrantError('Invalid refresh token');
    if (record.used) { this.revokeFamily(record.family); throw new InvalidGrantError('Refresh token reuse; reconnect'); }
    if (!validScope(scopes)) throw new InvalidScopeError('Only nightscout:read is available');
    record.used = true;
    return this.issue(client.client_id, record.family);
  }
  async verifyAccessToken(token) {
    this.prune();
    const record = this.access.get(key(token));
    if (!record || record.resource !== this.resource || !record.scopes.includes(SCOPE)) throw new InvalidTokenError('Invalid or expired access token');
    return { token, clientId: record.clientId, scopes: record.scopes, expiresAt: Math.floor(record.expires / 1000), resource: new URL(record.resource) };
  }
  revokeFamily(family) {
    for (const map of [this.access, this.refresh]) for (const [id, value] of map) if (value.family === family) map.delete(id);
  }
  async revokeToken(client, request) {
    const record = this.access.get(key(request.token)) || this.refresh.get(key(request.token));
    if (record?.clientId === client.client_id) this.revokeFamily(record.family);
  }
}
