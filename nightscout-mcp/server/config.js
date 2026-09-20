import { readFileSync } from 'node:fs';
import { z } from 'zod';

const secret = z.string().min(32).max(512);
const schema = z.object({
  nightscout_url: z.string().url(),
  nightscout_token: z.string().min(1).max(512),
  auth_mode: z.enum(['oauth', 'bearer']).default('oauth'),
  public_url: z.string().default(''),
  owner_password: z.string().default(''),
  bearer_token: z.string().default(''),
  allowed_hosts: z.array(z.string().min(1)).default([]),
  allowed_origins: z.array(z.string().url()).default([]),
  extra_redirect_uris: z.array(z.string().url()).default([]),
  max_history_hours: z.number().int().min(1).max(744).default(168),
  max_records: z.number().int().min(1).max(5000).default(1000),
  stale_after_minutes: z.number().int().min(5).max(120).default(15),
});
export function validateConfig(input) {
  const c = schema.parse(input);
  const ns = new URL(c.nightscout_url);
  if (!['http:', 'https:'].includes(ns.protocol) || ns.username || ns.password || ns.search || ns.hash)
    throw new Error('nightscout_url must be an HTTP(S) base URL without credentials, query or fragment');
  c.nightscout_url = ns.href.replace(/\/$/, '');
  if (c.public_url) {
    const u = new URL(c.public_url);
    if (u.protocol !== 'https:' || u.username || u.password || u.search || u.hash || u.pathname !== '/')
      throw new Error('public_url must be an HTTPS origin without path, credentials, query or fragment');
    c.public_url = u.origin;
    c.allowed_hosts.push(u.host);
  }
  if (c.auth_mode === 'oauth') {
    if (!c.public_url) throw new Error('OAuth requires public_url');
    secret.parse(c.owner_password);
  } else secret.parse(c.bearer_token);
  if (c.owner_password === c.nightscout_token || c.bearer_token === c.nightscout_token)
    throw new Error('MCP and Nightscout credentials must differ');
  if (!c.allowed_hosts.length) throw new Error('Configure allowed_hosts or public_url');
  for (const h of c.allowed_hosts) {
    if (!/^[a-zA-Z0-9.:[\]-]+$/.test(h)) throw new Error('allowed_hosts must contain exact hosts, optionally with ports');
  }
  for (const uri of c.extra_redirect_uris) {
    const u = new URL(uri);
    if (u.username || u.password || u.hash || (u.protocol !== 'https:' && !(u.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(u.hostname))))
      throw new Error('Redirects require HTTPS except loopback test clients');
  }
  return c;
}
export function loadConfig() {
  return validateConfig(JSON.parse(readFileSync(process.env.OPTIONS_PATH || '/data/options.json', 'utf8')));
}
