import express from 'express';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { mcpAuthRouter } from '@modelcontextprotocol/sdk/server/auth/router.js';
import { OwnerOAuth, SCOPE, equalSecret } from './auth.js';
import { Nightscout } from './nightscout.js';
import { createMcp } from './tools.js';

// Deliberately global: do not trust X-Forwarded-For from a public requester.
function budget(max, windowMs) {
  let count = 0, reset = 0;
  return (_req, res, next) => {
    if (Date.now() >= reset) { count = 0; reset = Date.now() + windowMs; }
    if (++count > max) return res.status(429).set('Retry-After', String(Math.ceil((reset - Date.now()) / 1000))).json({ error: 'Rate limit exceeded' });
    next();
  };
}
export function createApp(config, { dataDir, fetcher } = {}) {
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', false);
  app.use((_req, res, next) => {
    res.set({
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer',
      'X-Frame-Options': 'DENY',
      'Content-Security-Policy': "default-src 'none'; form-action 'self' https://chatgpt.com; frame-ancestors 'none'; base-uri 'none'"
    });
    next();
  });
  // Health reveals no configuration, connection state, or patient data.
  app.get('/health', (_req, res) => res.json({ status: 'ok' }));

  app.use((req, res, next) => {
    if (!config.allowed_hosts.includes(req.headers.host)) {
      return res.status(403).json({ error: 'Host not allowed' });
    }

    // OAuth authorization is a top-level browser navigation. Its Origin is not
    // an authentication boundary; the OAuth provider validates the registered
    // redirect URI, client, PKCE challenge, scope, resource, and state instead.
    const isOAuthBrowserFlow =
      (req.path === '/authorize' && req.method === 'GET') ||
      (req.path === '/approve' && req.method === 'POST');

    if (isOAuthBrowserFlow) {
      return next();
    }

    const origin = req.headers.origin;
    if (
      origin &&
      origin !== config.public_url &&
      !config.allowed_origins.includes(origin)
    ) {
      console.warn(
        `Rejected origin: method=${req.method} path=${req.path} origin=${origin}`
      );
      return res.status(403).json({ error: 'Origin not allowed' });
    }

    next();
  });


  app.use(budget(600, 60000));
  // Apply bounds before the SDK's own parsers; avoid recording any request bodies.
  app.use(express.json({ limit: '32kb' }));
  app.use(express.urlencoded({ extended: false, limit: '8kb' }));
  const oauth = config.auth_mode === 'oauth' ? new OwnerOAuth(config, dataDir) : null;
  if (oauth) {
    app.post('/approve', budget(10, 600000), (req, res) => oauth.approve(req, res));
    const limiter = { keyGenerator: () => 'single-owner', validate: false };
    app.use(mcpAuthRouter({
      provider: oauth, issuerUrl: new URL(config.public_url), resourceServerUrl: new URL(oauth.resource),
      scopesSupported: [SCOPE], resourceName: 'Nightscout read-only',
      authorizationOptions: { rateLimit: limiter }, tokenOptions: { rateLimit: limiter },
      clientRegistrationOptions: { rateLimit: limiter, clientSecretExpirySeconds: 0 },
      revocationOptions: { rateLimit: limiter },
    }));
  }
  app.use('/mcp', async (req, res, next) => {
    const token = /^Bearer ([^\s]+)$/i.exec(req.headers.authorization || '')?.[1];
    try {
      if (!token) throw new Error();
      if (oauth) await oauth.verifyAccessToken(token);
      else if (!equalSecret(token, config.bearer_token)) throw new Error();
      next();
    } catch {
      const challenge = oauth ? `Bearer resource_metadata="${config.public_url}/.well-known/oauth-protected-resource/mcp", scope="${SCOPE}"` : 'Bearer realm="nightscout-mcp"';
      res.status(401).set('WWW-Authenticate', challenge).json({ error: 'Authentication required' });
    }
  });
  const ns = new Nightscout(config, fetcher);
  app.post('/mcp', async (req, res) => {
    const server = createMcp(config, ns);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    res.on('close', () => { void transport.close().catch(() => { }); void server.close().catch(() => { }); });
    try { await server.connect(transport); await transport.handleRequest(req, res, req.body); }
    catch { if (!res.headersSent) res.status(500).json({ error: 'MCP request failed' }); }
  });
  // Stateless Streamable HTTP has no standalone SSE stream or persisted sessions.
  app.all('/mcp', (_req, res) => res.status(405).set('Allow', 'POST').json({ error: 'Method not allowed' }));
  app.use((_req, res) => res.status(404).json({ error: 'Not found' }));
  app.use((error, _req, res, _next) => {
    if (!res.headersSent) res.status(error.status === 413 ? 413 : 400).json({ error: 'Invalid request' });
  });
  return { app, oauth };
}
