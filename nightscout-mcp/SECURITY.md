# Security boundaries

Single owner, one Nightscout instance. The public HTTPS origin terminates TLS at
a trusted reverse proxy. Backend HTTP is confined to a trusted private network.
No anonymous MCP mode; OAuth is default and startup fails with missing secrets.

OAuth uses the official MCP SDK's authorization-code/PKCE-S256 flow and DCR.
Consent requires the owner's random 32+ character secret and a five-minute
browser-bound secure HttpOnly SameSite cookie. Origin is checked. Authorization
codes last one minute, are client/resource/redirect-bound and single-use. Access
tokens last one hour; refresh tokens rotate, expire after seven days, and replay
revokes the token family. Only hashes of these opaque tokens are held in RAM.
Restarting revokes all grants. Client registrations are atomically stored with
0600 permissions in `/data`, limited to 100 clients, with ChatGPT-only redirects
unless explicitly configured. Persistent client secrets have no automatic
expiry; reset registrations to revoke those. Tokens are never forwarded upstream.

MCP authorization runs before transport/tool execution. Host and Origin checks,
bounded JSON bodies, global request/login throttles, SDK OAuth throttles, response
size limits and ten-second upstream timeouts constrain abuse. Global throttles
avoid trusting forwarded IP headers but mean public abuse can temporarily deny
service to the owner. This small personal server is not a multi-user identity
provider and has not had an independent security audit.

Nightscout requests use one configured base URL, five fixed read endpoints and
GET only, with redirects disabled. No tool accepts URLs, paths, arbitrary filters,
headers, database queries or write methods. Use a dedicated Nightscout `readable`
token as defense in depth: the add-on cannot determine a token's permissions.
Only selected output fields are exposed; free-text treatment notes, serial
numbers, Nightscout settings and dosing suggestions are omitted. Profile names
and other source text remain untrusted data.

Secrets/health data are not logged or saved. Avoid enabling request-body, query,
or Authorization-header logs on any external proxy. Home Assistant options and
backups still contain the owner's configured secrets. No Supervisor API, Docker
socket, host networking, privileged mode, database access, or host mounts are used.
