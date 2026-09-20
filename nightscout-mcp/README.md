# Nightscout MCP

Read-only access to your Nightscout data from ChatGPT. Runs separately from
Nightscout and MongoDB in Home Assistant OS. No writes, dosing tools or pump controls.

Includes its own single-owner OAuth login; no Auth0 account is needed.

**Experimental:** local protocol/security tests pass. Container build, Home
Assistant installation, live Nightscout data and ChatGPT linking still need testing.

See [installation and ChatGPT setup](DOCS.md), [security details](SECURITY.md), and
[validation evidence](VALIDATION.md).
