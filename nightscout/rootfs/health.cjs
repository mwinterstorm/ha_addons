'use strict';
const fs = require('node:fs');
const http = require('node:http');
const crypto = require('node:crypto');
try {
  const secret = fs.readFileSync('/run/nightscout/api-secret', 'utf8');
  const req = http.get('http://127.0.0.1:1337/api/v1/entries.json?count=1', {
    headers: { 'api-secret': crypto.createHash('sha1').update(secret).digest('hex') },
    timeout: 8000,
  }, res => { res.resume(); process.exit(res.statusCode === 200 ? 0 : 1); });
  req.on('timeout', () => { req.destroy(); process.exit(1); });
  req.on('error', () => process.exit(1));
} catch { process.exit(1); }
