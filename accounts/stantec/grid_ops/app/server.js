// Server-side proxy for the Corridor Genie serving endpoint — ZERO npm deps.
//
// The Databricks Apps build sandbox has no egress to the public npm registry, so
// `npm install` of anything (even express) hangs and times out. This server is
// therefore written on Node's built-in http/fs only (`fetch` is a Node 18+
// global), so package.json has no dependencies and the build installs nothing.
//
// The browser never sees a token. This process authenticates as the app's own
// service principal (client-credentials injected by the Apps runtime) and
// forwards chat turns to the endpoint's /invocations API using the Responses
// schema: { input: messages, custom_inputs: { genie_conversation_id } }.
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIST = path.join(__dirname, 'dist');
const PORT = process.env.PORT || 8000;

const SERVING_ENDPOINT =
  process.env.SERVING_ENDPOINT ||
  'agents_ademianczuk_uc_1_catalog-stantec_grid_ops-corridor_genie';

// DATABRICKS_HOST is a bare hostname inside Databricks Apps; add the scheme.
function workspaceHost() {
  let host = process.env.DATABRICKS_HOST || '';
  if (host && !host.startsWith('http')) host = `https://${host}`;
  return host.replace(/\/+$/, '');
}

// --- auth --------------------------------------------------------------------
//   1. DATABRICKS_TOKEN            (local dev: PAT or `databricks auth token`)
//   2. client-credentials OAuth    (Databricks Apps: CLIENT_ID/CLIENT_SECRET)
let cachedToken = null; // { value, expiresAt }

async function getToken() {
  if (process.env.DATABRICKS_TOKEN) return process.env.DATABRICKS_TOKEN;

  const now = Date.now();
  if (cachedToken && cachedToken.expiresAt - 60_000 > now) return cachedToken.value;

  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  const host = workspaceHost();
  if (!clientId || !clientSecret || !host) {
    throw new Error(
      'No credentials: set DATABRICKS_TOKEN, or DATABRICKS_CLIENT_ID/SECRET + DATABRICKS_HOST.'
    );
  }

  const res = await fetch(`${host}/oidc/v1/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: 'Basic ' + Buffer.from(`${clientId}:${clientSecret}`).toString('base64'),
    },
    body: new URLSearchParams({ grant_type: 'client_credentials', scope: 'all-apis' }),
  });
  if (!res.ok) {
    throw new Error(`OAuth token request failed (${res.status}): ${await res.text()}`);
  }
  const data = await res.json();
  cachedToken = { value: data.access_token, expiresAt: now + (data.expires_in || 3600) * 1000 };
  return cachedToken.value;
}

// --- response parsing --------------------------------------------------------
function extractText(payload) {
  const parts = [];
  const output = payload?.output || payload?.messages || [];
  for (const item of output) {
    const content = item?.content;
    if (typeof content === 'string') parts.push(content);
    else if (Array.isArray(content)) {
      for (const c of content) if (typeof c?.text === 'string') parts.push(c.text);
    }
  }
  if (!parts.length && typeof payload?.output_text === 'string') parts.push(payload.output_text);
  if (!parts.length && payload?.choices?.[0]?.message?.content) {
    parts.push(payload.choices[0].message.content);
  }
  return parts.join('\n\n').trim();
}

// --- tiny http helpers -------------------------------------------------------
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.woff2': 'font/woff2',
  '.map': 'application/json',
};

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
}

function readBody(req, limit = 1_048_576) {
  return new Promise((resolve, reject) => {
    let data = '';
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) { reject(new Error('body too large')); req.destroy(); return; }
      data += chunk;
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

// Serve a file from dist/, guarding against path traversal. Returns true if served.
function serveStatic(res, urlPath) {
  const rel = path.normalize(decodeURIComponent(urlPath)).replace(/^(\.\.[/\\])+/, '');
  const filePath = path.join(DIST, rel);
  if (!filePath.startsWith(DIST)) return false;
  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) return false;
  res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
  fs.createReadStream(filePath).pipe(res);
  return true;
}

function sendIndex(res) {
  const index = path.join(DIST, 'index.html');
  res.writeHead(200, { 'Content-Type': MIME['.html'] });
  fs.createReadStream(index).pipe(res);
}

// --- request router ----------------------------------------------------------
const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, 'http://localhost');

  if (req.method === 'GET' && pathname === '/api/health') {
    return sendJson(res, 200, { status: 'ok', endpoint: SERVING_ENDPOINT });
  }

  // Body: { messages: [{role, content}], genie_conversation_id? }
  if (req.method === 'POST' && pathname === '/api/chat') {
    let body;
    try {
      body = JSON.parse((await readBody(req)) || '{}');
    } catch {
      return sendJson(res, 400, { error: 'invalid JSON body' });
    }
    const { messages, genie_conversation_id } = body;
    if (!Array.isArray(messages) || messages.length === 0) {
      return sendJson(res, 400, { error: 'messages[] is required' });
    }
    const custom_inputs = {};
    if (genie_conversation_id) custom_inputs.genie_conversation_id = genie_conversation_id;

    try {
      const token = await getToken();
      const url = `${workspaceHost()}/serving-endpoints/${SERVING_ENDPOINT}/invocations`;
      const upstream = await fetch(url, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ input: messages, custom_inputs }),
      });
      const raw = await upstream.text();
      if (!upstream.ok) {
        console.error(`endpoint error ${upstream.status}: ${raw}`);
        return sendJson(res, 502, { error: `Endpoint returned ${upstream.status}`, detail: raw });
      }
      let payload;
      try { payload = JSON.parse(raw); }
      catch { return sendJson(res, 502, { error: 'Endpoint returned non-JSON', detail: raw }); }

      const custom = payload.custom_outputs || {};
      return sendJson(res, 200, {
        content: extractText(payload) || '_(no answer returned)_',
        genie_conversation_id: custom.genie_conversation_id || genie_conversation_id || null,
        generated_sql: custom.generated_sql || null,
        custom_outputs: custom,
      });
    } catch (err) {
      console.error(err);
      return sendJson(res, 500, { error: err.message });
    }
  }

  // Static assets, then SPA fallback to index.html.
  if (req.method === 'GET') {
    if (pathname !== '/' && serveStatic(res, pathname)) return;
    return sendIndex(res);
  }

  sendJson(res, 405, { error: 'method not allowed' });
});

server.listen(PORT, () => {
  console.log(`Corridor Intelligence app on :${PORT} -> endpoint ${SERVING_ENDPOINT}`);
});
