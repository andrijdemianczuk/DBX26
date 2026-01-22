import express from "express";
import path from "path";
import { fileURLToPath } from "url";

// Node 18+ has fetch built-in.
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = Number(process.env.CHAT_APP_PORT || process.env.PORT || 3000);
const API_PROXY = process.env.API_PROXY || "http://localhost:8000/invocations";

/**
 * Best-effort extraction of assistant text from an MLflow ResponsesAgentResponse.
 * The exact schema can vary depending on agent SDK / response type.
 */
function extractText(payload) {
  if (!payload || typeof payload !== "object") return "";

  // Common: { output: [ { content: "..." } ] }
  const out = payload.output ?? payload.outputs ?? payload.response ?? payload.data;
  if (Array.isArray(out)) {
    return out.map(extractText).filter(Boolean).join("\n");
  }

  // If the object itself has content
  if (typeof payload.content === "string") return payload.content;

  // OpenAI Responses-style: content is array of parts like {type:"output_text", text:"..."}
  if (Array.isArray(payload.content)) {
    return payload.content
      .map((p) => {
        if (!p) return "";
        if (typeof p === "string") return p;
        if (typeof p.text === "string") return p.text;
        if (typeof p.content === "string") return p.content;
        return "";
      })
      .filter(Boolean)
      .join("");
  }

  // Chat-completions-style: { choices: [{ message: { content: "..." } }] }
  if (Array.isArray(payload.choices)) {
    return payload.choices
      .map((c) => c?.message?.content || c?.delta?.content || "")
      .filter(Boolean)
      .join("");
  }

  // Some responses have: { message: { content: ... } }
  if (payload.message) return extractText(payload.message);

  // Fallback: try nested known keys
  for (const k of ["result", "response", "output_text", "text"]) {
    if (payload[k]) return extractText(payload[k]);
  }

  return "";
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, apiProxy: API_PROXY });
});

app.post("/api/chat", async (req, res) => {
  const { input, stream } = req.body || {};
  if (!Array.isArray(input) || input.length === 0) {
    return res.status(400).send("Body must be { input: [{role, content}, ...], stream?: boolean }");
  }

  const upstreamBody = { input, stream: Boolean(stream) };

  // Non-streaming (default)
  if (!upstreamBody.stream) {
    const upstream = await fetch(API_PROXY, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(upstreamBody),
    });

    const text = await upstream.text();
    if (!upstream.ok) {
      return res.status(upstream.status).send(text || upstream.statusText);
    }

    let json;
    try {
      json = JSON.parse(text);
    } catch {
      json = { raw: text };
    }

    return res.json({ text: extractText(json), raw: json });
  }

  // Streaming: pass through whatever the Agent Server returns (often text/event-stream)
  const upstream = await fetch(API_PROXY, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "text/event-stream",
    },
    body: JSON.stringify(upstreamBody),
  });

  if (!upstream.ok || !upstream.body) {
    const err = await upstream.text().catch(() => upstream.statusText);
    return res.status(upstream.status).send(err);
  }

  res.setHeader("Content-Type", upstream.headers.get("content-type") || "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const reader = upstream.body.getReader();
  const encoder = new TextEncoder();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) res.write(value);
    }
  } catch (e) {
    res.write(encoder.encode(`\n\n`));
  } finally {
    res.end();
  }
});

// Serve built assets (vite build -> dist/)
const distDir = path.join(__dirname, "..", "dist");
app.use(express.static(distDir));
app.get("*", (_req, res) => {
  res.sendFile(path.join(distDir, "index.html"));
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
  console.log(`Proxying /api/chat -> ${API_PROXY}`);
});
