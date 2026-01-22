import React, { useEffect, useMemo, useRef, useState } from "react";
import type { ChatMessage, UiSettings } from "./types";
import { sendChat } from "./api";
import { clampCssColor, uid } from "./utils";

const DEFAULTS: UiSettings = {
  title: "Mobile Maintenance Advisor",
  subtitle: "Chat with the agent running on MLflow Agent Server",
  accent: "#7c5cff",
  apiMode: "local-proxy",
  streaming: false,
};

const SYSTEM_PRIMER = `You are a helpful assistant. Ask clarifying questions when needed.`;

export default function App() {
  const [settings, setSettings] = useState<UiSettings>(() => {
    const saved = localStorage.getItem("mma.settings.v1");
    return saved ? { ...DEFAULTS, ...JSON.parse(saved) } : DEFAULTS;
  });

  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    const saved = localStorage.getItem("mma.messages.v1");
    if (!saved) {
      return [
        { id: uid("sys"), role: "system", content: SYSTEM_PRIMER, createdAt: Date.now() },
        {
          id: uid("a"),
          role: "assistant",
          content:
            "Hi — upload or describe the maintenance issue you're seeing, and I'll help you troubleshoot.\n\nTip: Try including the machine type, symptoms, and any error codes.",
          createdAt: Date.now(),
        },
      ];
    }
    return JSON.parse(saved) as ChatMessage[];
  });

  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const scrollerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    localStorage.setItem("mma.settings.v1", JSON.stringify(settings));
  }, [settings]);

  useEffect(() => {
    localStorage.setItem("mma.messages.v1", JSON.stringify(messages));
    // auto-scroll
    requestAnimationFrame(() => {
      const el = scrollerRef.current;
      if (el) el.scrollTop = el.scrollHeight;
    });
  }, [messages]);

  // Apply accent color to CSS var
  useEffect(() => {
    document.documentElement.style.setProperty(
      "--accent",
      clampCssColor(settings.accent, DEFAULTS.accent),
    );
  }, [settings.accent]);

  const chatOnly = useMemo(() => messages.filter((m) => m.role !== "system"), [messages]);

  async function onSend() {
    const content = draft.trim();
    if (!content || busy) return;

    const userMsg: ChatMessage = { id: uid("u"), role: "user", content, createdAt: Date.now() };
    setMessages((prev) => [...prev, userMsg]);
    setDraft("");
    setBusy(true);

    try {
      const resp = await sendChat([...messages, userMsg], settings.streaming);
      const assistantMsg: ChatMessage = {
        id: uid("a"),
        role: "assistant",
        content: resp.text || "(no response text found)",
        createdAt: Date.now(),
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err: any) {
      const assistantMsg: ChatMessage = {
        id: uid("a"),
        role: "assistant",
        content: `⚠️ ${err?.message ?? String(err)}`,
        createdAt: Date.now(),
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } finally {
      setBusy(false);
    }
  }

  function resetChat() {
    const fresh: ChatMessage[] = [
      { id: uid("sys"), role: "system", content: SYSTEM_PRIMER, createdAt: Date.now() },
      {
        id: uid("a"),
        role: "assistant",
        content:
          "New conversation started. What are you working on today?",
        createdAt: Date.now(),
      },
    ];
    setMessages(fresh);
  }

  return (
    <div className="container">
      <div className="card">
        <div className="header">
          <div className="brand">
            <div className="badge">MM</div>
            <div>
              <div className="h1">{settings.title}</div>
              <div className="sub">{settings.subtitle}</div>
            </div>
          </div>
          <div className="row">
            <button className="ghost" onClick={resetChat} disabled={busy}>
              Reset
            </button>
            <button
              className="primary"
              onClick={() => {
                const next = prompt("Accent color (hex like #7c5cff):", settings.accent);
                if (next) setSettings((s) => ({ ...s, accent: next }));
              }}
              disabled={busy}
              title="Quick theme tweak"
            >
              Theme
            </button>
          </div>
        </div>

        <div className="main">
          <div className="chat">
            <div className="messages" ref={scrollerRef}>
              {chatOnly.map((m) => (
                <div key={m.id} className={`msg ${m.role}`}>
                  <div className="avatar">{m.role === "user" ? "You" : "AI"}</div>
                  <div className="bubble">{m.content}</div>
                </div>
              ))}
              {busy && (
                <div className="msg assistant">
                  <div className="avatar">AI</div>
                  <div className="bubble">Thinking…</div>
                </div>
              )}
            </div>

            <div className="composer">
              <textarea
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                placeholder="Type a message…"
                onKeyDown={(e) => {
                  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") onSend();
                }}
                disabled={busy}
              />
              <button className="primary" onClick={onSend} disabled={busy || !draft.trim()}>
                Send
              </button>
            </div>
            <div className="small" style={{ marginTop: 10 }}>
              Tip: Use <span className="mono">Ctrl/⌘ + Enter</span> to send.
            </div>
          </div>

          <div className="sidebar">
            <div className="kv">
              <div className="label">Title</div>
              <input
                type="text"
                value={settings.title}
                onChange={(e) => setSettings((s) => ({ ...s, title: e.target.value }))}
              />
            </div>

            <div className="kv">
              <div className="label">Subtitle</div>
              <input
                type="text"
                value={settings.subtitle}
                onChange={(e) => setSettings((s) => ({ ...s, subtitle: e.target.value }))}
              />
            </div>

            <div className="kv">
              <div className="label">Accent color</div>
              <input
                type="text"
                value={settings.accent}
                onChange={(e) => setSettings((s) => ({ ...s, accent: e.target.value }))}
              />
            </div>

            <hr />

            <div className="kv">
              <div className="label">Conversation memory (local)</div>
              <div className="small">
                Messages + settings are stored in your browser <span className="mono">localStorage</span>.
                Use <b>Reset</b> to clear the conversation.
              </div>
            </div>

            <hr />

            <div className="kv">
              <div className="label">Agent endpoint</div>
              <div className="small">
                The Node server proxies <span className="mono">/api/chat</span> to the MLflow Agent Server’s{" "}
                <span className="mono">/invocations</span> endpoint. (Configured via <span className="mono">API_PROXY</span>.)
              </div>
            </div>

            <div className="kv">
              <button
                onClick={async () => {
                  try {
                    const r = await fetch("/api/health");
                    alert(r.ok ? "Backend OK" : "Backend error");
                  } catch (e: any) {
                    alert(e?.message ?? String(e));
                  }
                }}
              >
                Test backend
              </button>
            </div>

            <div className="small">
              Next steps: add a left nav, file upload, “tool call” rendering, feedback buttons, and per-user
              conversation storage in a DB / UC volume.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
