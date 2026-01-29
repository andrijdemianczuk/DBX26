import React, { useEffect, useMemo, useRef, useState } from "react";
import type { AudioAttachment, ChatMessage, UiSettings } from "./types";
import { sendChat } from "./api";
import { fileToBase64, formatBytes, uid } from "./utils";

const DEFAULTS: UiSettings = {
  title: "Mobile Maintenance Advisor",
  subtitle: "Chat with the agent running on MLflow Agent Server",
  apiMode: "local-proxy",
  streaming: false,
};

const SYSTEM_PRIMER = `You are a helpful assistant. Ask clarifying questions when needed.`;
const INTRO_ASSISTANT_MESSAGE =
  "Hi — upload or describe the maintenance issue you're seeing, and I'll help you troubleshoot.\n\nTip: Try including the machine type, symptoms, and any error codes.";
const RESET_ASSISTANT_MESSAGE = "New conversation started. What are you working on today?";

const AUDIO_MIME_TO_FORMAT: Record<string, AudioAttachment["format"]> = {
  "audio/mpeg": "mp3",
  "audio/mp3": "mp3",
  "audio/wav": "wav",
  "audio/x-wav": "wav",
  "audio/wave": "wav",
  "audio/mp4": "m4a",
  "audio/x-m4a": "m4a",
  "audio/webm": "webm",
};

const AUDIO_EXT_TO_FORMAT: Record<string, AudioAttachment["format"]> = {
  mp3: "mp3",
  wav: "wav",
  m4a: "m4a",
  webm: "webm",
};

export default function App() {
  const [settings, setSettings] = useState<UiSettings>(DEFAULTS);
  const [theme, setTheme] = useState<"light" | "dark">(() => {
    if (typeof window === "undefined") return "light";
    const saved = window.localStorage.getItem("mma-theme");
    if (saved === "light" || saved === "dark") return saved;
    return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });
  const [palette, setPalette] = useState<"a" | "b" | "c" | "d">(() => {
    if (typeof window === "undefined") return "a";
    const saved = window.localStorage.getItem("mma-palette");
    if (saved === "a" || saved === "b" || saved === "c" || saved === "d") return saved;
    return "a";
  });

  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    return [
      { id: uid("sys"), role: "system", content: SYSTEM_PRIMER, createdAt: Date.now() },
      {
        id: uid("a"),
        role: "assistant",
        content: INTRO_ASSISTANT_MESSAGE,
        createdAt: Date.now(),
      },
    ];
  });

  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [audioFile, setAudioFile] = useState<File | null>(null);
  const [audioFormat, setAudioFormat] = useState<AudioAttachment["format"] | null>(null);
  const [audioError, setAudioError] = useState<string | null>(null);
  const [pinnedAudio, setPinnedAudio] = useState<AudioAttachment | null>(null);
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    // auto-scroll
    requestAnimationFrame(() => {
      const el = scrollerRef.current;
      if (el) el.scrollTop = el.scrollHeight;
    });
  }, [messages]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try {
      window.localStorage.setItem("mma-theme", theme);
    } catch {
      // ignore storage errors (private mode, etc.)
    }
  }, [theme]);

  useEffect(() => {
    document.documentElement.dataset.palette = palette;
    try {
      window.localStorage.setItem("mma-palette", palette);
    } catch {
      // ignore storage errors (private mode, etc.)
    }
  }, [palette]);

  const chatOnly = useMemo(() => messages.filter((m) => m.role !== "system"), [messages]);

  function inferAudioFormat(file: File): AudioAttachment["format"] | null {
    if (file.type && AUDIO_MIME_TO_FORMAT[file.type]) return AUDIO_MIME_TO_FORMAT[file.type];
    const ext = file.name.split(".").pop()?.toLowerCase();
    if (ext && AUDIO_EXT_TO_FORMAT[ext]) return AUDIO_EXT_TO_FORMAT[ext];
    return null;
  }

  function onSelectAudioFile(file: File | null) {
    if (!file) {
      setAudioFile(null);
      setAudioFormat(null);
      setAudioError(null);
      return;
    }
    const format = inferAudioFormat(file);
    if (!format) {
      setAudioFile(null);
      setAudioFormat(null);
      setAudioError("Only .wav, .mp3, .m4a, or .webm audio files are supported.");
      if (fileInputRef.current) fileInputRef.current.value = "";
      return;
    }
    setAudioFile(file);
    setAudioFormat(format);
    setAudioError(null);
  }

  async function onSend() {
    const content = draft.trim();
    if ((!content && !audioFile) || busy) return;

    let audioAttachment: AudioAttachment | undefined;
    setBusy(true);
    if (audioFile && audioFormat) {
      try {
        const data = await fileToBase64(audioFile);
        audioAttachment = {
          name: audioFile.name,
          mime: audioFile.type || `audio/${audioFormat}`,
          size: audioFile.size,
          format: audioFormat,
          data,
        };
      } catch (err: any) {
        setBusy(false);
        setAudioError(err?.message ?? "Failed to read the audio file.");
        return;
      }
    }

    const userMsg: ChatMessage = {
      id: uid("u"),
      role: "user",
      content,
      createdAt: Date.now(),
      audio: audioAttachment,
    };
    const activeAudio = audioAttachment ?? pinnedAudio ?? undefined;
    const outboundMsg = activeAudio ? { ...userMsg, audio: activeAudio } : userMsg;
    if (audioAttachment) {
      setPinnedAudio(audioAttachment);
    }
    setMessages((prev) => [...prev, userMsg]);
    setDraft("");
    setAudioFile(null);
    setAudioFormat(null);
    setAudioError(null);
    if (fileInputRef.current) fileInputRef.current.value = "";

    try {
      const shouldPinAudio = Boolean(pinnedAudio && !audioAttachment);
      const payload = [...messages, outboundMsg]
        .map((msg, index, arr) => {
          if (!shouldPinAudio) return msg;
          if (index === arr.length - 1) return msg;
          return { ...msg, audio: undefined };
        })
        .filter(
          (msg) =>
            !(
              msg.role === "assistant" &&
              (msg.content === INTRO_ASSISTANT_MESSAGE || msg.content === RESET_ASSISTANT_MESSAGE)
            ),
        );
      const resp = await sendChat(payload, settings.streaming);
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
        content: RESET_ASSISTANT_MESSAGE,
        createdAt: Date.now(),
      },
    ];
    setMessages(fresh);
    setDraft("");
    setAudioFile(null);
    setAudioFormat(null);
    setAudioError(null);
    setPinnedAudio(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
  }

  return (
    <div className="container">
      <div className="card">
        <div className="header">
          <div className="brand">
            <div className="badge">
              <svg viewBox="0 0 120 120" width="24" height="24" fill="currentColor">
                <path d="M60 11.23L27.08 30.22l0 37.98L60 107.19l32.92-18.99l0-37.98L60 11.23z M60 21.65l23.94 13.82l0 27.64L60 90.94l-23.94-13.82l0-27.64L60 21.65z" />
              </svg>
            </div>
            <div>
              <div className="h1">{settings.title}</div>
              <div className="sub">{settings.subtitle}</div>
            </div>
          </div>
          <div className="row">
            <button className="ghost" onClick={resetChat} disabled={busy}>
              Reset
            </button>
            <select
              className="select"
              value={palette}
              onChange={(e) => setPalette(e.target.value as "a" | "b" | "c" | "d")}
            >
              <option value="a">Ink + Electric Blue</option>
              <option value="b">Charcoal + Warm Gold</option>
              <option value="c">Slate + Neon Lime</option>
              <option value="d">Deep Navy + Violet</option>
            </select>
            <button className="ghost" onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>
              {theme === "dark" ? "Light mode" : "Dark mode"}
            </button>
          </div>
        </div>

        <div className="main">
          <div className="chat">
            <div className="messages" ref={scrollerRef}>
              {chatOnly.map((m) => (
                <div key={m.id} className={`msg ${m.role}`}>
                  <div className="avatar">{m.role === "user" ? "You" : "AI"}</div>
                  <div className="bubble">
                    {m.content && <div>{m.content}</div>}
                    {m.audio && (
                      <div className="attachment">
                        <div className="attachment-title">Audio attachment</div>
                        <div className="attachment-meta">
                          {m.audio.name} · {m.audio.format.toUpperCase()} · {formatBytes(m.audio.size)}
                        </div>
                      </div>
                    )}
                  </div>
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
              <div className="composer-body">
                <textarea
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  placeholder="Type a message…"
                  onKeyDown={(e) => {
                    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") onSend();
                  }}
                  disabled={busy}
                />
                <div className="composer-tools">
                  <input
                    ref={fileInputRef}
                    className="file-input"
                    type="file"
                    accept="audio/*"
                    onChange={(e) => onSelectAudioFile(e.target.files?.[0] ?? null)}
                    disabled={busy}
                  />
                  <button
                    className="ghost"
                    type="button"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={busy}
                  >
                    Attach audio
                  </button>
                  {audioFile && (
                    <button
                      className="ghost"
                      type="button"
                      onClick={() => onSelectAudioFile(null)}
                      disabled={busy}
                    >
                      Remove
                    </button>
                  )}
                  {audioError && <div className="error-text">{audioError}</div>}
                </div>
                {audioFile && audioFormat && (
                  <div className="attachment-preview">
                    {audioFile.name} · {audioFormat.toUpperCase()} · {formatBytes(audioFile.size)}
                  </div>
                )}
              </div>
              <button className="primary" onClick={onSend} disabled={busy || (!draft.trim() && !audioFile)}>
                Send
              </button>
            </div>
            <div className="small" style={{ marginTop: 10 }}>
              Tip: Use <span className="mono">Ctrl/⌘ + Enter</span> to send.
            </div>
          </div>

          {/*
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
          */}
        </div>
      </div>
    </div>
  );
}
