import React, { useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
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
const RESET_ASSISTANT_MESSAGE = "New conversation started. Please upload or describe the maintenance issue you're seeing, and I'll help you troubleshoot.\n\nTip: Try including the machine type, symptoms, and any error codes.";

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
  const [isRecording, setIsRecording] = useState(false);
  const [recordingLevel, setRecordingLevel] = useState(0);
  const recorderRef = useRef<MediaRecorder | null>(null);
  const recorderChunksRef = useRef<Blob[]>([]);
  const recorderStreamRef = useRef<MediaStream | null>(null);
  const recorderTimeoutRef = useRef<number | null>(null);
  const audioContextRef = useRef<AudioContext | null>(null);
  const analyserRef = useRef<AnalyserNode | null>(null);
  const sourceNodeRef = useRef<MediaStreamAudioSourceNode | null>(null);
  const rafRef = useRef<number | null>(null);
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

  useEffect(() => {
    return () => {
      recorderRef.current?.stop();
      recorderStreamRef.current?.getTracks().forEach((track) => track.stop());
      if (recorderTimeoutRef.current) window.clearTimeout(recorderTimeoutRef.current);
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      analyserRef.current?.disconnect();
      sourceNodeRef.current?.disconnect();
      audioContextRef.current?.close();
    };
  }, []);

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

  function pickRecorderMimeType() {
    if (typeof MediaRecorder === "undefined") return "";
    const candidates = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4"];
    return candidates.find((type) => MediaRecorder.isTypeSupported(type)) ?? "";
  }

  function mimeTypeToFormat(mimeType: string): AudioAttachment["format"] | null {
    if (mimeType.includes("webm")) return "webm";
    if (mimeType.includes("mp4")) return "m4a";
    if (mimeType.includes("wav")) return "wav";
    if (mimeType.includes("mpeg") || mimeType.includes("mp3")) return "mp3";
    return null;
  }

  function stopMeter() {
    if (rafRef.current) cancelAnimationFrame(rafRef.current);
    rafRef.current = null;
    analyserRef.current?.disconnect();
    sourceNodeRef.current?.disconnect();
    analyserRef.current = null;
    sourceNodeRef.current = null;
    audioContextRef.current?.close();
    audioContextRef.current = null;
    setRecordingLevel(0);
  }

  function startMeter(stream: MediaStream) {
    if (typeof AudioContext === "undefined") return;
    const audioContext = new AudioContext();
    const analyser = audioContext.createAnalyser();
    analyser.fftSize = 512;
    const source = audioContext.createMediaStreamSource(stream);
    source.connect(analyser);
    audioContextRef.current = audioContext;
    analyserRef.current = analyser;
    sourceNodeRef.current = source;

    const data = new Uint8Array(analyser.fftSize);
    const tick = () => {
      analyser.getByteTimeDomainData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i += 1) {
        const v = (data[i] - 128) / 128;
        sum += v * v;
      }
      const rms = Math.sqrt(sum / data.length);
      setRecordingLevel(rms);
      rafRef.current = requestAnimationFrame(tick);
    };
    tick();
  }

  async function startRecording() {
    if (isRecording) return;
    if (typeof navigator === "undefined" || !navigator.mediaDevices?.getUserMedia) {
      setAudioError("Recording isn't supported in this browser.");
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const mimeType = pickRecorderMimeType();
      const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
      recorderChunksRef.current = [];
      recorder.ondataavailable = (event) => {
        if (event.data.size > 0) recorderChunksRef.current.push(event.data);
      };
      recorder.onstop = () => {
        const blob = new Blob(recorderChunksRef.current, { type: recorder.mimeType });
        const format = mimeTypeToFormat(recorder.mimeType) ?? "webm";
        const filename = `recording-${Date.now()}.${format}`;
        const file = new File([blob], filename, { type: recorder.mimeType || `audio/${format}` });
        setAudioFile(file);
        setAudioFormat(format);
        setAudioError(null);
        recorderChunksRef.current = [];
      };
      recorderRef.current = recorder;
      recorderStreamRef.current = stream;
      setIsRecording(true);
      setAudioError(null);
      startMeter(stream);
      recorder.start();
      recorderTimeoutRef.current = window.setTimeout(() => {
        stopRecording();
      }, 30000);
    } catch (err: any) {
      setAudioError(err?.message ?? "Microphone access was denied.");
      recorderStreamRef.current?.getTracks().forEach((track) => track.stop());
      recorderStreamRef.current = null;
    }
  }

  function stopRecording() {
    if (!isRecording) return;
    recorderRef.current?.stop();
    recorderStreamRef.current?.getTracks().forEach((track) => track.stop());
    recorderStreamRef.current = null;
    recorderRef.current = null;
    setIsRecording(false);
    if (recorderTimeoutRef.current) window.clearTimeout(recorderTimeoutRef.current);
    recorderTimeoutRef.current = null;
    stopMeter();
  }

  async function onSend() {
    const content = draft.trim();
    if ((!content && !audioFile) || busy || isRecording) return;

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
    setIsRecording(false);
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
    setIsRecording(false);
    recorderRef.current?.stop();
    recorderStreamRef.current?.getTracks().forEach((track) => track.stop());
    recorderStreamRef.current = null;
    recorderRef.current = null;
    if (recorderTimeoutRef.current) window.clearTimeout(recorderTimeoutRef.current);
    recorderTimeoutRef.current = null;
    stopMeter();
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
                    {m.content && (
                      <ReactMarkdown className="md" remarkPlugins={[remarkGfm]}>
                        {m.content}
                      </ReactMarkdown>
                    )}
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
                    disabled={busy || isRecording}
                  />
                  <button
                    className="record-btn"
                    type="button"
                    onClick={isRecording ? stopRecording : startRecording}
                    disabled={busy}
                  >
                    {isRecording ? "Stop recording" : "Record audio"}
                  </button>
                  <button
                    className="upload-btn"
                    type="button"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={busy || isRecording}
                  >
                    Upload Audio
                  </button>
                  {audioFile && (
                    <button
                      className="ghost"
                      type="button"
                      onClick={() => onSelectAudioFile(null)}
                      disabled={busy || isRecording}
                    >
                      Remove
                    </button>
                  )}
                  {isRecording && (
                    <div className="recording-indicator">
                      Recording…
                      <div className="recording-meter">
                        <span style={{ width: `${Math.min(100, Math.round(recordingLevel * 220))}%` }} />
                      </div>
                    </div>
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
        </div>
      </div>
    </div>
  );
}
