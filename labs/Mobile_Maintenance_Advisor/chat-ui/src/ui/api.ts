import type { AudioAttachment, ChatMessage } from "./types";

export interface ChatResponse {
  text: string;
  raw?: unknown;
}

interface InputTextPart {
  type: "input_text";
  text: string;
}

interface InputAudioPart {
  type: "input_audio";
  audio: {
    data: string;
    format: AudioAttachment["format"];
  };
}

export async function sendChat(messages: ChatMessage[], stream: boolean): Promise<ChatResponse> {
  // Convert to Responses API-style inputs
  const input = messages
    .filter((m) => m.role !== "system" || m.content.trim().length > 0)
    .map((m) => {
      if (m.role === "assistant") {
        return { role: m.role, content: m.content };
      }

      const parts: Array<InputTextPart | InputAudioPart> = [];
      if (m.content.trim().length > 0) {
        parts.push({ type: "input_text", text: m.content });
      }
      if (m.audio) {
        parts.push({ type: "input_audio", audio: { data: m.audio.data, format: m.audio.format } });
      }

      return { role: m.role, content: parts.length > 0 ? parts : m.content };
    });

  const res = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ input, stream }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Chat request failed (${res.status}): ${body || res.statusText}`);
  }

  // This UI supports non-streaming responses out of the box.
  // (Streaming is supported by the backend, but the UI uses non-streaming parsing for simplicity.)
  const data = (await res.json()) as ChatResponse;
  return data;
}
