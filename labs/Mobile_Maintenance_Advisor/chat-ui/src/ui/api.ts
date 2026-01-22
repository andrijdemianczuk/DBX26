import type { ChatMessage } from "./types";

export interface ChatResponse {
  text: string;
  raw?: unknown;
}

export async function sendChat(messages: ChatMessage[], stream: boolean): Promise<ChatResponse> {
  // Convert to Responses API-style inputs
  const input = messages
    .filter((m) => m.role !== "system" || m.content.trim().length > 0)
    .map((m) => ({ role: m.role, content: m.content }));

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
