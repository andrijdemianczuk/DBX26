export type ChatRole = "user" | "assistant" | "system";

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: number;
  audio?: AudioAttachment;
}

export interface UiSettings {
  title: string;
  subtitle: string;
  apiMode: "local-proxy";
  streaming: boolean;
}

export interface AudioAttachment {
  name: string;
  mime: string;
  size: number;
  format: "wav" | "mp3" | "m4a" | "webm";
  data: string;
}
