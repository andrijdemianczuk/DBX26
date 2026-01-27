export function uid(prefix = "m"): string {
  return `${prefix}_${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
}

export function clampCssColor(color: string, fallback: string): string {
  // Very lightweight validation so a bad value doesn't break CSS variables
  if (!color) return fallback;
  const c = color.trim();
  if (c.startsWith("#") && (c.length === 4 || c.length === 7)) return c;
  if (c.startsWith("rgb(") || c.startsWith("rgba(") || c.startsWith("hsl(") || c.startsWith("hsla(")) return c;
  // allow CSS named colors as well
  if (/^[a-zA-Z]+$/.test(c)) return c;
  return fallback;
}

export function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      if (typeof reader.result !== "string") {
        reject(new Error("Failed to read file as data URL."));
        return;
      }
      const [, base64] = reader.result.split(",", 2);
      if (!base64) {
        reject(new Error("Failed to parse base64 data."));
        return;
      }
      resolve(base64);
    };
    reader.onerror = () => reject(reader.error ?? new Error("Failed to read file."));
    reader.readAsDataURL(file);
  });
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  const value = size >= 10 || idx === 0 ? Math.round(size) : Math.round(size * 10) / 10;
  return `${value} ${units[idx]}`;
}
