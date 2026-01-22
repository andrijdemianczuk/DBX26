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
