const DEVELOPMENT_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const DEVELOPMENT_ATTRIBUTION = "© OpenStreetMap contributors";

export const MAP_TILE_URL = (
  import.meta.env.VITE_MAP_TILE_URL?.trim() || DEVELOPMENT_TILE_URL
).replace("{token}", encodeURIComponent(import.meta.env.VITE_MAP_PUBLIC_PARAMETER?.trim() || ""));

export const MAP_ATTRIBUTION =
  import.meta.env.VITE_MAP_ATTRIBUTION?.trim() || DEVELOPMENT_ATTRIBUTION;

export function normalizeTelegramBotUrl(value: string | undefined): string | null {
  const candidate = value?.trim() || "";
  return /^https:\/\/t\.me\/[A-Za-z0-9_]{5,64}$/.test(candidate) ? candidate : null;
}

export const TELEGRAM_BOT_URL = normalizeTelegramBotUrl(import.meta.env.VITE_TELEGRAM_BOT_URL);

export const COVERAGE_DATA_URL = "/coverage/production-gallery-grid.json";
