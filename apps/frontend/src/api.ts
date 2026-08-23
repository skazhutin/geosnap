import { parseLocalizeResponse, type LocalizeResponse } from "./types";

const FALLBACK_API_BASE_URL = "http://localhost:8000";

function normalizeApiBaseUrl(value: string | undefined): string {
  const candidate = value?.trim() || FALLBACK_API_BASE_URL;
  return candidate.replace(/\/+$/, "");
}

export const API_BASE_URL = normalizeApiBaseUrl(import.meta.env.VITE_API_BASE_URL);

export async function localizeImage(
  file: File,
  signal?: AbortSignal,
): Promise<LocalizeResponse> {
  const body = new FormData();
  body.append("image", file, file.name);

  let response: Response;
  try {
    response = await fetch(`${API_BASE_URL}/localize`, {
      method: "POST",
      body,
      signal,
      headers: { Accept: "application/json" },
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    throw new Error("api_unreachable");
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new Error("invalid_api_response");
  }

  const parsed = parseLocalizeResponse(payload);
  if (!response.ok && parsed.status === "ok") {
    throw new Error("invalid_api_response");
  }
  return parsed;
}

export function safeThumbnailUrl(value: string | null | undefined): string | null {
  if (!value || !/^\/thumbnails\/[A-Za-z0-9%._~-]+$/.test(value)) return null;
  if (value.toLowerCase().includes("%2e")) return null;
  return `${API_BASE_URL}${value}`;
}
