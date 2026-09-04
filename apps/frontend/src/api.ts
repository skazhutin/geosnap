import { parseLocalizeResponse, type LocalizeResponse } from "./types";

const FALLBACK_API_BASE_URL = "/api";

function normalizeApiBaseUrl(value: string | undefined): string {
  const candidate = value?.trim() || FALLBACK_API_BASE_URL;
  return candidate.replace(/\/+$/, "");
}

export const API_BASE_URL = normalizeApiBaseUrl(import.meta.env.VITE_API_BASE_URL);

export type ApiFailureKind = "unreachable" | "timeout" | "malformed";

export class ApiClientError extends Error {
  constructor(public readonly kind: ApiFailureKind) {
    super(kind);
    this.name = "ApiClientError";
  }
}

export interface LocalizeOutcome {
  response: LocalizeResponse;
  retryAfterSeconds: number | null;
}

function retryAfterSeconds(value: string | null): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds);
  const date = Date.parse(value);
  if (!Number.isFinite(date)) return null;
  return Math.max(0, Math.ceil((date - Date.now()) / 1000));
}

export async function localizeImage(
  file: File,
  signal?: AbortSignal,
  timeoutMs = 35_000,
): Promise<LocalizeOutcome> {
  const body = new FormData();
  body.append("image", file, file.name);

  const controller = new AbortController();
  let timedOut = false;
  const onAbort = () => controller.abort();
  signal?.addEventListener("abort", onAbort, { once: true });
  const timer = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  let response: Response;
  try {
    response = await fetch(`${API_BASE_URL}/localize`, {
      method: "POST",
      body,
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
  } catch (error) {
    if (timedOut) throw new ApiClientError("timeout");
    if (signal?.aborted) throw new DOMException("Request aborted", "AbortError");
    throw new ApiClientError("unreachable");
  } finally {
    window.clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new ApiClientError("malformed");
  }

  let parsed: LocalizeResponse;
  try {
    parsed = parseLocalizeResponse(payload);
  } catch {
    throw new ApiClientError("malformed");
  }
  if (!response.ok && parsed.status === "ok") {
    throw new ApiClientError("malformed");
  }
  return {
    response: parsed,
    retryAfterSeconds: retryAfterSeconds(response.headers.get("Retry-After")),
  };
}

export function safeThumbnailUrl(value: string | null | undefined): string | null {
  if (!value || !/^\/thumbnails\/[A-Za-z0-9%._~-]+$/.test(value)) return null;
  if (value.toLowerCase().includes("%2e")) return null;
  return `${API_BASE_URL}${value}`;
}
