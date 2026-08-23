export const API_STATUSES = [
  "ok",
  "invalid_image",
  "unsupported_format",
  "image_too_large",
  "model_not_ready",
  "index_not_ready",
  "low_confidence",
  "out_of_coverage",
  "internal_error",
] as const;

export type ApiStatus = (typeof API_STATUSES)[number];

export interface Prediction {
  lat: number;
  lon: number;
  confidence: number;
  uncertainty_radius_m?: number | null;
}

export interface Hypothesis {
  lat: number;
  lon: number;
  score: number;
  uncertainty_radius_m?: number | null;
}

export interface ReferenceMatch {
  reference_id: string;
  source: string;
  lat: number;
  lon: number;
  retrieval_score: number;
  verification_score?: number | null;
  thumbnail_url?: string | null;
  attribution: string;
  license: string;
  source_url: string;
  license_url?: string | null;
  contributor_url?: string | null;
}

export interface Diagnostics {
  total_ms?: number | null;
}

export interface LocalizeResponse {
  status: ApiStatus;
  prediction: Prediction | null;
  hypotheses: Hypothesis[];
  matches: ReferenceMatch[];
  diagnostics: Diagnostics;
  message: string | null;
  request_id: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function safeHttpsUrl(value: unknown, allowedHosts: Set<string>): string | null {
  if (typeof value !== "string") return null;
  try {
    const parsed = new URL(value);
    if (
      parsed.protocol !== "https:" ||
      parsed.username !== "" ||
      parsed.password !== "" ||
      !allowedHosts.has(parsed.hostname.toLowerCase())
    ) {
      return null;
    }
    const sensitive = new Set(["access_token", "token", "api_key", "apikey", "signature", "sig"]);
    if ([...parsed.searchParams.keys()].some((key) => sensitive.has(key.toLowerCase()))) {
      return null;
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

function isCoordinate(lat: unknown, lon: unknown): lat is number {
  return (
    isFiniteNumber(lat) &&
    isFiniteNumber(lon) &&
    lat >= -90 &&
    lat <= 90 &&
    lon >= -180 &&
    lon <= 180
  );
}

function optionalRadius(value: unknown): number | null {
  return isFiniteNumber(value) && value > 0 ? value : null;
}

function parsePrediction(value: unknown): Prediction | null {
  if (!isRecord(value) || !isCoordinate(value.lat, value.lon)) {
    return null;
  }
  if (!isFiniteNumber(value.confidence) || value.confidence < 0 || value.confidence > 1) {
    return null;
  }
  return {
    lat: value.lat,
    lon: value.lon as number,
    confidence: value.confidence,
    uncertainty_radius_m: optionalRadius(value.uncertainty_radius_m),
  };
}

function parseHypotheses(value: unknown): Hypothesis[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item): Hypothesis[] => {
    if (!isRecord(item) || !isCoordinate(item.lat, item.lon)) return [];
    if (!isFiniteNumber(item.score) || item.score < 0 || item.score > 1) return [];
    return [
      {
        lat: item.lat,
        lon: item.lon as number,
        score: item.score,
        uncertainty_radius_m: optionalRadius(item.uncertainty_radius_m),
      },
    ];
  });
}

function parseMatches(value: unknown): ReferenceMatch[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item): ReferenceMatch[] => {
    if (!isRecord(item) || !isCoordinate(item.lat, item.lon)) return [];
    if (
      typeof item.reference_id !== "string" ||
      typeof item.source !== "string" ||
      typeof item.attribution !== "string" ||
      typeof item.license !== "string" ||
      !isFiniteNumber(item.retrieval_score)
    ) {
      return [];
    }
    const sourceHosts =
      item.source.toLowerCase() === "mapillary"
        ? new Set(["mapillary.com", "www.mapillary.com"])
        : item.source.toLowerCase() === "kartaview"
          ? new Set(["kartaview.org", "www.kartaview.org"])
          : new Set<string>();
    const sourceUrl = safeHttpsUrl(item.source_url, sourceHosts);
    if (sourceUrl === null) return [];
    const licenseUrl = safeHttpsUrl(
      item.license_url,
      new Set(["creativecommons.org", "www.creativecommons.org"]),
    );
    const contributorUrl = safeHttpsUrl(item.contributor_url, sourceHosts);
    return [
      {
        reference_id: item.reference_id,
        source: item.source,
        lat: item.lat,
        lon: item.lon as number,
        retrieval_score: item.retrieval_score,
        verification_score: isFiniteNumber(item.verification_score)
          ? item.verification_score
          : null,
        thumbnail_url: typeof item.thumbnail_url === "string" ? item.thumbnail_url : null,
        attribution: item.attribution,
        license: item.license,
        source_url: sourceUrl,
        license_url: licenseUrl,
        contributor_url: contributorUrl,
      },
    ];
  });
}

export function parseLocalizeResponse(value: unknown): LocalizeResponse {
  if (!isRecord(value) || !API_STATUSES.includes(value.status as ApiStatus)) {
    throw new Error("invalid_api_response");
  }

  const prediction = parsePrediction(value.prediction);
  if (value.status === "ok" && prediction === null) {
    throw new Error("invalid_api_response");
  }

  const diagnostics = isRecord(value.diagnostics)
    ? {
        total_ms: isFiniteNumber(value.diagnostics.total_ms)
          ? value.diagnostics.total_ms
          : null,
      }
    : {};

  return {
    status: value.status as ApiStatus,
    prediction,
    hypotheses: parseHypotheses(value.hypotheses),
    matches: parseMatches(value.matches),
    diagnostics,
    message: typeof value.message === "string" ? value.message : null,
    request_id: typeof value.request_id === "string" ? value.request_id : "",
  };
}
