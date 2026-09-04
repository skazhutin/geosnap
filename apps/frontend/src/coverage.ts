import type { FeatureCollection, Polygon } from "geojson";

import { COVERAGE_DATA_URL } from "./config";

export const PRODUCTION_GALLERY_SHA256 =
  "ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da";

export interface CoverageProperties {
  cell: string;
  references: number;
  provider_count: number;
  providers: string;
  density: "sparse" | "medium" | "dense";
}

export interface CoveragePayload {
  schema_version: 1;
  kind: "geosnap-production-gallery-coverage-grid";
  gallery_sha256: string;
  reference_count: number;
  grid: {
    bins_per_axis: number;
    occupied_cells: number;
    total_cells: number;
    density_thresholds: { sparse_max: number; medium_max: number };
  };
  methodology: string;
  geojson: FeatureCollection<Polygon, CoverageProperties>;
}

function isCoveragePayload(value: unknown): value is CoveragePayload {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<CoveragePayload>;
  return (
    candidate.schema_version === 1 &&
    candidate.kind === "geosnap-production-gallery-coverage-grid" &&
    candidate.gallery_sha256 === PRODUCTION_GALLERY_SHA256 &&
    candidate.reference_count === 20_487 &&
    candidate.geojson?.type === "FeatureCollection" &&
    Array.isArray(candidate.geojson.features)
  );
}

export async function loadCoverage(signal?: AbortSignal): Promise<CoveragePayload> {
  const response = await fetch(COVERAGE_DATA_URL, { signal, headers: { Accept: "application/json" } });
  if (!response.ok) throw new Error("coverage_unavailable");
  const payload: unknown = await response.json();
  if (!isCoveragePayload(payload)) throw new Error("coverage_invalid");
  return payload;
}
