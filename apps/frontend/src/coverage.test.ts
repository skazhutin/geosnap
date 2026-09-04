import { describe, expect, it, vi } from "vitest";

import { loadCoverage, PRODUCTION_GALLERY_SHA256 } from "./coverage";

function response(gallerySha = PRODUCTION_GALLERY_SHA256) {
  return new Response(JSON.stringify({
    schema_version: 1,
    kind: "geosnap-production-gallery-coverage-grid",
    gallery_sha256: gallerySha,
    reference_count: 20487,
    grid: { bins_per_axis: 20, occupied_cells: 1, total_cells: 400, density_thresholds: { sparse_max: 1, medium_max: 2 } },
    methodology: "test",
    geojson: { type: "FeatureCollection", features: [] },
  }), { status: 200, headers: { "Content-Type": "application/json" } });
}

describe("coverage contract", () => {
  it("accepts data bound to the frozen gallery", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response()));
    await expect(loadCoverage()).resolves.toMatchObject({ reference_count: 20487 });
  });

  it("rejects a stale gallery binding", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response("0".repeat(64))));
    await expect(loadCoverage()).rejects.toThrow("coverage_invalid");
  });
});
