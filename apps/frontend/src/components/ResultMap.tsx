import { useEffect, useRef, useState } from "react";
import * as maplibregl from "maplibre-gl";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import type { GeoJSONSource, MapMouseEvent, StyleSpecification } from "maplibre-gl";

import { loadCoverage, type CoveragePayload } from "../coverage";
import { MAP_ATTRIBUTION, MAP_TILE_URL } from "../config";
import type { Prediction } from "../types";
import { LayersIcon, LocateIcon } from "./Icons";

interface ResultMapProps {
  prediction: Prediction | null;
  coverageVisible: boolean;
  onCoverageToggle: (visible: boolean) => void;
}

const MOSCOW_CENTER: [number, number] = [37.6173, 55.7558];
const MOSCOW_ZOOM = 10.2;
const COVERAGE_SOURCE = "production-coverage";

maplibregl.setWorkerUrl(maplibreWorkerUrl);

const MAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    tiles: {
      type: "raster",
      tiles: [MAP_TILE_URL],
      tileSize: 256,
      maxzoom: 19,
      attribution: MAP_ATTRIBUTION,
    },
  },
  layers: [{ id: "tiles", type: "raster", source: "tiles" }],
};

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === "function" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

export function ResultMap({ prediction, coverageVisible, onCoverageToggle }: ResultMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const markerRef = useRef<maplibregl.Marker | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const [mapReady, setMapReady] = useState(false);
  const [mapUnavailable, setMapUnavailable] = useState(false);
  const [coverage, setCoverage] = useState<CoveragePayload | null>(null);
  const [coverageError, setCoverageError] = useState(false);

  useEffect(() => {
    if (!containerRef.current) return undefined;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: MOSCOW_CENTER,
      zoom: MOSCOW_ZOOM,
      attributionControl: { compact: false },
      maxPitch: 0,
      dragRotate: false,
      pitchWithRotate: false,
    });
    mapRef.current = map;
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.once("load", () => {
      setMapReady(true);
      setMapUnavailable(false);
    });
    map.on("error", () => setMapUnavailable(true));
    const resizeObserver =
      typeof ResizeObserver === "undefined" ? null : new ResizeObserver(() => map.resize());
    resizeObserver?.observe(containerRef.current);
    return () => {
      resizeObserver?.disconnect();
      markerRef.current?.remove();
      popupRef.current?.remove();
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    markerRef.current?.remove();
    markerRef.current = null;
    if (!map || !prediction) return;
    const markerElement = document.createElement("div");
    markerElement.className = "estimate-marker";
    markerElement.tabIndex = 0;
    markerElement.setAttribute("role", "img");
    markerElement.setAttribute(
      "aria-label",
      `Estimated location ${prediction.lat.toFixed(4)}, ${prediction.lon.toFixed(4)}`,
    );
    markerElement.innerHTML = "<span></span>";
    markerRef.current = new maplibregl.Marker({ element: markerElement, anchor: "center" })
      .setLngLat([prediction.lon, prediction.lat])
      .addTo(map);
    const mobileOffset: [number, number] = window.innerWidth <= 760
      ? [0, -Math.min(280, window.innerHeight * 0.32)]
      : [0, 0];
    const camera = {
      center: [prediction.lon, prediction.lat] as [number, number],
      zoom: 15.4,
      offset: mobileOffset,
    };
    if (prefersReducedMotion()) map.jumpTo(camera);
    else map.easeTo({ ...camera, duration: 1200, essential: false });
  }, [prediction]);

  useEffect(() => {
    if (!coverageVisible || coverage || coverageError) return undefined;
    const controller = new AbortController();
    loadCoverage(controller.signal)
      .then(setCoverage)
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError")) setCoverageError(true);
      });
    return () => controller.abort();
  }, [coverage, coverageError, coverageVisible]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady || !coverage) return undefined;
    let source = map.getSource(COVERAGE_SOURCE) as GeoJSONSource | undefined;
    if (!source) {
      map.addSource(COVERAGE_SOURCE, { type: "geojson", data: coverage.geojson });
      map.addLayer({
        id: "coverage-fill",
        type: "fill",
        source: COVERAGE_SOURCE,
        paint: {
          "fill-color": [
            "match", ["get", "density"],
            "sparse", "#f2cb6c", "medium", "#ef8d5b", "dense", "#db4e46", "#f2cb6c",
          ],
          "fill-opacity": 0.34,
        },
      });
      map.addLayer({
        id: "coverage-outline",
        type: "line",
        source: COVERAGE_SOURCE,
        paint: { "line-color": "#7b332f", "line-width": 0.8, "line-opacity": 0.62 },
      });
      source = map.getSource(COVERAGE_SOURCE) as GeoJSONSource;
    } else {
      source.setData(coverage.geojson);
    }

    const onCellClick = (event: MapMouseEvent) => {
      const feature = map.queryRenderedFeatures(event.point, { layers: ["coverage-fill"] })[0];
      if (!feature?.properties) return;
      popupRef.current?.remove();
      const providers = String(feature.properties.providers)
        .replace("mapillary", "Mapillary")
        .replace("kartaview", "KartaView");
      const content = document.createElement("div");
      content.className = "coverage-popup";
      const title = document.createElement("strong");
      title.textContent = `${Number(feature.properties.references).toLocaleString("en-US")} references`;
      const detail = document.createElement("span");
      detail.textContent = `${providers} · ${String(feature.properties.density)} relative density`;
      content.append(title, detail);
      popupRef.current = new maplibregl.Popup({ closeButton: true, maxWidth: "240px" })
        .setLngLat(event.lngLat)
        .setDOMContent(content)
        .addTo(map);
    };
    const onEnter = () => { map.getCanvas().style.cursor = "pointer"; };
    const onLeave = () => { map.getCanvas().style.cursor = ""; };
    map.on("click", "coverage-fill", onCellClick);
    map.on("mouseenter", "coverage-fill", onEnter);
    map.on("mouseleave", "coverage-fill", onLeave);
    return () => {
      map.off("click", "coverage-fill", onCellClick);
      map.off("mouseenter", "coverage-fill", onEnter);
      map.off("mouseleave", "coverage-fill", onLeave);
    };
  }, [coverage, mapReady]);

  useEffect(() => {
    const map = mapRef.current;
    if (!mapReady || !map?.getLayer("coverage-fill")) return;
    const visibility = coverageVisible ? "visible" : "none";
    map.setLayoutProperty("coverage-fill", "visibility", visibility);
    map.setLayoutProperty("coverage-outline", "visibility", visibility);
    if (!coverageVisible) popupRef.current?.remove();
  }, [coverageVisible, mapReady]);

  function resetMoscow() {
    const map = mapRef.current;
    if (!map) return;
    const camera = { center: MOSCOW_CENTER, zoom: MOSCOW_ZOOM };
    if (prefersReducedMotion()) map.jumpTo(camera);
    else map.easeTo({ ...camera, duration: 700, essential: false });
  }

  return (
    <section className="map-pane" aria-label="Interactive Moscow map">
      <div
        ref={containerRef}
        className="map-canvas"
        data-testid="map-canvas"
        role="application"
        aria-label={prediction
          ? `Map centered on estimated location ${prediction.lat.toFixed(4)}, ${prediction.lon.toFixed(4)}`
          : "Map of Moscow. No estimate yet."}
      />
      <div className="map-actions" aria-label="Map controls">
        <button type="button" className="map-action" onClick={resetMoscow} title="Reset to Moscow">
          <LocateIcon /> <span>Reset Moscow</span>
        </button>
        <button
          type="button"
          className={`map-action ${coverageVisible ? "is-active" : ""}`}
          aria-pressed={coverageVisible}
          onClick={() => onCoverageToggle(!coverageVisible)}
          title="Toggle reference coverage"
        >
          <LayersIcon /> <span>Coverage</span>
        </button>
      </div>
      {coverageVisible && (
        <div className="coverage-legend" role="note">
          <strong>Reference coverage</strong>
          {coverageError ? (
            <span>Coverage data is unavailable.</span>
          ) : coverage ? (
            <>
              <div><i className="density-sparse" />Sparse <i className="density-medium" />Medium <i className="density-dense" />Dense</div>
              <span>{coverage.reference_count.toLocaleString("en-US")} references · coverage ≠ accuracy</span>
            </>
          ) : (
            <span>Loading verified gallery cells…</span>
          )}
        </div>
      )}
      {mapUnavailable && (
        <div className="map-error" role="status">
          Map tiles are unavailable. Result coordinates and map links remain available.
        </div>
      )}
      <span className="sr-only" aria-live="polite">
        {prediction ? `Estimated location shown at ${prediction.lat.toFixed(4)}, ${prediction.lon.toFixed(4)}.` : ""}
      </span>
    </section>
  );
}
