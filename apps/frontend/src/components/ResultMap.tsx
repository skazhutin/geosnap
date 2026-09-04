import { useEffect, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import type { StyleSpecification } from "maplibre-gl";
import type { Feature, Polygon } from "geojson";

import type { Prediction } from "../types";

interface ResultMapProps {
  prediction: Prediction;
}

const EARTH_RADIUS_M = 6_371_008.8;

// Vite rewrites this explicit asset URL in both dev and production builds.
// Without it MapLibre derives a worker URL next to the optimized dependency
// chunk, which is not a real file in the Vite dev server.
maplibregl.setWorkerUrl(maplibreWorkerUrl);

const DEVELOPMENT_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const DEVELOPMENT_ATTRIBUTION = "© OpenStreetMap contributors";
const configuredTileUrl = import.meta.env.VITE_MAP_TILE_URL?.trim() || DEVELOPMENT_TILE_URL;
const tilePublicParameter = import.meta.env.VITE_MAP_PUBLIC_PARAMETER?.trim() || "";
const tileUrl = configuredTileUrl.replace(
  "{token}",
  encodeURIComponent(tilePublicParameter),
);
const tileAttribution = import.meta.env.VITE_MAP_ATTRIBUTION?.trim() || DEVELOPMENT_ATTRIBUTION;

const MAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    tiles: {
      type: "raster",
      tiles: [tileUrl],
      tileSize: 256,
      maxzoom: 19,
      attribution: tileAttribution,
    },
  },
  layers: [
    {
      id: "tiles",
      type: "raster",
      source: "tiles",
    },
  ],
};

function toRadians(value: number): number {
  return (value * Math.PI) / 180;
}

function toDegrees(value: number): number {
  return (value * 180) / Math.PI;
}

function uncertaintyPolygon(
  lat: number,
  lon: number,
  radiusM: number,
  segments = 80,
): Feature<Polygon> {
  const angularDistance = radiusM / EARTH_RADIUS_M;
  const originLat = toRadians(lat);
  const originLon = toRadians(lon);
  const coordinates: [number, number][] = [];

  for (let index = 0; index <= segments; index += 1) {
    const bearing = (index / segments) * Math.PI * 2;
    const targetLat = Math.asin(
      Math.sin(originLat) * Math.cos(angularDistance) +
        Math.cos(originLat) * Math.sin(angularDistance) * Math.cos(bearing),
    );
    const targetLon =
      originLon +
      Math.atan2(
        Math.sin(bearing) * Math.sin(angularDistance) * Math.cos(originLat),
        Math.cos(angularDistance) - Math.sin(originLat) * Math.sin(targetLat),
      );
    const normalizedLon = ((toDegrees(targetLon) + 540) % 360) - 180;
    coordinates.push([normalizedLon, toDegrees(targetLat)]);
  }

  return {
    type: "Feature",
    properties: {},
    geometry: { type: "Polygon", coordinates: [coordinates] },
  };
}

export function ResultMap({ prediction }: ResultMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return undefined;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: [prediction.lon, prediction.lat],
      zoom: prediction.uncertainty_radius_m ? 13 : 15,
      attributionControl: { compact: true },
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    const marker = new maplibregl.Marker({ color: "#ff5a47" })
      .setLngLat([prediction.lon, prediction.lat])
      .addTo(map);
    marker.getElement().setAttribute("aria-label", "Предсказанное местоположение");

    map.once("load", () => {
      const radius = prediction.uncertainty_radius_m;
      if (!radius) return;
      const polygon = uncertaintyPolygon(prediction.lat, prediction.lon, radius);
      map.addSource("uncertainty", { type: "geojson", data: polygon });
      map.addLayer({
        id: "uncertainty-fill",
        type: "fill",
        source: "uncertainty",
        paint: { "fill-color": "#2b67f6", "fill-opacity": 0.16 },
      });
      map.addLayer({
        id: "uncertainty-line",
        type: "line",
        source: "uncertainty",
        paint: { "line-color": "#2b67f6", "line-width": 2 },
      });

      const bounds = new maplibregl.LngLatBounds();
      polygon.geometry.coordinates[0].forEach((point) => bounds.extend([point[0], point[1]]));
      map.fitBounds(bounds, { padding: 72, maxZoom: 16, duration: 0 });
    });

    const resizeObserver =
      typeof ResizeObserver === "undefined" ? null : new ResizeObserver(() => map.resize());
    resizeObserver?.observe(containerRef.current);

    return () => {
      resizeObserver?.disconnect();
      marker.remove();
      map.remove();
    };
  }, [prediction.lat, prediction.lon, prediction.uncertainty_radius_m]);

  const radiusDescription = prediction.uncertainty_radius_m
    ? `, радиус неопределённости ${Math.round(prediction.uncertainty_radius_m)} метров`
    : "";

  return (
    <div className="map-shell">
      <div
        ref={containerRef}
        className="map-canvas"
        role="img"
        aria-label={`Карта с прогнозом ${prediction.lat.toFixed(5)}, ${prediction.lon.toFixed(5)}${radiusDescription}`}
      />
      <p className="map-credit">
        {tileAttribution}
      </p>
    </div>
  );
}
