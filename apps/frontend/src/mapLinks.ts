function coordinate(value: number): string {
  if (!Number.isFinite(value)) throw new Error("invalid_coordinate");
  return value.toFixed(6);
}

export function googleMapsUrl(lat: number, lon: number): string {
  return `https://www.google.com/maps/search/?api=1&query=${coordinate(lat)},${coordinate(lon)}`;
}

export function yandexMapsUrl(lat: number, lon: number): string {
  const point = `${coordinate(lon)},${coordinate(lat)}`;
  const query = new URLSearchParams({ ll: point, z: "16", pt: `${point},pm2rdm` });
  return `https://yandex.com/maps/?${query.toString()}`;
}
