FROM node:22.19.0-alpine3.22@sha256:d2166de198f26e17e5a442f537754dd616ab069c47cc57b889310a717e0abbf9 AS frontend-build

WORKDIR /build
COPY apps/frontend/package.json apps/frontend/package-lock.json ./
RUN npm ci
COPY apps/frontend ./

ARG VITE_API_BASE_URL=/api
ARG VITE_MAP_TILE_URL
ARG VITE_MAP_ATTRIBUTION
ARG VITE_MAP_PUBLIC_PARAMETER
ARG VITE_TELEGRAM_BOT_URL
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL} \
    VITE_MAP_TILE_URL=${VITE_MAP_TILE_URL} \
    VITE_MAP_ATTRIBUTION=${VITE_MAP_ATTRIBUTION} \
    VITE_MAP_PUBLIC_PARAMETER=${VITE_MAP_PUBLIC_PARAMETER} \
    VITE_TELEGRAM_BOT_URL=${VITE_TELEGRAM_BOT_URL}
RUN test "${VITE_API_BASE_URL}" = "/api" \
    && test -n "${VITE_MAP_TILE_URL}" \
    && test -n "${VITE_MAP_ATTRIBUTION}" \
    && ! printf '%s' "${VITE_MAP_TILE_URL}" | grep -q 'tile.openstreetmap.org' \
    && npm run build \
    && ! grep -R -E 'https?://(localhost|127\.0\.0\.1)' dist

FROM caddy:2.10.2-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d

RUN addgroup -S -g 10001 geosnap \
    && adduser -S -D -H -u 10001 -G geosnap geosnap \
    && chown -R geosnap:geosnap /data /config /srv /etc/caddy
COPY infra/caddy/Caddyfile /etc/caddy/Caddyfile
COPY --from=frontend-build /build/dist /srv

EXPOSE 80 443 8080
USER geosnap
