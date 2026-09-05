import { afterEach, describe, expect, it, vi } from "vitest";

import { localizeImage, safeThumbnailUrl } from "./api";

const file = new File(["pixels"], "street.jpg", { type: "image/jpeg" });

function okResponse(): Response {
  return new Response(JSON.stringify({
    status: "ok",
    prediction: { lat: 55.75, lon: 37.61, confidence: 0.95 },
    hypotheses: [],
    matches: [],
    diagnostics: {},
    message: null,
    request_id: "request",
  }), { status: 200, headers: { "Content-Type": "application/json" } });
}

afterEach(() => vi.useRealTimers());

describe("localization API client", () => {
  it("returns the parsed contract", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(okResponse()));
    await expect(localizeImage(file)).resolves.toMatchObject({ response: { status: "ok" } });
  });

  it("classifies malformed JSON", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("not json", { status: 500 })));
    await expect(localizeImage(file)).rejects.toMatchObject({ kind: "malformed" });
  });

  it("classifies a bounded client timeout", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", vi.fn((_url, init) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
    })));
    const request = localizeImage(file, undefined, 50);
    const assertion = expect(request).rejects.toEqual(expect.objectContaining({ kind: "timeout" }));
    await vi.advanceTimersByTimeAsync(51);
    await assertion;
  });

  it("allows only opaque same-origin thumbnail routes", () => {
    expect(safeThumbnailUrl("/thumbnails/ref-1")).toBe("/api/thumbnails/ref-1");
    expect(safeThumbnailUrl("/thumbnails/%2e%2e")).toBeNull();
    expect(safeThumbnailUrl("https://example.test/image?token=secret")).toBeNull();
  });
});
