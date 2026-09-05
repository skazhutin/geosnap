import { describe, expect, it } from "vitest";

import { normalizeTelegramBotUrl } from "./config";

describe("public Telegram CTA configuration", () => {
  it("accepts only a public t.me bot path", () => {
    expect(normalizeTelegramBotUrl("https://t.me/geosnap_bot")).toBe("https://t.me/geosnap_bot");
    expect(normalizeTelegramBotUrl("https://example.test/geosnap_bot")).toBeNull();
    expect(normalizeTelegramBotUrl("https://t.me/a")).toBeNull();
    expect(normalizeTelegramBotUrl(undefined)).toBeNull();
  });
});
