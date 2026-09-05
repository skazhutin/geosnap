import { describe, expect, it } from "vitest";

import { googleMapsUrl, yandexMapsUrl } from "./mapLinks";

describe("map links", () => {
  it("keeps latitude/longitude order for Google", () => {
    expect(googleMapsUrl(55.751244, 37.618423)).toBe(
      "https://www.google.com/maps/search/?api=1&query=55.751244,37.618423",
    );
  });

  it("keeps longitude/latitude order for Yandex", () => {
    const url = new URL(yandexMapsUrl(55.751244, 37.618423));
    expect(url.origin).toBe("https://yandex.com");
    expect(url.searchParams.get("ll")).toBe("37.618423,55.751244");
    expect(url.searchParams.get("pt")).toBe("37.618423,55.751244,pm2rdm");
  });
});
