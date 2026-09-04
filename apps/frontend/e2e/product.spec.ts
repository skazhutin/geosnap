import { expect, test, type Page } from "@playwright/test";

const TILE = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M/wHwAEAQH/2p2zWQAAAABJRU5ErkJggg==",
  "base64",
);

function productResponse(status = "ok") {
  return {
    status,
    prediction: status === "ok" ? { lat: 55.751244, lon: 37.618423, confidence: 0.947 } : null,
    hypotheses: [],
    matches: status === "ok" ? [1, 2, 3, 4].map((rank) => ({
      reference_id: `ref-${rank}`,
      source: rank % 2 ? "mapillary" : "kartaview",
      lat: 55.75,
      lon: 37.61,
      retrieval_score: 0.94 - rank / 100,
      verification_score: null,
      thumbnail_url: null,
      attribution: rank % 2 ? "Mapillary contributor" : "KartaView contributor",
      license: "CC BY-SA 4.0",
      source_url: rank % 2
        ? `https://www.mapillary.com/app/?pKey=ref-${rank}`
        : `https://kartaview.org/details/${rank}/1/track-info`,
      license_url: "https://creativecommons.org/licenses/by-sa/4.0/",
    })) : [],
    diagnostics: { total_ms: 1234 },
    message: null,
    request_id: "e2e-request",
  };
}

async function prepare(page: Page, status = "ok") {
  page.on("console", (message) => {
    const text = message.text();
    const isHeadlessDriverNoise = message.type() === "warning" && /GL Driver Message.*GPU stall due to ReadPixels/.test(text);
    if (!isHeadlessDriverNoise && (message.type() === "error" || message.type() === "warning")) {
      throw new Error(`browser console ${message.type()}: ${text}`);
    }
  });
  page.on("pageerror", (error) => {
    throw error;
  });
  await page.route("https://tile.openstreetmap.org/**", (route) => route.fulfill({ status: 200, contentType: "image/png", body: TILE }));
  await page.route("**/api/localize", (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(productResponse(status)) }));
}

async function upload(page: Page) {
  await page.getByLabel("Choose a street photo").setInputFiles({
    name: "street.jpg",
    mimeType: "image/jpeg",
    buffer: Buffer.from("fake-jpeg"),
  });
  await expect(page.getByRole("button", { name: "Replace" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Remove selected photo" })).toBeVisible();
  await page.getByRole("button", { name: "Estimate location" }).click();
}

test("map-first shell is responsive and keeps the map visible", async ({ page }, testInfo) => {
  await prepare(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Find a place from a photo" })).toBeVisible();
  await expect(page.getByTestId("map-canvas")).toBeVisible();
  const panel = page.locator(".side-panel");
  const map = page.locator(".map-pane");
  const panelBox = await panel.boundingBox();
  const mapBox = await map.boundingBox();
  expect(panelBox).not.toBeNull();
  expect(mapBox).not.toBeNull();
  if (testInfo.project.name === "mobile-chromium") {
    expect(panelBox!.y).toBeGreaterThan(250);
    expect(mapBox!.height).toBeGreaterThan(panelBox!.height);
  } else {
    expect(mapBox!.width / (mapBox!.width + panelBox!.width)).toBeGreaterThan(0.64);
  }
});

test("accepted result places one marker and exposes correct actions", async ({ page }, testInfo) => {
  await prepare(page);
  await page.goto("/");
  await upload(page);
  await expect(page.getByRole("heading", { name: "Estimated location" })).toBeVisible();
  await expect(page.locator(".estimate-marker")).toHaveCount(1);
  await expect(page.getByText("55.7512, 37.6184", { exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Google Maps" })).toHaveAttribute("href", /query=55\.751244,37\.618423/);
  await expect(page.getByRole("link", { name: "Yandex Maps" })).toHaveAttribute("href", /ll=37\.618423%2C55\.751244/);
  await expect(page.getByText(/not a probability/i)).toBeVisible();
  await expect(page.getByRole("heading", { name: "Strongest references" })).toBeVisible();
  if (testInfo.project.name === "mobile-chromium") {
    await page.waitForTimeout(1_400);
    const markerBox = await page.locator(".estimate-marker").boundingBox();
    const panelBox = await page.locator(".side-panel").boundingBox();
    expect(markerBox).not.toBeNull();
    expect(panelBox).not.toBeNull();
    expect(markerBox!.y + markerBox!.height).toBeLessThan(panelBox!.y);
  }
  await page.getByRole("button", { name: "Try another photo" }).click();
  await expect(page.getByRole("heading", { name: "Find a place from a photo" })).toBeVisible();
  await expect(page.locator(".estimate-marker")).toHaveCount(0);
});

test("coverage query flag loads the frozen aggregate legend", async ({ page }) => {
  await prepare(page);
  await page.goto("/?coverage=1");
  await expect(page.getByRole("button", { name: "Coverage" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText("20,487 references · coverage ≠ accuracy")).toBeVisible();
  await page.getByRole("button", { name: "Coverage" }).click();
  await expect(page.getByRole("button", { name: "Coverage" })).toHaveAttribute("aria-pressed", "false");
  await expect(page.getByText("20,487 references · coverage ≠ accuracy")).toHaveCount(0);
});

test("required desktop and mobile viewports keep a usable map and panel", async ({ page }) => {
  await prepare(page);
  for (const viewport of [
    { width: 1440, height: 900, mobile: false },
    { width: 1280, height: 800, mobile: false },
    { width: 1024, height: 768, mobile: false },
    { width: 390, height: 844, mobile: true },
    { width: 360, height: 800, mobile: true },
  ]) {
    await page.setViewportSize(viewport);
    await page.goto("/");
    await expect(page.locator(".map-pane")).toBeVisible();
    await expect(page.locator(".side-panel")).toBeVisible();
    const panelBox = await page.locator(".side-panel").boundingBox();
    const mapBox = await page.locator(".map-pane").boundingBox();
    expect(panelBox).not.toBeNull();
    expect(mapBox).not.toBeNull();
    if (viewport.mobile) {
      expect(panelBox!.y).toBeGreaterThan(240);
      expect(mapBox!.height).toBeGreaterThan(panelBox!.height);
    } else {
      expect(mapBox!.width / (mapBox!.width + panelBox!.width)).toBeGreaterThan(0.64);
    }
  }
});

for (const [status, heading] of [
  ["low_confidence", "Not enough evidence"],
  ["out_of_coverage", "Scene not represented"],
] as const) {
  test(`${status} abstains without an authoritative marker`, async ({ page }) => {
    await prepare(page, status);
    await page.goto("/");
    await upload(page);
    await expect(page.getByRole("heading", { name: heading })).toBeVisible();
    await expect(page.locator(".estimate-marker")).toHaveCount(0);
    await expect(page.getByRole("link", { name: "Google Maps" })).toHaveCount(0);
    await expect(page.getByText("No location pin has been placed.")).toBeVisible();
  });
}
