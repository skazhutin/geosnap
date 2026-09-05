import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

vi.mock("./components/ResultMap", () => ({
  ResultMap: ({ prediction, estimateTier, coverageVisible, onCoverageToggle }: {
    prediction: { lat: number; lon: number } | null;
    estimateTier: "accepted" | "tentative" | null;
    coverageVisible: boolean;
    onCoverageToggle: (value: boolean) => void;
  }) => (
    <div data-testid="map">
      {prediction && <span data-testid="estimate-marker" data-estimate-tier={estimateTier}>{prediction.lat},{prediction.lon}</span>}
      <button type="button" aria-pressed={coverageVisible} onClick={() => onCoverageToggle(!coverageVisible)}>Coverage</button>
    </div>
  ),
}));

function match(rank: number) {
  return {
    reference_id: `ref-${rank}`,
    source: rank % 2 ? "mapillary" : "kartaview",
    lat: 55.75,
    lon: 37.61,
    retrieval_score: 0.9 - rank / 100,
    verification_score: null,
    thumbnail_url: `/thumbnails/ref-${rank}`,
    attribution: rank % 2 ? "Mapillary contributor" : "KartaView contributor",
    license: "CC BY-SA 4.0",
    source_url: rank % 2
      ? `https://www.mapillary.com/app/?pKey=ref-${rank}`
      : `https://kartaview.org/details/${rank}/1/track-info`,
    license_url: "https://creativecommons.org/licenses/by-sa/4.0/",
  };
}

function apiResponse(overrides: Record<string, unknown> = {}) {
  return {
    status: "ok",
    prediction: { lat: 55.751244, lon: 37.618423, confidence: 0.947, uncertainty_radius_m: 85 },
    hypotheses: [],
    matches: [],
    diagnostics: { total_ms: 1234 },
    message: null,
    request_id: "req-1",
    ...overrides,
  };
}

function jsonResponse(payload: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

async function selectPhoto(name = "street.jpg") {
  const user = userEvent.setup();
  await user.upload(
    screen.getByLabelText(/choose a street photo/i),
    new File(["pixels"], name, { type: "image/jpeg" }),
  );
  return user;
}

async function localize(name = "street.jpg") {
  render(<App />);
  const user = await selectPhoto(name);
  await user.click(screen.getByRole("button", { name: /estimate location/i }));
  return user;
}

describe("GeoSnap product UI", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
    window.history.replaceState(null, "", "/");
  });

  it("starts map-first with an accessible bounded upload contract", async () => {
    render(<App />);
    expect(await screen.findByTestId("map")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /find a place from a photo/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/choose a street photo/i)).toHaveAttribute(
      "accept",
      "image/jpeg,image/png,image/webp",
    );
    expect(screen.queryByRole("button", { name: /estimate location/i })).not.toBeInTheDocument();
    expect(screen.getByText(/not permanently retained/i)).toBeInTheDocument();
  });

  it("shows selection, replacement controls and revokes previews on remove", async () => {
    render(<App />);
    const user = await selectPhoto();
    expect(screen.getByAltText(/selected street photo preview/i)).toHaveAttribute("src", "blob:query-preview");
    expect(screen.getByText("street.jpg")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /remove selected photo/i }));
    expect(screen.getByRole("button", { name: /drop a photo/i })).toBeInTheDocument();
    expect(URL.revokeObjectURL).toHaveBeenCalledWith("blob:query-preview");
  });

  it("accepts drag and drop", () => {
    render(<App />);
    const panel = screen.getByRole("complementary");
    fireEvent.drop(panel, { dataTransfer: { files: [new File(["p"], "drop.png", { type: "image/png" })] } });
    expect(screen.getByText("drop.png")).toBeInTheDocument();
  });

  it("renders an accepted result without an uncertainty claim and exposes tested map links", async () => {
    vi.mocked(fetch).mockResolvedValue(jsonResponse(apiResponse({ matches: [1, 2, 3, 4, 5].map(match) })));
    const user = await localize();
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    expect(await screen.findByRole("heading", { name: /estimated location/i })).toBeInTheDocument();
    expect(screen.getByTestId("estimate-marker")).toHaveTextContent("55.751244,37.618423");
    expect(screen.getByTestId("estimate-marker")).toHaveAttribute("data-estimate-tier", "accepted");
    expect(screen.getByText("55.7512, 37.6184")).toBeInTheDocument();
    expect(screen.getByText(/ranking signal, not a probability/i)).toBeInTheDocument();
    expect(screen.queryByText(/uncertainty/i)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: /google maps/i })).toHaveAttribute(
      "href",
      "https://www.google.com/maps/search/?api=1&query=55.751244,37.618423",
    );
    expect(screen.getAllByRole("article")).toHaveLength(3);
    await user.click(screen.getByRole("button", { name: /copy coordinates/i }));
    expect(writeText).toHaveBeenCalledWith("55.7512, 37.6184");
    expect(screen.getByRole("button", { name: /copy coordinates/i })).toHaveTextContent("Copied");
    await user.click(screen.getByRole("button", { name: /show 2 more/i }));
    expect(screen.getAllByRole("article")).toHaveLength(5);
    expect(vi.mocked(fetch).mock.calls[0][0]).toBe("/api/localize");
    expect(vi.mocked(fetch).mock.calls[0][1]).toEqual(expect.objectContaining({ method: "POST", body: expect.any(FormData) }));
  });

  it("shows a compact processing state in the panel", async () => {
    let resolveRequest: ((response: Response) => void) | undefined;
    vi.mocked(fetch).mockImplementation(() => new Promise<Response>((resolve) => { resolveRequest = resolve; }));
    await localize();
    expect(screen.getByRole("status")).toHaveTextContent(/comparing visual evidence/i);
    expect(screen.getByLabelText(/choose a street photo/i)).not.toBeDisabled();
    resolveRequest?.(jsonResponse(apiResponse()));
    expect(await screen.findByRole("heading", { name: /estimated location/i })).toBeInTheDocument();
  });

  it("renders a low-confidence prediction as a distinctly tentative location", async () => {
    vi.mocked(fetch).mockResolvedValue(jsonResponse(apiResponse({
      status: "low_confidence",
      prediction: { lat: 55.701234, lon: 37.665432, confidence: 0.62 },
      matches: [1, 2, 3, 4].map(match),
    })));
    await localize();
    expect(await screen.findByRole("heading", { name: "Tentative location" })).toBeInTheDocument();
    expect(screen.getByText(/below the acceptance threshold/i)).toBeInTheDocument();
    expect(screen.getByText(/may be significantly wrong/i)).toBeInTheDocument();
    expect(screen.getByText("Tentative coordinates")).toBeInTheDocument();
    expect(screen.getByText("55.7012, 37.6654")).toBeInTheDocument();
    expect(screen.getByTestId("estimate-marker")).toHaveAttribute("data-estimate-tier", "tentative");
    expect(screen.getByRole("link", { name: /open tentative point in google maps/i })).toHaveAttribute(
      "href",
      "https://www.google.com/maps/search/?api=1&query=55.701234,37.665432",
    );
    expect(screen.getByRole("link", { name: /open tentative point in yandex maps/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Possible visual matches" })).toBeInTheDocument();
    expect(screen.getAllByRole("article")).toHaveLength(3);
    expect(screen.queryByText(/no location pin has been placed/i)).not.toBeInTheDocument();
  });

  it("keeps a safe abstention UI when low confidence has no prediction", async () => {
    vi.mocked(fetch).mockResolvedValue(jsonResponse(apiResponse({ status: "low_confidence", prediction: null })));
    await localize();
    expect(await screen.findByRole("heading", { name: "Not enough evidence" })).toBeInTheDocument();
    expect(screen.getByText(/no location pin has been placed/i)).toBeInTheDocument();
    expect(screen.queryByTestId("estimate-marker")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /google maps/i })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /try another photo/i })).toBeInTheDocument();
  });

  it("keeps out of coverage strict without a prediction or map actions", async () => {
    vi.mocked(fetch).mockResolvedValue(jsonResponse(apiResponse({ status: "out_of_coverage", prediction: null })));
    await localize();
    expect(await screen.findByRole("heading", { name: "Scene not represented" })).toBeInTheDocument();
    expect(screen.getByText(/no location pin has been placed/i)).toBeInTheDocument();
    expect(screen.queryByTestId("estimate-marker")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /google maps/i })).not.toBeInTheDocument();
  });

  it.each([
    ["invalid_image", 422, "We couldn’t read that image"],
    ["model_not_ready", 503, "GeoSnap is starting"],
    ["service_overloaded", 503, "GeoSnap is busy"],
    ["gateway_timeout", 504, "Localization timed out"],
  ])("maps backend %s to a safe error", async (status, httpStatus, title) => {
    vi.mocked(fetch).mockResolvedValue(jsonResponse(apiResponse({ status, prediction: null }), httpStatus));
    await localize();
    expect(await screen.findByRole("heading", { name: title })).toBeInTheDocument();
    expect(screen.queryByTestId("estimate-marker")).not.toBeInTheDocument();
  });

  it("honors Retry-After for a rate-limited request", async () => {
    vi.mocked(fetch)
      .mockResolvedValueOnce(
        jsonResponse(apiResponse({ status: "rate_limited", prediction: null }), 429, { "Retry-After": "12" }),
      )
      .mockResolvedValueOnce(jsonResponse(apiResponse()));
    await localize();
    expect(await screen.findByText(/retry after about 12 seconds/i)).toBeInTheDocument();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /retry this photo/i }));
    expect(await screen.findByRole("heading", { name: /estimated location/i })).toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("reports backend unavailability and preserves the photo", async () => {
    vi.mocked(fetch).mockRejectedValue(new TypeError("network down"));
    await localize();
    expect(await screen.findByRole("heading", { name: /geosnap is unavailable/i })).toBeInTheDocument();
    expect(screen.getByText("street.jpg")).toBeInTheDocument();
  });

  it("rejects unsupported and oversized files before API access", () => {
    render(<App />);
    const input = screen.getByLabelText(/choose a street photo/i);
    fireEvent.change(input, { target: { files: [new File(["text"], "notes.txt", { type: "text/plain" })] } });
    expect(screen.getByRole("heading", { name: /unsupported file format/i })).toBeInTheDocument();
    const oversized = new File([new Uint8Array(10 * 1024 * 1024 + 1)], "huge.webp", { type: "image/webp" });
    fireEvent.change(input, { target: { files: [oversized] } });
    expect(screen.getByRole("heading", { name: /image is too large/i })).toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("aborts stale work so an older response cannot replace a newer photo", async () => {
    let resolveFirst: ((response: Response) => void) | undefined;
    vi.mocked(fetch)
      .mockImplementationOnce((_url, init) => new Promise<Response>((resolve, reject) => {
        resolveFirst = resolve;
        init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      }))
      .mockResolvedValueOnce(jsonResponse(apiResponse({ prediction: { lat: 55.8, lon: 37.7, confidence: 0.95 } })));
    render(<App />);
    const user = await selectPhoto("first.jpg");
    await user.click(screen.getByRole("button", { name: /estimate location/i }));
    await user.upload(screen.getByLabelText(/choose a street photo/i), new File(["second"], "second.jpg", { type: "image/jpeg" }));
    await user.click(screen.getByRole("button", { name: /estimate location/i }));
    expect(await screen.findByText("55.8000, 37.7000")).toBeInTheDocument();
    resolveFirst?.(jsonResponse(apiResponse()));
    await waitFor(() => expect(screen.queryByText("55.7512, 37.6184")).not.toBeInTheDocument());
  });

  it("toggles coverage and reflects the deliberate URL flag", async () => {
    render(<App />);
    const user = userEvent.setup();
    const toggle = await screen.findByRole("button", { name: "Coverage" });
    expect(toggle).toHaveAttribute("aria-pressed", "false");
    await user.click(toggle);
    expect(toggle).toHaveAttribute("aria-pressed", "true");
    expect(window.location.search).toBe("?coverage=1");
  });
});
