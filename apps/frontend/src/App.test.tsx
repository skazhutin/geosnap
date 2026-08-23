import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

vi.mock("./components/ResultMap", () => ({
  ResultMap: ({ prediction }: { prediction: { lat: number; lon: number } }) => (
    <div data-testid="result-map">{prediction.lat},{prediction.lon}</div>
  ),
}));

function apiResponse(overrides: Record<string, unknown> = {}) {
  return {
    status: "ok",
    prediction: {
      lat: 55.751244,
      lon: 37.618423,
      confidence: 0.91,
      uncertainty_radius_m: 85,
    },
    hypotheses: [{ lat: 55.751244, lon: 37.618423, score: 0.91 }],
    matches: [],
    diagnostics: { total_ms: 123.4 },
    message: null,
    request_id: "req-1",
    ...overrides,
  };
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("GeoSnap frontend", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  it("provides an accessible image upload and keeps localization disabled until selection", async () => {
    const user = userEvent.setup();
    render(<App />);

    const input = screen.getByLabelText(/выберите фотографию/i);
    const submit = screen.getByRole("button", { name: /найти место/i });
    expect(input).toHaveAttribute("accept", "image/jpeg,image/png,image/webp");
    expect(submit).toBeDisabled();

    await user.upload(input, new File(["pixels"], "street.jpg", { type: "image/jpeg" }));

    expect(await screen.findByAltText(/предпросмотр выбранной фотографии/i)).toBeInTheDocument();
    expect(screen.getByText(/street.jpg/i)).toBeInTheDocument();
    expect(submit).toBeEnabled();
  });

  it("posts multipart data and renders prediction, uncertainty and only safe thumbnails", async () => {
    const fetchMock = vi.mocked(fetch);
    fetchMock.mockResolvedValue(
      jsonResponse(
        apiResponse({
          matches: [
            {
              reference_id: "ref-1",
              source: "mapillary",
              lat: 55.75,
              lon: 37.61,
              retrieval_score: 0.884,
              verification_score: 0.72,
              thumbnail_url: "/thumbnails/ref-1",
              attribution: "Mapillary contributor",
              license: "CC BY-SA 4.0",
              source_url: "https://www.mapillary.com/app/?pKey=ref-1",
              license_url: "https://creativecommons.org/licenses/by-sa/4.0/",
              contributor_url: "https://www.mapillary.com/app/user/contributor",
            },
            {
              reference_id: "ref-2",
              source: "kartaview",
              lat: 55.76,
              lon: 37.62,
              retrieval_score: 0.79,
              verification_score: null,
              thumbnail_url: "https://example.test/signed?token=secret",
              attribution: "KartaView contributor",
              license: "CC BY-SA 4.0",
              source_url: "https://kartaview.org/details/1/1/track-info",
              license_url: "https://creativecommons.org/licenses/by-sa/4.0/",
            },
          ],
        }),
      ),
    );
    const user = userEvent.setup();
    render(<App />);

    await user.upload(
      screen.getByLabelText(/выберите фотографию/i),
      new File(["pixels"], "street.jpg", { type: "image/jpeg" }),
    );
    await user.click(screen.getByRole("button", { name: /найти место/i }));

    expect(await screen.findByRole("heading", { name: /предсказанная точка/i })).toBeInTheDocument();
    expect(screen.getByTestId("result-map")).toHaveTextContent("55.751244,37.618423");
    expect(screen.getByText(/91\s*%/)).toBeInTheDocument();
    expect(screen.getByText("± 85 м")).toBeInTheDocument();
    expect(screen.getByAltText(/эталонное изображение 1/i)).toHaveAttribute(
      "src",
      "http://localhost:8000/thumbnails/ref-1",
    );
    expect(screen.getAllByRole("img")).toHaveLength(2);
    expect(screen.getAllByRole("link", { name: "Снимок" })[0]).toHaveAttribute(
      "href",
      "https://www.mapillary.com/app/?pKey=ref-1",
    );
    expect(screen.getByRole("link", { name: "Открыть снимок в Mapillary" })).toHaveAttribute(
      "href",
      "https://www.mapillary.com/app/?pKey=ref-1",
    );
    expect(screen.getByRole("link", { name: "Автор" })).toHaveAttribute(
      "href",
      "https://www.mapillary.com/app/user/contributor",
    );
    expect(screen.getAllByRole("link", { name: "CC BY-SA 4.0" })).toHaveLength(2);

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:8000/localize",
      expect.objectContaining({ method: "POST", body: expect.any(FormData) }),
    );
  });

  it("shows the processing state while the API request is pending", async () => {
    let resolveRequest: ((response: Response) => void) | undefined;
    vi.mocked(fetch).mockImplementation(
      () => new Promise<Response>((resolve) => { resolveRequest = resolve; }),
    );
    const user = userEvent.setup();
    render(<App />);

    await user.upload(
      screen.getByLabelText(/выберите фотографию/i),
      new File(["pixels"], "street.jpg", { type: "image/jpeg" }),
    );
    await user.click(screen.getByRole("button", { name: /найти место/i }));

    expect(screen.getByRole("status")).toHaveTextContent(/сравниваем визуальные признаки/i);
    expect(screen.getByRole("button", { name: /локализуем снимок/i })).toBeDisabled();

    resolveRequest?.(jsonResponse(apiResponse()));
    await waitFor(() => expect(screen.queryByText(/сравниваем визуальные признаки/i)).not.toBeInTheDocument());
  });

  it.each([
    ["low_confidence", "Недостаточно уверенности"],
    ["out_of_coverage", "Вне текущего покрытия"],
    ["index_not_ready", "Галерея ещё не готова"],
  ])("renders an explicit %s state", async (status, title) => {
    vi.mocked(fetch).mockResolvedValue(
      jsonResponse(
        apiResponse({
          status,
          prediction: null,
          hypotheses: [],
        }),
        status === "index_not_ready" ? 503 : 200,
      ),
    );
    const user = userEvent.setup();
    render(<App />);

    await user.upload(
      screen.getByLabelText(/выберите фотографию/i),
      new File(["pixels"], "street.jpg", { type: "image/jpeg" }),
    );
    await user.click(screen.getByRole("button", { name: /найти место/i }));

    expect(await screen.findByRole("heading", { name: title })).toBeInTheDocument();
    expect(screen.queryByTestId("result-map")).not.toBeInTheDocument();
  });

  it("rejects unsupported files before making a request", () => {
    render(<App />);
    const input = screen.getByLabelText(/выберите фотографию/i);

    fireEvent.change(input, {
      target: { files: [new File(["text"], "notes.txt", { type: "text/plain" })] },
    });

    expect(screen.getByRole("alert")).toHaveTextContent(/принимает фотографии JPEG, PNG и WebP/i);
    expect(fetch).not.toHaveBeenCalled();
  });
});
