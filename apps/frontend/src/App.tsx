import {
  type ChangeEvent,
  type DragEvent,
  type FormEvent,
  lazy,
  Suspense,
  useEffect,
  useRef,
  useState,
} from "react";

import { ApiClientError, localizeImage, safeThumbnailUrl } from "./api";
import { TELEGRAM_BOT_URL } from "./config";
import {
  AlertIcon,
  CheckIcon,
  ChevronIcon,
  CloseIcon,
  CopyIcon,
  ExternalIcon,
  ImageIcon,
  TelegramIcon,
  UploadIcon,
} from "./components/Icons";
import { googleMapsUrl, yandexMapsUrl } from "./mapLinks";
import type { ApiStatus, LocalizeResponse, Prediction, ReferenceMatch } from "./types";

const ResultMap = lazy(() =>
  import("./components/ResultMap").then((module) => ({ default: module.ResultMap })),
);

const MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
const SUPPORTED_IMAGE_TYPES = new Set(["image/jpeg", "image/png", "image/webp"]);

type ErrorCode = Exclude<ApiStatus, "ok" | "low_confidence" | "out_of_coverage"> |
  "api_unreachable" | "api_timeout" | "malformed_response";

type UiState =
  | { phase: "idle" }
  | { phase: "selected"; file: File }
  | { phase: "processing"; file: File }
  | { phase: "ok"; file: File; result: LocalizeResponse }
  | { phase: "low_confidence"; file: File; result: LocalizeResponse }
  | { phase: "out_of_coverage"; file: File; result: LocalizeResponse }
  | { phase: "error"; file: File | null; fileName?: string; code: ErrorCode; retryAfter: number | null };

const ERROR_COPY: Record<ErrorCode, { title: string; body: string; retryable: boolean }> = {
  invalid_image: {
    title: "We couldn’t read that image",
    body: "Choose an intact JPEG, PNG or WebP street photo.",
    retryable: false,
  },
  unsupported_format: {
    title: "Unsupported file format",
    body: "GeoSnap accepts JPEG, PNG and WebP images.",
    retryable: false,
  },
  image_too_large: {
    title: "Image is too large",
    body: "Choose a file no larger than 10 MiB.",
    retryable: false,
  },
  model_not_ready: {
    title: "GeoSnap is starting",
    body: "The localization model is not ready yet. Try again shortly.",
    retryable: true,
  },
  index_not_ready: {
    title: "Reference gallery is starting",
    body: "The production index is not ready yet. Try again shortly.",
    retryable: true,
  },
  internal_error: {
    title: "Localization failed",
    body: "GeoSnap hit an internal error. Your photo remains here so you can retry.",
    retryable: true,
  },
  rate_limited: {
    title: "Too many requests",
    body: "GeoSnap is protecting localization capacity. Wait, then retry this photo.",
    retryable: true,
  },
  service_overloaded: {
    title: "GeoSnap is busy",
    body: "The localization queue is full. Try this photo again in a few seconds.",
    retryable: true,
  },
  gateway_timeout: {
    title: "Localization timed out",
    body: "The request took too long. Try this photo again.",
    retryable: true,
  },
  api_unreachable: {
    title: "GeoSnap is unavailable",
    body: "The website cannot reach the localization service. Try again shortly.",
    retryable: true,
  },
  api_timeout: {
    title: "Localization timed out",
    body: "The service did not answer in time. Try this photo again.",
    retryable: true,
  },
  malformed_response: {
    title: "Unexpected service response",
    body: "GeoSnap returned an invalid response. Try again later.",
    retryable: true,
  },
};

function stateFile(state: UiState): File | null {
  return "file" in state ? state.file : null;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024 * 1024) return `${Math.max(1, Math.round(bytes / 1024))} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatCoordinate(value: number): string {
  return value.toFixed(4);
}

function sourceName(source: string): string {
  const known: Record<string, string> = { mapillary: "Mapillary", kartaview: "KartaView" };
  return known[source.toLowerCase()] ?? source;
}

function MatchCard({ match, rank }: { match: ReferenceMatch; rank: number }) {
  const thumbnail = safeThumbnailUrl(match.thumbnail_url);
  const [thumbnailFailed, setThumbnailFailed] = useState(false);
  return (
    <article className="match-card">
      {thumbnail && !thumbnailFailed ? (
        <img
          className="match-thumbnail"
          src={thumbnail}
          alt={`Reference ${rank} from ${sourceName(match.source)}`}
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => setThumbnailFailed(true)}
        />
      ) : (
        <div className="match-placeholder" aria-hidden="true"><ImageIcon /></div>
      )}
      <div className="match-copy">
        <div className="match-heading">
          <strong>#{rank} · {sourceName(match.source)}</strong>
          <span>{match.retrieval_score.toFixed(3)}</span>
        </div>
        <p>{match.attribution}</p>
        <div className="match-links">
          <a href={match.source_url} target="_blank" rel="noreferrer">View source <ExternalIcon /></a>
          {match.license_url ? (
            <a href={match.license_url} target="_blank" rel="noreferrer">{match.license}</a>
          ) : <span>{match.license}</span>}
        </div>
      </div>
    </article>
  );
}

function PhotoSummary({ file, previewUrl, onReplace, onRemove }: {
  file: File;
  previewUrl: string | null;
  onReplace: () => void;
  onRemove: () => void;
}) {
  return (
    <div className="photo-summary">
      {previewUrl ? <img src={previewUrl} alt="Selected street photo preview" /> : <ImageIcon />}
      <div><strong>{file.name}</strong><span>{formatBytes(file.size)}</span></div>
      <button type="button" onClick={onReplace}>Replace</button>
      <button type="button" className="icon-button" onClick={onRemove} aria-label="Remove selected photo"><CloseIcon /></button>
    </div>
  );
}

function SuccessResult({ result }: { result: LocalizeResponse }) {
  const prediction = result.prediction as Prediction;
  const [copied, setCopied] = useState(false);
  const [showAll, setShowAll] = useState(false);
  const visibleMatches = showAll ? result.matches : result.matches.slice(0, 3);
  const coordinates = `${formatCoordinate(prediction.lat)}, ${formatCoordinate(prediction.lon)}`;

  async function copyCoordinates() {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(coordinates);
      } else {
        const text = document.createElement("textarea");
        text.value = coordinates;
        text.setAttribute("readonly", "");
        text.style.position = "fixed";
        text.style.opacity = "0";
        document.body.appendChild(text);
        text.select();
        const copied = document.execCommand("copy");
        text.remove();
        if (!copied) throw new Error("clipboard_unavailable");
      }
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch {
      setCopied(false);
    }
  }

  return (
    <section className="result-block" aria-labelledby="result-title">
      <div className="result-heading">
        <span className="result-icon"><CheckIcon /></span>
        <div>
          <p className="eyebrow success">Strong visual agreement</p>
          <h2 id="result-title" tabIndex={-1}>Estimated location</h2>
        </div>
      </div>
      <p className="evidence-copy">The accepted result passed GeoSnap’s evidence policy. This is a ranking signal, not a probability.</p>
      <div className="coordinate-row">
        <code>{coordinates}</code>
        <button type="button" onClick={copyCoordinates} aria-label="Copy coordinates"><CopyIcon /> {copied ? "Copied" : "Copy"}</button>
      </div>
      <div className="external-actions">
        <a href={googleMapsUrl(prediction.lat, prediction.lon)} target="_blank" rel="noreferrer">Google Maps <ExternalIcon /></a>
        <a href={yandexMapsUrl(prediction.lat, prediction.lon)} target="_blank" rel="noreferrer">Yandex Maps <ExternalIcon /></a>
      </div>
      {result.matches.length > 0 && (
        <section className="matches-block" aria-labelledby="matches-title">
          <div className="section-heading"><h3 id="matches-title">Strongest references</h3><span>{result.matches.length} returned</span></div>
          <div className="matches-list">{visibleMatches.map((match, index) => <MatchCard key={`${match.source}-${match.reference_id}`} match={match} rank={index + 1} />)}</div>
          {result.matches.length > 3 && (
            <button className="show-more" type="button" onClick={() => setShowAll((value) => !value)} aria-expanded={showAll}>
              {showAll ? "Show fewer" : `Show ${result.matches.length - 3} more`} <ChevronIcon />
            </button>
          )}
          <p className="source-note">Reference imagery attribution and licenses are attached to each source.</p>
        </section>
      )}
      {typeof result.diagnostics.total_ms === "number" && <p className="timing">Processed in {(result.diagnostics.total_ms / 1000).toFixed(1)} s</p>}
    </section>
  );
}

function AbstentionResult({ phase }: { phase: "low_confidence" | "out_of_coverage" }) {
  const copy = phase === "low_confidence"
    ? {
        label: "Evidence below acceptance threshold",
        title: "Not enough evidence",
        body: "Potential matches were found, but they do not support a reliable location. Try another angle with distinctive buildings, signs or street structure.",
      }
    : {
        label: "Reference coverage gap",
        title: "Scene not represented",
        body: "This scene is not sufficiently represented by GeoSnap’s current Moscow reference gallery. The photo itself may still be valid.",
      };
  return (
    <section className="status-block abstention" aria-labelledby="status-title">
      <span className="status-icon"><AlertIcon /></span>
      <div>
        <p className="eyebrow">{copy.label}</p>
        <h2 id="status-title" tabIndex={-1}>{copy.title}</h2>
        <p>{copy.body}</p>
        <strong>No location pin has been placed.</strong>
      </div>
    </section>
  );
}

function ErrorResult({ state, onRetry }: {
  state: Extract<UiState, { phase: "error" }>;
  onRetry: () => void;
}) {
  const copy = ERROR_COPY[state.code];
  return (
    <section className="status-block error" role="alert" aria-labelledby="error-title">
      <span className="status-icon"><AlertIcon /></span>
      <div>
        <p className="eyebrow">Request not completed</p>
        <h2 id="error-title" tabIndex={-1}>{copy.title}</h2>
        <p>{copy.body}</p>
        {state.code === "rate_limited" && state.retryAfter !== null && <strong>Retry after about {state.retryAfter} seconds.</strong>}
        {copy.retryable && state.file && <button type="button" className="text-button" onClick={onRetry}>Retry this photo</button>}
      </div>
    </section>
  );
}

function App() {
  const [ui, setUi] = useState<UiState>({ phase: "idle" });
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [coverageVisible, setCoverageVisible] = useState(() => new URLSearchParams(window.location.search).get("coverage") === "1");
  const fileInput = useRef<HTMLInputElement>(null);
  const activeRequest = useRef<AbortController | null>(null);
  const requestSequence = useRef(0);
  const file = stateFile(ui);

  useEffect(() => {
    if (!file || !SUPPORTED_IMAGE_TYPES.has(file.type)) {
      setPreviewUrl(null);
      return undefined;
    }
    const objectUrl = URL.createObjectURL(file);
    setPreviewUrl(objectUrl);
    return () => URL.revokeObjectURL(objectUrl);
  }, [file]);

  useEffect(() => () => activeRequest.current?.abort(), []);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (coverageVisible) url.searchParams.set("coverage", "1");
    else url.searchParams.delete("coverage");
    window.history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
  }, [coverageVisible]);

  useEffect(() => {
    if (ui.phase === "ok") document.getElementById("result-title")?.focus();
    else if (ui.phase === "low_confidence" || ui.phase === "out_of_coverage") {
      document.getElementById("status-title")?.focus();
    } else if (ui.phase === "error") document.getElementById("error-title")?.focus();
  }, [ui.phase]);

  function resetInput() {
    if (fileInput.current) fileInput.current.value = "";
  }

  function chooseFile(selected: File | undefined) {
    activeRequest.current?.abort();
    requestSequence.current += 1;
    setIsDragging(false);
    if (!selected) {
      resetInput();
      setUi({ phase: "idle" });
      return;
    }
    if (!SUPPORTED_IMAGE_TYPES.has(selected.type)) {
      resetInput();
      setUi({ phase: "error", file: null, fileName: selected.name, code: "unsupported_format", retryAfter: null });
      return;
    }
    if (selected.size > MAX_UPLOAD_BYTES) {
      setUi({ phase: "error", file: selected, code: "image_too_large", retryAfter: null });
      return;
    }
    setUi({ phase: "selected", file: selected });
  }

  function onFileChange(event: ChangeEvent<HTMLInputElement>) {
    chooseFile(event.currentTarget.files?.[0]);
  }

  async function submitFile(selected: File) {
    const controller = new AbortController();
    activeRequest.current?.abort();
    activeRequest.current = controller;
    const sequence = ++requestSequence.current;
    setUi({ phase: "processing", file: selected });
    try {
      const outcome = await localizeImage(selected, controller.signal);
      if (activeRequest.current !== controller || sequence !== requestSequence.current) return;
      const { response, retryAfterSeconds } = outcome;
      if (response.status === "ok") setUi({ phase: "ok", file: selected, result: response });
      else if (response.status === "low_confidence") setUi({ phase: "low_confidence", file: selected, result: response });
      else if (response.status === "out_of_coverage") setUi({ phase: "out_of_coverage", file: selected, result: response });
      else setUi({ phase: "error", file: selected, code: response.status, retryAfter: retryAfterSeconds });
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (activeRequest.current !== controller || sequence !== requestSequence.current) return;
      const code: ErrorCode = error instanceof ApiClientError
        ? error.kind === "timeout" ? "api_timeout" : error.kind === "unreachable" ? "api_unreachable" : "malformed_response"
        : "malformed_response";
      setUi({ phase: "error", file: selected, code, retryAfter: null });
    } finally {
      if (activeRequest.current === controller) activeRequest.current = null;
    }
  }

  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (ui.phase === "selected") void submitFile(ui.file);
  }

  function onDrop(event: DragEvent<HTMLElement>) {
    event.preventDefault();
    chooseFile(event.dataTransfer.files?.[0]);
  }

  function openFilePicker() {
    fileInput.current?.click();
  }

  function removePhoto() {
    activeRequest.current?.abort();
    requestSequence.current += 1;
    resetInput();
    setUi({ phase: "idle" });
  }

  function tryAnotherPhoto() {
    removePhoto();
    openFilePicker();
  }

  const prediction = ui.phase === "ok" ? ui.result.prediction : null;
  const liveMessage = ui.phase === "processing" ? "Photo selected. Localization in progress."
    : ui.phase === "ok" ? "Estimated location is ready and shown on the map."
      : ui.phase === "low_confidence" || ui.phase === "out_of_coverage" ? "GeoSnap abstained and placed no location marker."
        : ui.phase === "error" ? ERROR_COPY[ui.code].title : "";

  return (
    <div className={`app-shell phase-${ui.phase}`}>
      <header className="site-header">
        <a className="brand" href="/" aria-label="GeoSnap home">
          <span className="brand-mark" aria-hidden="true"><i /></span>
          <span>GeoSnap</span>
        </a>
        <span className="scope-badge">Experimental · Moscow</span>
        {TELEGRAM_BOT_URL && <a className="telegram-link" href={TELEGRAM_BOT_URL} target="_blank" rel="noreferrer"><TelegramIcon />Open Telegram</a>}
      </header>

      <main className="workspace">
        <Suspense fallback={<section className="map-pane map-loading" role="status">Loading map…</section>}>
          <ResultMap prediction={prediction} coverageVisible={coverageVisible} onCoverageToggle={setCoverageVisible} />
        </Suspense>

        <aside
          className={`side-panel ${isDragging ? "is-dragging" : ""}`}
          aria-busy={ui.phase === "processing"}
          onDragEnter={(event) => { event.preventDefault(); setIsDragging(true); }}
          onDragOver={(event) => event.preventDefault()}
          onDragLeave={(event) => { if (event.currentTarget === event.target) setIsDragging(false); }}
          onDrop={onDrop}
        >
          <div className="sheet-handle" aria-hidden="true" />
          <div className="panel-scroll">
            <div className="panel-intro">
              <p className="eyebrow">Visual geolocation</p>
              <h1>Find a place from a photo</h1>
              <p>Upload a Moscow street scene. GeoSnap returns a location only when visual evidence is strong enough.</p>
            </div>

            <form className="upload-form" onSubmit={onSubmit}>
              <input ref={fileInput} className="visually-hidden-input" type="file" name="image" aria-label="Choose a street photo" accept="image/jpeg,image/png,image/webp" onChange={onFileChange} />
              {ui.phase === "idle" || (ui.phase === "error" && !ui.file) ? (
                <button className="drop-zone" type="button" onClick={openFilePicker}>
                  <span className="upload-glyph"><UploadIcon /></span>
                  <strong>Drop a photo or choose a file</strong>
                  <span>JPEG, PNG or WebP · up to 10 MiB</span>
                </button>
              ) : file ? (
                <PhotoSummary file={file} previewUrl={previewUrl} onReplace={openFilePicker} onRemove={removePhoto} />
              ) : null}

              {ui.phase === "selected" && (
                <button className="primary-button" type="submit">Estimate location <span aria-hidden="true">→</span></button>
              )}
              {ui.phase === "processing" && (
                <div className="processing-block" role="status">
                  <span className="spinner" aria-hidden="true" />
                  <div><strong>Comparing visual evidence…</strong><span>Embedding the photo, retrieving references and checking geographic agreement.</span></div>
                </div>
              )}
            </form>

            {ui.phase === "ok" && <SuccessResult result={ui.result} />}
            {(ui.phase === "low_confidence" || ui.phase === "out_of_coverage") && <AbstentionResult phase={ui.phase} />}
            {ui.phase === "error" && <ErrorResult state={ui} onRetry={() => ui.file && void submitFile(ui.file)} />}

            {(ui.phase === "ok" || ui.phase === "low_confidence" || ui.phase === "out_of_coverage") && (
              <button className="secondary-button" type="button" onClick={tryAnotherPhoto}>Try another photo</button>
            )}

            <footer className="privacy-note">
              <p>GPS metadata is not used. Photos are processed in memory and are not permanently retained by default.</p>
              <p>Coverage is incomplete and any estimate may be wrong.</p>
            </footer>
          </div>
          {isDragging && <div className="drop-overlay"><UploadIcon /><strong>Drop photo to replace</strong></div>}
        </aside>
      </main>
      <div className="sr-only" aria-live="polite" aria-atomic="true">{liveMessage}</div>
    </div>
  );
}

export default App;
