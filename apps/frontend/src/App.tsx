import {
  type ChangeEvent,
  type FormEvent,
  lazy,
  Suspense,
  useEffect,
  useRef,
  useState,
} from "react";

import { localizeImage, safeThumbnailUrl } from "./api";
import mapillaryLogo from "./assets/mapillary-logo.svg";
import type { ApiStatus, LocalizeResponse, ReferenceMatch } from "./types";

const ResultMap = lazy(() =>
  import("./components/ResultMap").then((module) => ({ default: module.ResultMap })),
);

const SUPPORTED_IMAGE_TYPES = new Set(["image/jpeg", "image/png", "image/webp"]);

const STATUS_COPY: Record<Exclude<ApiStatus, "ok">, { title: string; body: string }> = {
  invalid_image: {
    title: "Изображение не читается",
    body: "Выберите целую фотографию в формате JPEG, PNG или WebP.",
  },
  unsupported_format: {
    title: "Формат не поддерживается",
    body: "GeoSnap принимает фотографии JPEG, PNG и WebP.",
  },
  image_too_large: {
    title: "Файл слишком большой",
    body: "Уменьшите размер изображения и попробуйте ещё раз.",
  },
  model_not_ready: {
    title: "Модель ещё не готова",
    body: "Сервис локализации запускается. Попробуйте немного позже.",
  },
  index_not_ready: {
    title: "Галерея ещё не готова",
    body: "Индекс эталонных фотографий недоступен. Попробуйте немного позже.",
  },
  low_confidence: {
    title: "Недостаточно уверенности",
    body: "Похожие места не образуют надёжную географическую гипотезу. Попробуйте другой ракурс с заметными зданиями или вывесками.",
  },
  out_of_coverage: {
    title: "Вне текущего покрытия",
    body: "Эта сцена недостаточно представлена в текущей эталонной галерее Москвы.",
  },
  internal_error: {
    title: "Не удалось выполнить локализацию",
    body: "Сервис столкнулся с внутренней ошибкой. Повторите попытку позже.",
  },
  rate_limited: {
    title: "Слишком много запросов",
    body: "Подождите немного и повторите попытку.",
  },
  service_overloaded: {
    title: "Сервис занят",
    body: "Очередь локализации заполнена. Попробуйте ещё раз через несколько секунд.",
  },
  gateway_timeout: {
    title: "Превышено время ожидания",
    body: "Локализация заняла слишком много времени. Повторите попытку позже.",
  },
};

function formatBytes(bytes: number): string {
  if (bytes < 1024 * 1024) return `${Math.max(1, Math.round(bytes / 1024))} КБ`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} МБ`;
}

function formatConfidence(value: number): string {
  return value.toFixed(3);
}

function formatCoordinate(value: number): string {
  return value.toFixed(5);
}

function sourceName(source: string): string {
  const known: Record<string, string> = {
    mapillary: "Mapillary",
    kartaview: "KartaView",
  };
  return known[source.toLowerCase()] ?? source;
}

function MatchCard({ match, rank }: { match: ReferenceMatch; rank: number }) {
  const thumbnail = safeThumbnailUrl(match.thumbnail_url);
  const [thumbnailFailed, setThumbnailFailed] = useState(false);
  const isMapillary = match.source.toLowerCase() === "mapillary";
  return (
    <article className="match-card">
      {thumbnail && !thumbnailFailed ? (
        <img
          className="match-thumbnail"
          src={thumbnail}
          alt={`Эталонное изображение ${rank} из ${sourceName(match.source)}`}
          loading="lazy"
          referrerPolicy="no-referrer"
          onError={() => setThumbnailFailed(true)}
        />
      ) : (
        <div className="match-placeholder" aria-hidden="true">
          {String(rank).padStart(2, "0")}
        </div>
      )}
      <div className="match-copy">
        <div className="match-heading">
          {isMapillary ? (
            <a
              className="mapillary-brand"
              href={match.source_url}
              target="_blank"
              rel="noreferrer"
              aria-label="Открыть снимок в Mapillary"
            >
              <img src={mapillaryLogo} alt="" aria-hidden="true" />
              <span>Mapillary</span>
            </a>
          ) : (
            <span>{sourceName(match.source)}</span>
          )}
          <span className="score">score {match.retrieval_score.toFixed(3)}</span>
        </div>
        <p>
          {formatCoordinate(match.lat)}, {formatCoordinate(match.lon)}
        </p>
        <small>{match.attribution}</small>
        {thumbnail && !thumbnailFailed && (
          <small className="image-change-note">Миниатюра уменьшена GeoSnap.</small>
        )}
        <div className="attribution-links">
          <a href={match.source_url} target="_blank" rel="noreferrer">
            Снимок
          </a>
          {match.contributor_url && (
            <a href={match.contributor_url} target="_blank" rel="noreferrer">
              Автор
            </a>
          )}
          {match.license_url ? (
            <a href={match.license_url} target="_blank" rel="noreferrer">
              {match.license}
            </a>
          ) : (
            <span>{match.license}</span>
          )}
        </div>
      </div>
    </article>
  );
}

function ResultDetails({ result }: { result: LocalizeResponse }) {
  const prediction = result.status === "ok" ? result.prediction : null;
  const elapsed = result.diagnostics.total_ms;

  return (
    <section className="results" aria-live="polite">
      {prediction ? (
        <>
          <div className="result-title-row">
            <div>
              <p className="eyebrow success-label">Гипотеза найдена</p>
              <h2>Предсказанная точка</h2>
            </div>
            <div className="confidence" aria-label={`Оценка свидетельств ${formatConfidence(prediction.confidence)}`}>
              <strong>{formatConfidence(prediction.confidence)}</strong>
              <span>оценка, не вероятность</span>
            </div>
          </div>

          <Suspense fallback={<div className="map-loading" role="status">Загружаем карту…</div>}>
            <ResultMap prediction={prediction} />
          </Suspense>

          <div className="coordinate-strip">
            <div>
              <span>Широта</span>
              <strong>{formatCoordinate(prediction.lat)}</strong>
            </div>
            <div>
              <span>Долгота</span>
              <strong>{formatCoordinate(prediction.lon)}</strong>
            </div>
            <div>
              <span>Неопределённость</span>
              <strong>
                {prediction.uncertainty_radius_m
                  ? `± ${Math.round(prediction.uncertainty_radius_m)} м`
                  : "не рассчитана"}
              </strong>
            </div>
          </div>
        </>
      ) : (
        <div className={`status-card status-${result.status}`} role="status">
          <span className="status-symbol" aria-hidden="true">!</span>
          <div>
            <p className="eyebrow">Результат анализа</p>
            <h2>{STATUS_COPY[result.status as Exclude<ApiStatus, "ok">].title}</h2>
            <p>{STATUS_COPY[result.status as Exclude<ApiStatus, "ok">].body}</p>
          </div>
        </div>
      )}

      {result.hypotheses.length > 1 && (
        <div className="hypotheses-block">
          <div className="section-heading">
            <h3>Географические гипотезы</h3>
            <span>ранжированы по согласованности</span>
          </div>
          <ol className="hypotheses-list">
            {result.hypotheses.slice(0, 3).map((hypothesis, index) => (
              <li key={`${hypothesis.lat}-${hypothesis.lon}-${index}`}>
                <span>{index + 1}</span>
                <strong>
                  {formatCoordinate(hypothesis.lat)}, {formatCoordinate(hypothesis.lon)}
                </strong>
                <small>{formatConfidence(hypothesis.score)}</small>
              </li>
            ))}
          </ol>
        </div>
      )}

      {result.matches.length > 0 && (
        <div className="matches-block">
          <div className="section-heading">
            <h3>Ближайшие эталоны</h3>
            <span>показано {Math.min(result.matches.length, 6)}</span>
          </div>
          <div className="matches-grid">
            {result.matches.slice(0, 6).map((match, index) => (
              <MatchCard key={`${match.source}-${match.reference_id}`} match={match} rank={index + 1} />
            ))}
          </div>
          <p className="imagery-note">
            Авторство каждого эталонного изображения указано непосредственно в карточке источника.
          </p>
        </div>
      )}

      {typeof elapsed === "number" && (
        <p className="timing">Обработано за {Math.max(1, Math.round(elapsed))} мс</p>
      )}
    </section>
  );
}

function App() {
  const [file, setFile] = useState<File | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<LocalizeResponse | null>(null);
  const [clientError, setClientError] = useState<string | null>(null);
  const activeRequest = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!file) {
      setPreviewUrl(null);
      return undefined;
    }
    const objectUrl = URL.createObjectURL(file);
    setPreviewUrl(objectUrl);
    return () => URL.revokeObjectURL(objectUrl);
  }, [file]);

  useEffect(
    () => () => {
      activeRequest.current?.abort();
    },
    [],
  );

  function chooseFile(selected: File | undefined) {
    activeRequest.current?.abort();
    setResult(null);
    setClientError(null);
    setIsLoading(false);

    if (!selected) {
      setFile(null);
      return;
    }
    if (!SUPPORTED_IMAGE_TYPES.has(selected.type)) {
      setFile(null);
      setClientError(STATUS_COPY.unsupported_format.body);
      return;
    }
    setFile(selected);
  }

  function onFileChange(event: ChangeEvent<HTMLInputElement>) {
    const selected = event.currentTarget.files?.[0];
    chooseFile(selected);
    if (selected && !SUPPORTED_IMAGE_TYPES.has(selected.type)) {
      event.currentTarget.value = "";
    }
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file || isLoading) return;

    const controller = new AbortController();
    activeRequest.current?.abort();
    activeRequest.current = controller;
    setClientError(null);
    setResult(null);
    setIsLoading(true);

    try {
      const response = await localizeImage(file, controller.signal);
      if (activeRequest.current === controller) setResult(response);
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (activeRequest.current === controller) {
        setClientError(
          error instanceof Error && error.message === "api_unreachable"
            ? "Не удалось связаться с API GeoSnap. Проверьте, что backend запущен, и повторите попытку."
            : "Сервис вернул некорректный ответ. Повторите попытку позже.",
        );
      }
    } finally {
      if (activeRequest.current === controller) {
        activeRequest.current = null;
        setIsLoading(false);
      }
    }
  }

  return (
    <div className="app-shell">
      <header className="site-header">
        <a className="brand" href="#top" aria-label="GeoSnap — на главную">
          <span className="brand-mark" aria-hidden="true"><i /></span>
          <span>GeoSnap</span>
        </a>
        <span className="scope-badge">Москва · visual retrieval</span>
      </header>

      <main id="top" className="page">
        <section className="intro">
          <p className="eyebrow">Визуальная геолокация</p>
          <h1>Где сделан<br />этот снимок?</h1>
          <p className="lede">
            Загрузите уличную фотографию. GeoSnap сравнит её с геопривязанной галереей и покажет только подтверждённую гипотезу.
          </p>
          <div className="method-note">
            <span aria-hidden="true">01</span>
            <p>Галерея частично покрывает отдельные зоны Москвы. При слабых совпадениях сервис честно вернёт неопределённый результат.</p>
          </div>
        </section>

        <section className="workspace" aria-busy={isLoading}>
          <form className="upload-card" onSubmit={onSubmit}>
            <div className="upload-heading">
              <div>
                <p className="eyebrow">Новый запрос</p>
                <h2>Выберите фотографию</h2>
              </div>
              <span className="step-count">JPEG · PNG · WebP</span>
            </div>

            <label className={`drop-zone ${previewUrl ? "has-preview" : ""}`}>
              <input
                type="file"
                name="image"
                aria-label="Выберите фотографию"
                accept="image/jpeg,image/png,image/webp"
                onChange={onFileChange}
                disabled={isLoading}
              />
              {previewUrl ? (
                <img className="query-preview" src={previewUrl} alt="Предпросмотр выбранной фотографии" />
              ) : (
                <span className="upload-glyph" aria-hidden="true">＋</span>
              )}
              <span className="drop-copy">
                <strong>{file ? "Заменить снимок" : "Нажмите, чтобы выбрать снимок"}</strong>
                <small>{file ? `${file.name} · ${formatBytes(file.size)}` : "Один файл с уличной сценой"}</small>
              </span>
            </label>

            {clientError && (
              <div className="inline-error" role="alert">
                <span aria-hidden="true">!</span>
                <p>{clientError}</p>
              </div>
            )}

            <button className="submit-button" type="submit" disabled={!file || isLoading}>
              {isLoading ? (
                <>
                  <span className="spinner" aria-hidden="true" />
                  Локализуем снимок…
                </>
              ) : (
                <>
                  Найти место
                  <span aria-hidden="true">↗</span>
                </>
              )}
            </button>
            <p className="form-footnote">
              Анализируется содержание кадра, GPS из EXIF не используется. Фото по умолчанию не сохраняется постоянно; оценка может быть ошибочной.
            </p>
          </form>

          {isLoading && (
            <div className="processing-card" role="status" aria-live="polite">
              <div className="processing-visual" aria-hidden="true"><span /></div>
              <div>
                <p className="eyebrow">Обработка</p>
                <h2>Сравниваем визуальные признаки</h2>
                <p>Строим эмбеддинг, ищем похожие эталоны и проверяем географическую согласованность.</p>
              </div>
            </div>
          )}

          {result && <ResultDetails result={result} />}
        </section>
      </main>

      <footer className="site-footer">
        <span>GeoSnap</span>
        <p>Retrieval-based geolocation · Источники снимков указываются в результатах</p>
      </footer>
    </div>
  );
}

export default App;
