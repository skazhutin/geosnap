const $ = (id) => document.getElementById(id);
const number = new Intl.NumberFormat('ru-RU');
const clock = new Intl.DateTimeFormat('ru-RU', {timeZone:'Europe/Moscow', hour:'2-digit', minute:'2-digit'});
const liveClock = new Intl.DateTimeFormat('ru-RU', {timeZone:'Europe/Moscow', hour:'2-digit', minute:'2-digit', second:'2-digit', fractionalSecondDigits:1});
const dateTime = new Intl.DateTimeFormat('ru-RU', {timeZone:'Europe/Moscow', day:'numeric', month:'short', hour:'2-digit', minute:'2-digit'});
let selectedRun = null;
let selectedChunk = null;
let lastPhotosKey = '';
let lastGridKey = '';
let lastRunsKey = '';
let latestStatus = null;
let paused = localStorage.getItem('geosnap-dashboard-paused') === '1';
const intervals = new Set([500, 1000, 2000, 5000, 10000, 30000, 60000]);
const savedInterval = Number(localStorage.getItem('geosnap-dashboard-interval'));
let refreshInterval = intervals.has(savedInterval) ? savedInterval : 500;
let pollTimer = null;
let activeRequest = null;
let photoRequest = null;

function duration(seconds) {
  if (seconds == null || !Number.isFinite(seconds)) return 'Нет оценки';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.round((seconds % 3600) / 60);
  return hours ? `≈ ${hours} ч ${minutes} мин` : `≈ ${minutes} мин`;
}
function runState(state) {
  return {running:'Идёт', waiting:'Ожидает', stalled:'Нет новых данных', stopped:'Остановлен', complete:'Завершён'}[state] || state;
}
function element(tag, className, value) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (value != null) node.textContent = value;
  return node;
}
function renderRuns(runs) {
  $('run-count').textContent = number.format(runs.length);
  const key = JSON.stringify(runs.map((run) => [run.id, run.title, run.phase, run.state])) + selectedRun;
  if (key === lastRunsKey) return;
  lastRunsKey = key;
  const list = $('run-list');
  list.replaceChildren(...runs.map((run) => {
    const button = element('button', `run-item ${run.state} ${selectedRun === run.id ? 'selected' : ''}`);
    button.type = 'button';
    button.setAttribute('aria-current', selectedRun === run.id ? 'true' : 'false');
    const title = element('strong', '', run.title);
    const meta = element('small');
    meta.append(element('span', 'mini-dot'), document.createTextNode(`${runState(run.state)} · ${run.phase}`));
    button.append(title, meta);
    button.addEventListener('click', () => {selectedRun = run.id; selectedChunk = null; lastPhotosKey = ''; render(latestStatus);});
    return button;
  }));
}
function renderChart(history) {
  const rates = history.map((item) => item.per_second).filter(Number.isFinite);
  if (rates.length < 2) { $('rate-line').setAttribute('points', ''); return; }
  const high = Math.max(...rates, 1), low = Math.min(...rates, 0);
  const points = rates.map((rate, index) => {
    const x = 5 + index * 270 / (rates.length - 1);
    const y = 58 - (rate - low) / Math.max(high - low, .1) * 45;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  $('rate-line').setAttribute('points', points);
}
function renderGrid(grid) {
  $('batch-count').textContent = grid.length ? `${grid.filter((item) => item.state === 'done').length} / ${grid.length}` : '—';
  const key = `${selectedRun}/${selectedChunk}/${grid.length}/${grid.filter((item) => item.state === 'done').length}/${grid.findIndex((item) => item.state === 'working')}`;
  if (key === lastGridKey) return;
  lastGridKey = key;
  $('batch-grid').replaceChildren(...grid.map((item) => {
    const button = element('button', `batch-cell ${item.state} ${selectedChunk === item.index ? 'selected' : ''}`);
    button.type = 'button';
    button.disabled = item.state !== 'done';
    const state = item.state === 'done' ? 'готова' : item.state === 'working' ? 'в работе' : 'ожидает';
    button.title = `Партия ${item.index + 1}: снимки ${number.format(item.start)}–${number.format(item.end)}, ${state}`;
    button.setAttribute('aria-label', button.title);
    if (item.state === 'done') button.addEventListener('click', () => selectChunk(item.index, item));
    return button;
  }));
  $('batches-title').parentElement.parentElement.parentElement.hidden = grid.length === 0;
}
function renderStages(stages) {
  $('stages').replaceChildren(...stages.map((stage) => {
    const row = element('li', stage.done ? 'done' : '');
    const mark = element('span', 'stage-mark', stage.done ? '✓' : '');
    mark.setAttribute('aria-hidden', 'true');
    const content = element('div');
    content.append(element('div', 'stage-name', stage.name), element('div', 'stage-detail', stage.detail || ''));
    row.append(mark, content);
    return row;
  }));
}
function renderPhotos(photos, caption) {
  const key = JSON.stringify(photos.map((item) => item.id)) + caption;
  if (key === lastPhotosKey) return;
  lastPhotosKey = key;
  $('photos-caption').textContent = caption;
  $('photo-empty').hidden = photos.length > 0;
  $('photo-strip').replaceChildren(...photos.map((photo) => {
    const button = element('button', `photo-card ${photo.suspicious ? 'suspicious' : ''}`); button.type = 'button';
    button.setAttribute('aria-label', `Открыть снимок ${photo.id}`);
    const image = element('img'); image.src = photo.image_url; image.alt = `Снимок из ${photo.source || 'галереи'}`; image.loading = 'lazy';
    const label = element('span', '', photo.suspicious ? `${photo.source} · проверить` : photo.source || 'Снимок');
    image.addEventListener('error', () => {
      image.remove();
      button.prepend(element('div', 'photo-fallback', 'Снимок недоступен'));
    }, {once:true});
    button.append(image, label);
    button.addEventListener('click', () => openPhoto(photo));
    return button;
  }));
}
const flagNames = {
  near_black:'Почти чёрный кадр', near_white:'Почти белый кадр', nearly_flat:'Почти без деталей',
  multiple_low_detail_signals:'Несколько признаков сильного размытия',
  source_unavailable:'Файл недоступен', decode_or_measure_error:'Ошибка чтения изображения'
};
const reasonNames = {
  severe_blur:'Сильное размытие', nearly_black:'Почти чёрный кадр', nearly_white:'Почти белый кадр',
  obstructed:'Обзор закрыт', no_street_view:'Нет вида улицы', other:'Другая причина', none:'Причин для исключения не найдено'
};
function addResultSection(title) {
  const section = element('section', 'result-section');
  section.append(element('h3', '', title));
  $('dialog-results').append(section);
  return section;
}
function metric(parent, title, value) {
  const wrapper = element('div');
  wrapper.append(element('dt', '', title), element('dd', '', value));
  parent.append(wrapper);
}
function renderPhotoDetails(data) {
  $('dialog-results').replaceChildren();
  $('dialog-state').textContent = data.scan_status === 'ok' ? 'Измерения этой фотографии сохранены.'
    : data.scan_status === 'not_yet_committed' ? 'Партия ещё не сохранена.'
    : `Снимок требует проверки: ${data.scan_status}.`;
  const technical = addResultSection('Числовая проверка');
  if (data.technical_flags?.length) {
    data.technical_flags.forEach((flag) => technical.append(element('span', 'result-chip flag', flagNames[flag] || flag)));
  } else if (data.features) {
    technical.append(element('p', '', 'Строгие признаки явного брака не сработали. Это не оценка геолокационной полезности.'));
  }
  if (data.features) {
    const f = data.features;
    const grid = element('dl', 'metric-grid');
    metric(grid, 'Размер', `${number.format(f.width)} × ${number.format(f.height)} px`);
    metric(grid, 'Средняя яркость', f.mean_luminance.toFixed(1) + ' / 255');
    metric(grid, 'Тёмные пиксели', (f.dark_fraction * 100).toFixed(1).replace('.', ',') + '%');
    metric(grid, 'Контраст 5–95%', f.p95_minus_p5_luminance.toFixed(1));
    metric(grid, 'Детали (Laplacian)', f.laplacian_variance.toFixed(1));
    metric(grid, 'Контуры', (f.canny_edge_density * 100).toFixed(2).replace('.', ',') + '%');
    technical.append(grid);
    const details = element('details');
    details.append(element('summary', '', 'Все 17 измерений'), element('pre', '', JSON.stringify(f, null, 2)));
    technical.append(details);
  }
  const vlm = addResultSection('Ответ визуальной модели');
  if (!data.reviews?.length) {
    vlm.append(element('div', 'empty-answer', 'VLM ещё не проверяла этот кадр. Она запускается только для подозрительных снимков после полного числового прохода.'));
    return;
  }
  data.reviews.forEach((review) => {
    vlm.append(element('p', '', `${review.source}${review.model ? ` · ${review.model}` : ''}${review.model_revision ? ` · ${review.model_revision.slice(0, 12)}` : ''}`));
    review.passes.forEach((pass) => {
      const box = element('div', 'pass-result');
      box.append(element('h4', '', `Независимый проход ${pass.pass}`));
      if (pass.valid && pass.answer) {
        const severe = pass.answer.severe_technical_defect || pass.answer.street_view_absent;
        box.append(element('span', `result-chip ${severe ? 'bad' : 'good'}`, severe ? 'Модель отмечает явный брак' : 'Явный брак не отмечен'));
        box.append(element('p', '', `Причина: ${reasonNames[pass.answer.reason] || pass.answer.reason}. Сильный технический дефект: ${pass.answer.severe_technical_defect ? 'да' : 'нет'}. Вид улицы отсутствует: ${pass.answer.street_view_absent ? 'да' : 'нет'}.`));
      } else {
        box.append(element('p', '', `Ответ не удалось разобрать${pass.error ? `: ${pass.error}` : '.'}`));
      }
      box.append(element('pre', '', pass.raw_text || 'Нет текста ответа'));
      vlm.append(box);
    });
  });
}
async function openPhoto(photo) {
  photoRequest?.abort();
  photoRequest = new AbortController();
  $('dialog-image').src = photo.image_url;
  $('dialog-image').alt = `Снимок из ${photo.source || 'галереи'}`;
  $('dialog-caption').textContent = `${photo.source || 'Галерея'} · ${photo.id}`;
  $('dialog-state').textContent = 'Загружаем измерения…';
  $('dialog-results').replaceChildren();
  $('photo-dialog').showModal();
  try {
    const response = await fetch(`/api/photo/${encodeURIComponent(photo.id)}`, {signal:photoRequest.signal, cache:'no-store'});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    renderPhotoDetails(await response.json());
  } catch (error) {
    if (error.name !== 'AbortError') $('dialog-state').textContent = `Не удалось загрузить ответ: ${error.message}`;
  }
}
async function selectChunk(index, item) {
  try {
    const response = await fetch(`/api/chunk?index=${index}`, {cache:'no-store'});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    selectedChunk = index;
    $('return-live').hidden = false;
    renderGrid(latestStatus.runs.find((run) => run.id === selectedRun).grid);
    renderPhotos(data.photos, `Партия ${index + 1}: снимки ${number.format(item.start)}–${number.format(item.end)}`);
  } catch (error) { showError(`Не удалось открыть партию: ${error.message}`); }
}
function showError(message) { $('error').textContent = message; $('error').hidden = false; }
function render(data) {
  latestStatus = data;
  const runs = data.runs || [];
  if (!runs.some((run) => run.id === selectedRun)) selectedRun = runs[0]?.id || null;
  renderRuns(runs);
  $('empty').hidden = runs.length > 0;
  $('content').hidden = runs.length === 0;
  $('server-time').textContent = `Обновлено ${liveClock.format(new Date(data.server_time))} МСК`;
  $('connection').classList.remove('offline');
  $('connection-text').textContent = 'Связь с локальным прогоном';
  if (!runs.length) return;
  const run = runs.find((item) => item.id === selectedRun);
  const pct = run.total ? Math.max(0, Math.min(100, 100 * run.completed / run.total)) : 0;
  $('run-state').textContent = runState(run.state);
  $('run-state').className = `state-pill ${run.state}`;
  $('run-phase').textContent = run.phase;
  $('run-title').textContent = run.title;
  $('run-description').textContent = run.description;
  $('percentage').textContent = `${pct.toFixed(1).replace('.', ',')}%`;
  $('fraction').textContent = `${number.format(run.completed)} из ${number.format(run.total)}`;
  $('progress-fill').style.width = `${pct}%`;
  $('progress-track').setAttribute('aria-valuemax', run.total);
  $('progress-track').setAttribute('aria-valuenow', run.completed);
  $('progress-context').textContent = `${run.phase}: ${number.format(run.completed)} ${run.unit} из ${number.format(run.total)}.`;
  $('eta').textContent = run.state === 'complete' ? 'Готово' : duration(run.eta_seconds);
  $('finish-time').textContent = run.eta_seconds && run.state === 'running' ? `Ориентир: ${dateTime.format(new Date(Date.now() + run.eta_seconds * 1000))} МСК` : run.state === 'stopped' ? 'Процесс не работает' : 'Оценка обновляется по мере работы';
  $('rate').textContent = run.rate_per_second == null ? '—' : number.format(Math.round(run.rate_per_second * 10) / 10);
  $('rate-unit').textContent = `${run.unit} в секунду`;
  $('last-update').textContent = run.last_update ? clock.format(new Date(run.last_update)) : '—';
  renderChart(run.history || []);
  renderGrid(run.grid || []);
  renderStages(run.stages || []);
  $('run-note').textContent = run.note || 'Прогресс записывается без изменения исходных артефактов.';
  if (selectedChunk == null) renderPhotos(run.photos || [], run.grid?.length ? 'Последняя завершённая партия' : 'Последние обработанные снимки');
  const other = data.other_processes || [];
  $('other-processes-panel').hidden = other.length === 0;
  $('other-processes').replaceChildren(...other.map((line) => element('li', '', line)));
}
function updatePollControl() {
  $('poll-toggle').textContent = paused ? 'Возобновить обновление' : 'Пауза обновления';
  $('poll-toggle').setAttribute('aria-pressed', String(paused));
  const effectiveSeconds = Math.max(refreshInterval, document.hidden ? 5000 : 0) / 1000;
  $('refresh-description').textContent = paused
    ? 'Автообновление на паузе: страница не запрашивает новые данные.'
    : `Данные с локального компьютера. Обновление каждые ${number.format(effectiveSeconds)} с${document.hidden && refreshInterval < 5000 ? ' в скрытой вкладке' : ''}.`;
  $('connection').classList.toggle('paused', paused);
  if (paused) $('connection-text').textContent = 'Обновление на паузе';
}
function scheduleRefresh() {
  clearTimeout(pollTimer);
  if (!paused) pollTimer = setTimeout(refresh, Math.max(refreshInterval, document.hidden ? 5000 : 0));
}
async function refresh(initial = false) {
  if (activeRequest || (paused && !initial)) return;
  const controller = new AbortController();
  activeRequest = controller;
  try {
    const response = await fetch('/api/status', {cache:'no-store', signal:controller.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    $('error').hidden = true;
    render(data);
    updatePollControl();
  } catch (error) {
    if (error.name === 'AbortError') return;
    $('connection').classList.add('offline');
    $('connection-text').textContent = 'Нет связи с сервером';
    showError(`Прогресс временно недоступен: ${error.message}. ${paused ? 'Нажмите «Возобновить обновление».' : 'Повторим автоматически.'}`);
  } finally {
    if (activeRequest === controller) activeRequest = null;
    scheduleRefresh();
  }
}
$('return-live').addEventListener('click', () => {selectedChunk = null; $('return-live').hidden = true; lastPhotosKey = ''; render(latestStatus);});
$('photo-dialog').addEventListener('click', (event) => {if (event.target === $('photo-dialog')) $('photo-dialog').close();});
$('photo-dialog').addEventListener('close', () => photoRequest?.abort());
$('poll-toggle').addEventListener('click', () => {
  paused = !paused;
  localStorage.setItem('geosnap-dashboard-paused', paused ? '1' : '0');
  if (paused) {clearTimeout(pollTimer); activeRequest?.abort();}
  updatePollControl();
  if (!paused) refresh();
});
$('refresh-interval').value = String(refreshInterval);
$('refresh-interval').addEventListener('change', (event) => {
  const next = Number(event.target.value);
  if (!intervals.has(next)) return;
  refreshInterval = next;
  localStorage.setItem('geosnap-dashboard-interval', String(next));
  updatePollControl();
  clearTimeout(pollTimer);
  if (!paused && !activeRequest) refresh();
});
document.addEventListener('visibilitychange', () => {
  updatePollControl();
  if (!paused) {clearTimeout(pollTimer); if (!activeRequest) refresh();}
});
updatePollControl();
refresh(true);
