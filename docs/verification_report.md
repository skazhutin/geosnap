# Отчёт о локальной геометрической проверке

Актуально на 2026-08-15. Выполнена воспроизводимая абляция
`retrieval-only` против `retrieval + OpenCV SIFT verification` на frozen
Moscow Commons proxy: 8 held-out query и 12 gallery изображений.

> **Важно:** это маленький, вручную подобранный, landmark-biased набор
> Wikimedia Commons. Это **не street-view**, не репрезентативная выборка улиц
> Москвы и не доказательство production coverage. Результат оставляет
> verification `default-off`; leakage-resistant benchmark на реальных
> московских street-view данных остаётся production blocker.

## Конфигурация

- Manifest: `moscow-commons-landmarks-proxy-v1`; stable canonical dataset
  SHA-256 `39c331f6b53b439f4e40b993d58caf80f69f279a07fe1845442f2f650943b6ab`.
  Raw manifest SHA-256 конкретного прогона (включая `created_at`):
  `437f994ed75bcc8a060513e22b60fa0d48eec57eee02271c61d2c7a16ec2a30a`;
  snapshot-ledger SHA-256:
  `5dd1a9fbfe7ab9c655f8816c6b19cc95be53c0d45fac888395134d1ffa1793dd`.
- Split: 12 gallery / 8 query, четыре landmark; gallery и query используют
  разные Commons page ID, Commons SHA-1, downloaded-byte SHA-256 и perceptual
  hash. Same-author и same-capture-time cross-split pairs отсутствуют.
- Retriever: официальный MegaLoc на MPS; repository revision
  `5fe0dd697c4a70ba3e23607f6716ab3c606b16db`.
- Checkpoint: `gberton/MegaLoc`, revision
  `37bb43d65dd6388d1578052de5eb0bcdceb497e7`, SHA-256
  `d4f9f2bcb60018f91eb6a8e061ed054fd55654e10c2569cf13841ea986ffb4f8`.
- Search: normalized exact `faiss.IndexFlatIP`, `top_k=12`.
- Localizer: неизменённый default `weighted_medoid`; confidence не
  калибровался и не настраивался на proxy.
- Verification: OpenCV SIFT `4.14.0.94`, `verify_top_k=10`, hard limit 20,
  `geometric_weight=0.35`.
- Recall-positive: gallery geotag не дальше 100 м от query geotag. Accuracy
  ≤25/50/100 м использует все восемь query; abstention считается ошибкой.

Для честного сравнения каждый query был embedded и найден exact FAISS ровно
один раз. Оба arm получили одинаковые raw matches. В verification arm raw
cosine остаётся `RetrievalResult.score`, а уже один раз смешанный
`rerank_score` передаётся как `metadata.localization_score`. Spatial localizer
использует этот combined score напрямую и не применяет geometric weight
повторно.

## Измеренные метрики

| Метрика | Retrieval-only | + OpenCV SIFT | Δ |
|---|---:|---:|---:|
| Recall@1 | 87.5% | 87.5% | 0 п.п. |
| Recall@5 | 100.0% | 100.0% | 0 п.п. |
| Recall@10 | 100.0% | 100.0% | 0 п.п. |
| Accuracy ≤25 м, все query | 37.5% | 12.5% | −25.0 п.п. |
| Accuracy ≤50 м, все query | 62.5% | 12.5% | −50.0 п.п. |
| Accuracy ≤100 м, все query | 75.0% | 12.5% | −62.5 п.п. |
| Answer rate (`status=ok`) | 75.0% (6/8) | 12.5% (1/8) | −62.5 п.п. |
| Median error, answered only | 25.780 м | 4.280 м | не сравнивать как gain |
| P90 error, answered only | 52.921 м | 4.280 м | не сравнивать как gain |

Verification не изменила top-1 ни для одного из восьми query. Два исходных
`out_of_coverage` остались такими же; из шести исходных ответов только один
остался `ok`, а пять стали `low_confidence`. Поэтому меньшие median/P90 в
verification arm вычислены по единственному оставшемуся ответу и являются
selection effect, а не улучшением локализации.

## Latency

Измерено на одном Apple Silicon host (`arm64`, Python 3.12.12, Torch 2.13.0,
MPS). Model load, gallery embedding и index build исключены из per-query
latency. `offline query pipeline` — сумма измеренных query embedding, exact
search и arm-specific этапов; network/API overhead отсутствует.

| Этап | N | Median, ms | P90, ms |
|---|---:|---:|---:|
| Shared query embedding | 8 | 88.000 | 108.830 |
| Shared exact FAISS search | 8 | 0.503 | 9.363 |
| Retrieval-only spatial localization | 8 | 0.175 | 0.192 |
| OpenCV SIFT verification + rerank | 8 | 634.625 | 1055.117 |
| OpenCV SIFT backend only | 8 | 634.585 | 794.712 |
| Verified spatial localization | 8 | 0.171 | 0.179 |
| Retrieval-only offline query pipeline | 8 | 88.638 | 121.224 |
| Verified offline query pipeline | 8 | 719.421 | 1176.304 |

Median offline overhead составил **+630.784 ms/query**. Первый вызов полного
verification stage включает lazy backend construction; строка `backend only`
измеряет непосредственно SIFT matching/RANSAC. Setup, исключённый из таблицы:
model load 4033.939 ms, gallery embedding 1007.080 ms, exact FAISS build
370.150 ms. Latency относится только к этому host и этому tiny top-12 setup;
она не является прогнозом CUDA или production gallery latency.

## Воспроизведение

```bash
HF_HOME="$PWD/.cache/huggingface" .venv/bin/python \
  -m ml.evaluation.verification_ablation \
  --manifest data/evaluation/generated/moscow_commons_proxy_v1/manifest.json \
  --output data/evaluation/generated/moscow_commons_proxy_v1/reports/verification_ablation_megaloc_opencv_sift.json \
  --model megaloc --device mps --batch-size 4 \
  --cache-dir .cache/torch/hub \
  --top-k 12 --verify-top-k 10 --geometric-weight 0.35 \
  --positive-distance-m 100 --estimator weighted_medoid
```

Machine-readable JSON содержит полные config/runtime metadata, обе метрики,
delta, latency summaries, per-query localization, порядок кандидатов и SIFT /
RANSAC evidence. Он генерируется локально по пути из команды и намеренно
остаётся под `data/evaluation/generated/**` (gitignored); измеренные итоговые
значения зафиксированы в этом tracked отчёте.

## Решение и production blocker

`VERIFICATION_ENABLED=false` остаётся правильным default: на этом proxy нет
прироста Recall, all-query localization metrics и answer rate ухудшились, а
median offline latency выросла примерно на 631 мс. Вес 0.35 и thresholds не
следует перенастраивать по восьми landmark query — это было бы proxy overfit.

Для пересмотра решения нужен заранее зафиксированный, leakage-resistant
Moscow street-view split с независимыми sequence/time, обычными улицами и
повторяющимися фасадами, сменой сезона/освещения/viewpoint, realistic gallery
scale и теми же Recall@1/5/10, ≤25/50/100 м, answer-rate и end-to-end latency
denominators. Пока такого результата нет, representative street-view
evaluation является явным production blocker.

## Предыдущий механический smoke-test

Предыдущий self-transform опыт остаётся только проверкой работоспособности
matching/RANSAC: perspective + brightness transform одного KartaView кадра из
Рима сравнивался с crop того же кадра и четырьмя Moscow Commons distractors.
Positive уже был retrieval top-1. OpenCV SIFT сохранил его top-1 с p50
295.607 ms по top-5; optional official LightGlue + SIFT на CPU также сохранил
top-1 с p50 707.629 ms. Этот test содержит pixel/scene leakage, не является
московской localization evaluation и не влияет на production-решение выше.

Unit/regression проверки bounded top-K, single-weight integration и структуры
8×12 ablation находятся в
[`ml/evaluation/tests/test_verification_ablation.py`](../ml/evaluation/tests/test_verification_ablation.py);
backend-specific проверки — в
[`ml/verification/tests/test_verification.py`](../ml/verification/tests/test_verification.py).
