# Отчёт об оценке

Актуально на 2026-08-15. Детальный воспроизводимый протокол и команды находятся
в [tracked Commons report](../data/evaluation/moscow_commons_proxy_run_2026-08-15.md),
а абляция геометрии — в [verification report](verification_report.md).

> Текущий набор — 20 реальных геопривязанных JPEG с Wikimedia Commons: 12
> gallery и 8 held-out query по четырём московским landmarks. Это маленький,
> вручную подобранный, landmark-biased и **не street-view** proxy. Он проверяет
> код и сравнение конфигураций, но не доказывает покрытие/точность по Москве и
> не годится для калибровки confidence.

## Воспроизводимость и leakage

- canonical dataset SHA-256:
  `39c331f6b53b439f4e40b993d58caf80f69f279a07fe1845442f2f650943b6ab`;
- 20/20 записей закреплены Commons SHA-1, JPEG SHA-256, координатой и
  attribution/license в tracked snapshot ledger;
- cross-split overlap page ID / Commons SHA-1 / JPEG SHA-256 / pHash: 0;
- same-landmark cross-split author/time matches: 0/0;
- retrieval positive: gallery geotag в пределах 100 м от query geotag;
- accuracy ≤25/50/100 м использует все 8 query, abstention считается ошибкой;
  median/p90 считаются только по `status=ok`.

## Основной результат

| Model | R@1 / R@5 / R@10 | ≤25 / ≤50 / ≤100 м | Answer rate | Median / p90, answered | Median offline query |
|---|---:|---:|---:|---:|---:|
| MegaLoc | 87.5 / 100 / 100% | 37.5 / 62.5 / 75% | 75% | 25.78 / 52.92 м | 72.78 ms |
| DINOv2 + SALAD | 87.5 / 100 / 100% | 37.5 / 62.5 / 75% | 75% | 23.37 / 52.92 м | 70.65 ms |

Оба retriever имеют descriptor 8,448 `float32`; 12 gallery descriptors занимают
405,504 bytes. Средняя query embedding latency: MegaLoc 73.33 ms, SALAD
71.56 ms; median exact FAISS: 0.428 и 0.500 ms; gallery throughput: 15.11 и
16.46 images/s соответственно. Это offline path без HTTP/network overhead.

MegaLoc остаётся инженерным default: метрики на proxy связаны, а его официальный
код/checkpoint имеют MIT-лицензию; SALAD repository — GPL-3.0 и остаётся
challenger. Это решение по deployability, а не заявление о большей точности.

## Robustness и полезные абляции

Девять детерминированных преобразований каждого реального query дали 72
derived query; они не считаются новыми географическими примерами. MegaLoc
сохранил R@1/5/10 `87.5/100/100%`, accuracy `37.5/62.5/75%`, answer rate 75%; у
SALAD — те же retrieval/answer-rate, accuracy `40.28/62.5/75%`.

Для MegaLoc weighted centroid улучшил primary ≤25 м с 37.5% до 50%, median с
25.78 до 15.35 м и p90 с 52.92 до 41.44 м. На derived query он улучшил ≤25 м
до 50%, ≤50 м до 65.28%, median до 14.29 м и p90 до 50.26 м. Default остаётся
weighted medoid до проверки на независимом street-view split: менять production
policy по восьми landmark query было бы overfit.

OpenCV SIFT verification не изменила Recall или top-1, но снизила accuracy
≤25/50/100 м с `37.5/62.5/75%` до `12.5/12.5/12.5%`, answer rate с 75% до
12.5% и увеличила median offline path с 88.64 до 719.42 ms. Поэтому
`VERIFICATION_ENABLED=false` остаётся default.

## Production blocker

Нужен заранее зафиксированный leakage-resistant Moscow street-view benchmark:
разные sequence/time для gallery и query, обычные улицы, повторяющиеся фасады,
сезоны/освещение/viewpoint и реалистичный масштаб галереи. До этого confidence
и uncertainty не калиброваны, а ни один результат выше нельзя выдавать за
city-wide product accuracy.
