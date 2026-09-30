# GeoSnap: подробный реестр сохранённых экспериментов

10 сентября 2026. Сводка существующих результатов; inference, calibration и final не запускались.

Этот реестр дополняет [итоговый отчёт](/Users/Daniil/Documents/VSCode/geosnap/docs/research_summary_20260910.md). Повторные записи с одинаковыми координатами под разными confidence-политиками сохранены: количество строк не равно числу независимых экспериментов.

Проценты из разных выборок и галерей напрямую не сравнивать. ∞ / не определено передаёт null из исходного артефакта; его смысл определяется оригинальным протоколом.

## Ранний retriever benchmark: 602 development-запроса, 20 031 reference

| Модель | R@1 ≤100 м | R@5 | R@10 | R@20 | R@50 |
|---|---:|---:|---:|---:|---:|
| megaloc | 21.93% | 28.07% | 29.40% | 31.89% | 35.38% |
| salad | 21.93% | 31.56% | 35.38% | 40.53% | 45.35% |
| sage | 30.23% | 35.55% | 39.04% | 43.52% | 49.00% |
| selavprplusplus_base | 29.07% | 35.22% | 38.70% | 42.52% | 46.18% |
| selavprplusplus_rerank | 29.07% | 35.38% | 37.87% | 40.86% | 45.51% |

Это retrieval до отказа от ответа. Старые product-localization показатели с отказами не переименованы в RAW.

## V5: все 423 RAW-записи из 85 development summary-артефактов

Техническое имя варианта сохранено дословно для поиска исходных результатов. N=751 — историческая development; N=1184 — основная development.

### development/branch_sage-vitl_baseline_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → original → raw_top1 | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |
| metrics → scene_weight_0.000000 → raw_top1 | 1184 | 6.84% | 11.91% | 16.22% | 12 805 | 35 402 | 78.29% |
| metrics → scene_weight_0.015385 → raw_top1 | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |
| metrics → scene_weight_0.050000 → raw_top1 | 1184 | 6.42% | 11.57% | 15.88% | 13 203 | 36 529 | 78.46% |
| metrics → scene_weight_0.100000 → raw_top1 | 1184 | 6.08% | 10.73% | 14.78% | 12 714 | 36 580 | 79.81% |
| metrics → scene_weight_0.250000 → raw_top1 | 1184 | 4.65% | 7.94% | 10.30% | 13 522 | 36 232 | 84.97% |
| metrics → scene_weight_0.500000 → raw_top1 | 1184 | 2.62% | 4.48% | 5.41% | 15 155 | 35 985 | 91.81% |
| metrics → scene_weight_1.000000 → raw_top1 | 1184 | 0.08% | 0.08% | 0.17% | 17 982 | 37 405 | 99.41% |

### development/confidence_fusion50_top1_union/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw_unchanged | 1184 | 9.71% | 17.48% | 24.24% | 6 803 | 31 965 | 68.16% |

### development/confidence_hybrid_context30_mix05/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw_unchanged | 1184 | 10.30% | 18.07% | 25.17% | 6 474 | 31 317 | 66.64% |

### development/confidence_hybrid_transfer/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw_unchanged | 1184 | 10.30% | 18.07% | 25.17% | 6 474 | 31 317 | 66.64% |

### development/confidence_hybrid_transfer_aux/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw_unchanged | 1184 | 10.30% | 18.07% | 25.17% | 6 474 | 31 317 | 66.64% |

### development/confidence_sage_b_union/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw_unchanged | 1184 | 8.02% | 14.02% | 20.02% | 9 127 | 32 979 | 72.55% |

### development/confidence_sage_l_union/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| raw_unchanged | 1184 | 9.80% | 17.31% | 23.73% | 8 347 | 31 913 | 69.51% |

### development/context_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → full_preencoder → raw_top1 | 751 | 14.38% | 20.77% | 26.10% | 7 597 | 26 370 | 68.58% |
| metrics → context30_mix0.25 → raw_top1 | 751 | 14.11% | 20.64% | 26.23% | 7 134 | 26 542 | 67.91% |
| metrics → context30_mix0.5 → raw_top1 | 751 | 14.38% | 21.30% | 27.30% | 6 851 | 26 468 | 66.98% |
| metrics → context30_mix0.75 → raw_top1 | 751 | 14.65% | 22.24% | 28.10% | 6 658 | 26 643 | 65.78% |
| metrics → context30_mix1.0 → raw_top1 | 751 | 14.51% | 21.97% | 27.56% | 7 060 | 27 649 | 66.18% |
| metrics → context100_mix0.25 → raw_top1 | 751 | 14.11% | 20.64% | 26.23% | 7 239 | 26 468 | 67.78% |
| metrics → context100_mix0.5 → raw_top1 | 751 | 14.38% | 21.17% | 27.30% | 7 134 | 26 678 | 67.24% |
| metrics → context100_mix0.75 → raw_top1 | 751 | 14.51% | 21.70% | 27.83% | 6 815 | 26 678 | 65.78% |
| metrics → context100_mix1.0 → raw_top1 | 751 | 14.65% | 21.97% | 27.96% | 6 623 | 26 621 | 65.51% |

### development/context_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → full_preencoder → raw_top1 | 1184 | 5.83% | 10.05% | 12.58% | 13 280 | 34 779 | 82.52% |
| metrics → context30_mix0.25 → raw_top1 | 1184 | 5.91% | 10.05% | 12.58% | 13 168 | 34 779 | 82.43% |
| metrics → context30_mix0.5 → raw_top1 | 1184 | 5.83% | 9.97% | 12.58% | 13 427 | 34 969 | 82.52% |
| metrics → context30_mix0.75 → raw_top1 | 1184 | 5.83% | 9.88% | 12.50% | 13 457 | 35 123 | 82.26% |
| metrics → context30_mix1.0 → raw_top1 | 1184 | 5.57% | 9.80% | 12.84% | 13 442 | 35 286 | 82.01% |
| metrics → context100_mix0.25 → raw_top1 | 1184 | 5.91% | 10.05% | 12.67% | 13 112 | 34 879 | 82.43% |
| metrics → context100_mix0.5 → raw_top1 | 1184 | 6.00% | 10.05% | 12.75% | 12 887 | 34 879 | 82.26% |
| metrics → context100_mix0.75 → raw_top1 | 1184 | 6.08% | 10.14% | 12.75% | 13 048 | 35 443 | 82.18% |
| metrics → context100_mix1.0 → raw_top1 | 1184 | 6.00% | 9.88% | 12.50% | 13 289 | 35 978 | 82.35% |

### development/database_augmentation/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → visual_dba3 → raw_top1 | 751 | 14.65% | 23.04% | 29.96% | 6 991 | 26 432 | 65.25% |
| metrics → visual_dba3_aqe3 → raw_top1 | 751 | 13.72% | 21.17% | 27.03% | 7 781 | 25 961 | 68.31% |
| metrics → visual_dba10 → raw_top1 | 751 | 14.38% | 22.77% | 29.43% | 7 050 | 26 498 | 65.51% |
| metrics → visual_dba10_aqe3 → raw_top1 | 751 | 13.45% | 20.77% | 26.50% | 7 821 | 27 821 | 68.58% |
| metrics → geo_dba3 → raw_top1 | 751 | 14.91% | 23.30% | 30.09% | 6 991 | 26 038 | 64.98% |
| metrics → geo_dba10 → raw_top1 | 751 | 14.78% | 23.17% | 29.96% | 7 050 | 25 894 | 64.98% |

### development/exact_faiss_baseline_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |

### development/gallery_fusion_historical/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → sage-vitl → raw_top1 | 751 | 16.38% | 25.43% | 34.09% | 4 236 | 24 540 | 58.19% |
| metrics → fusion25 → raw_top1 | 751 | 15.58% | 24.37% | 32.22% | 5 098 | 25 496 | 61.92% |
| metrics → fusion50 → raw_top1 | 751 | 16.51% | 25.30% | 33.56% | 3 973 | 24 951 | 59.39% |
| metrics → fusion75 → raw_top1 | 751 | 16.91% | 25.83% | 33.95% | 4 051 | 24 951 | 58.19% |

### development/gallery_fusion_historical/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 751 | 14.65% | 22.90% | 29.56% | 7 050 | 26 498 | 65.38% |
| metrics → sage-vitl → raw_top1 | 751 | 15.85% | 24.63% | 33.42% | 4 192 | 25 092 | 59.12% |
| metrics → fusion25 → raw_top1 | 751 | 15.45% | 24.23% | 32.09% | 5 098 | 26 038 | 62.05% |
| metrics → fusion50 → raw_top1 | 751 | 16.25% | 25.17% | 33.29% | 3 434 | 25 092 | 59.92% |
| metrics → fusion75 → raw_top1 | 751 | 16.51% | 25.30% | 33.56% | 3 958 | 25 748 | 58.72% |

### development/gallery_fusion_historical/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 751 | 15.71% | 25.43% | 32.62% | 5 608 | 25 772 | 62.72% |
| metrics → sage-vitl → raw_top1 | 751 | 17.44% | 28.36% | 37.95% | 2 172 | 24 254 | 55.13% |
| metrics → fusion25 → raw_top1 | 751 | 16.91% | 27.70% | 36.09% | 3 514 | 24 128 | 58.59% |
| metrics → fusion50 → raw_top1 | 751 | 17.98% | 28.89% | 37.28% | 2 346 | 23 055 | 56.06% |
| metrics → fusion75 → raw_top1 | 751 | 18.11% | 29.03% | 38.08% | 2 465 | 24 076 | 55.53% |

### development/gallery_fusion_historical/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 751 | 15.71% | 25.30% | 32.36% | 5 772 | 26 375 | 62.85% |
| metrics → sage-vitl → raw_top1 | 751 | 17.04% | 27.83% | 37.28% | 2 372 | 24 315 | 56.06% |
| metrics → fusion25 → raw_top1 | 751 | 16.91% | 27.70% | 36.09% | 3 375 | 25 171 | 58.72% |
| metrics → fusion50 → raw_top1 | 751 | 17.84% | 28.89% | 37.42% | 2 346 | 24 223 | 56.46% |
| metrics → fusion75 → raw_top1 | 751 | 17.84% | 28.76% | 37.82% | 2 664 | 24 761 | 56.06% |

### development/gallery_fusion_localized_historical/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| methods → sage-vitl → raw | 751 | 16.64% | 25.97% | 34.22% | 4 151 | 25 405 | 58.59% |
| methods → fusion25 → raw | 751 | 15.18% | 24.10% | 31.96% | 5 098 | 25 836 | 62.32% |
| methods → fusion50 → raw | 751 | 16.25% | 25.30% | 33.29% | 3 948 | 24 966 | 59.79% |
| methods → fusion75 → raw | 751 | 16.91% | 26.10% | 34.22% | 4 224 | 25 486 | 58.59% |

### development/gallery_fusion_localized_historical/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 751 | 13.85% | 22.24% | 28.89% | 7 016 | 26 498 | 65.91% |
| methods → sage-vitl → raw | 751 | 15.85% | 24.77% | 33.42% | 4 192 | 25 734 | 59.79% |
| methods → fusion25 → raw | 751 | 15.05% | 23.97% | 31.69% | 5 444 | 27 416 | 62.72% |
| methods → fusion50 → raw | 751 | 15.71% | 24.90% | 33.02% | 3 958 | 25 836 | 60.85% |
| methods → fusion75 → raw | 751 | 16.25% | 25.17% | 33.69% | 4 236 | 25 766 | 59.52% |

### development/gallery_fusion_localized_historical/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 751 | 14.91% | 24.50% | 31.69% | 5 928 | 25 486 | 64.05% |
| methods → sage-vitl → raw | 751 | 17.98% | 28.89% | 38.22% | 2 346 | 24 254 | 55.66% |
| methods → fusion25 → raw | 751 | 16.51% | 27.30% | 35.69% | 3 887 | 24 761 | 59.65% |
| methods → fusion50 → raw | 751 | 17.44% | 28.63% | 37.28% | 2 644 | 22 884 | 56.99% |
| methods → fusion75 → raw | 751 | 18.38% | 29.56% | 38.75% | 2 468 | 24 254 | 55.53% |

### development/gallery_fusion_localized_historical/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 751 | 14.91% | 24.63% | 31.56% | 6 575 | 26 498 | 64.05% |
| methods → sage-vitl → raw | 751 | 17.31% | 27.96% | 37.02% | 3 187 | 24 724 | 57.26% |
| methods → fusion25 → raw | 751 | 16.51% | 27.43% | 35.69% | 3 887 | 25 836 | 59.79% |
| methods → fusion50 → raw | 751 | 17.18% | 28.36% | 37.02% | 2 911 | 24 741 | 57.66% |
| methods → fusion75 → raw | 751 | 18.11% | 29.16% | 38.22% | 3 158 | 25 106 | 56.46% |

### development/gallery_fusion_localized_new/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |
| methods → sage-vitl → raw | 1184 | 6.42% | 11.66% | 16.39% | 12 675 | 35 206 | 77.79% |
| methods → fusion25 → raw | 1184 | 5.91% | 10.73% | 15.12% | 12 335 | 35 290 | 79.05% |
| methods → fusion50 → raw | 1184 | 6.59% | 11.99% | 16.72% | 11 960 | 34 548 | 77.20% |
| methods → fusion75 → raw | 1184 | 6.84% | 11.99% | 16.55% | 11 766 | 34 152 | 77.53% |

### development/gallery_fusion_localized_new/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 1184 | 7.09% | 12.33% | 17.74% | 9 861 | 32 860 | 74.49% |
| methods → sage-vitl → raw | 1184 | 8.11% | 14.86% | 20.95% | 9 482 | 32 190 | 71.54% |
| methods → fusion25 → raw | 1184 | 7.52% | 13.60% | 19.34% | 8 360 | 32 670 | 72.64% |
| methods → fusion50 → raw | 1184 | 8.02% | 14.86% | 20.78% | 7 896 | 31 377 | 70.86% |
| methods → fusion75 → raw | 1184 | 8.53% | 15.12% | 21.11% | 8 335 | 31 965 | 70.69% |

### development/gallery_fusion_localized_new/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 1184 | 6.76% | 11.91% | 16.89% | 10 770 | 33 152 | 77.20% |
| methods → sage-vitl → raw | 1184 | 8.11% | 14.36% | 19.43% | 10 447 | 34 550 | 73.65% |
| methods → fusion25 → raw | 1184 | 7.60% | 13.68% | 19.43% | 9 739 | 33 730 | 73.82% |
| methods → fusion50 → raw | 1184 | 8.02% | 14.61% | 20.27% | 9 461 | 33 161 | 72.97% |
| methods → fusion75 → raw | 1184 | 8.53% | 14.61% | 19.93% | 9 980 | 33 161 | 73.14% |

### development/gallery_fusion_localized_new/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 1184 | 8.02% | 14.02% | 20.02% | 9 127 | 32 979 | 72.55% |
| methods → sage-vitl → raw | 1184 | 9.80% | 17.31% | 23.73% | 8 347 | 31 913 | 69.51% |
| methods → fusion25 → raw | 1184 | 8.95% | 15.71% | 22.72% | 7 217 | 32 864 | 69.59% |
| methods → fusion50 → raw | 1184 | 9.46% | 17.15% | 23.82% | 6 855 | 31 965 | 68.33% |
| methods → fusion75 → raw | 1184 | 10.05% | 17.31% | 23.90% | 7 370 | 31 623 | 68.50% |

### development/gallery_fusion_new/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → sage-vitl → raw_top1 | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |
| metrics → fusion25 → raw_top1 | 1184 | 6.17% | 10.73% | 14.95% | 12 568 | 35 838 | 79.65% |
| metrics → fusion50 → raw_top1 | 1184 | 6.59% | 11.91% | 16.13% | 12 299 | 35 515 | 78.21% |
| metrics → fusion75 → raw_top1 | 1184 | 7.01% | 12.08% | 16.22% | 12 033 | 34 817 | 78.12% |

### development/gallery_fusion_new/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 1184 | 7.26% | 12.50% | 17.57% | 10 038 | 32 898 | 74.83% |
| metrics → sage-vitl → raw_top1 | 1184 | 8.45% | 15.20% | 20.95% | 9 606 | 32 132 | 71.62% |
| metrics → fusion25 → raw_top1 | 1184 | 7.77% | 13.77% | 19.34% | 8 474 | 32 111 | 72.97% |
| metrics → fusion50 → raw_top1 | 1184 | 8.28% | 15.12% | 20.78% | 8 266 | 31 773 | 71.11% |
| metrics → fusion75 → raw_top1 | 1184 | 8.78% | 15.46% | 21.20% | 8 503 | 31 039 | 70.95% |

### development/gallery_fusion_new/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 1184 | 7.18% | 12.50% | 17.23% | 11 426 | 34 098 | 77.45% |
| metrics → sage-vitl → raw_top1 | 1184 | 8.19% | 14.27% | 19.43% | 10 891 | 34 801 | 73.90% |
| metrics → fusion25 → raw_top1 | 1184 | 7.77% | 13.51% | 18.75% | 10 230 | 34 098 | 75.17% |
| metrics → fusion50 → raw_top1 | 1184 | 8.19% | 14.78% | 20.19% | 9 902 | 33 956 | 73.31% |
| metrics → fusion75 → raw_top1 | 1184 | 8.45% | 14.36% | 19.51% | 10 324 | 34 382 | 73.48% |

### development/gallery_fusion_new/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage-vitb → raw_top1 | 1184 | 8.45% | 14.70% | 20.35% | 9 216 | 32 792 | 72.72% |
| metrics → sage-vitl → raw_top1 | 1184 | 9.97% | 17.40% | 23.82% | 8 759 | 32 691 | 69.26% |
| metrics → fusion25 → raw_top1 | 1184 | 9.29% | 16.13% | 22.55% | 7 290 | 32 574 | 70.19% |
| metrics → fusion50 → raw_top1 | 1184 | 9.71% | 17.48% | 24.24% | 6 803 | 31 965 | 68.16% |
| metrics → fusion75 → raw_top1 | 1184 | 10.05% | 17.31% | 23.99% | 7 437 | 32 150 | 68.50% |

### development/gallery_fusion_top1_historical/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 751 | 15.71% | 25.30% | 32.36% | 5 772 | 26 375 | 62.85% |
| methods → sage-vitl → raw | 751 | 17.04% | 27.83% | 37.28% | 2 372 | 24 315 | 56.06% |
| methods → fusion25 → raw | 751 | 16.91% | 27.70% | 36.09% | 3 375 | 25 171 | 58.72% |
| methods → fusion50 → raw | 751 | 17.84% | 28.89% | 37.42% | 2 346 | 24 223 | 56.46% |
| methods → fusion75 → raw | 751 | 17.84% | 28.76% | 37.82% | 2 664 | 24 761 | 56.06% |

### development/gallery_fusion_top1_new/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitb → raw | 1184 | 8.45% | 14.70% | 20.35% | 9 216 | 32 792 | 72.72% |
| methods → sage-vitl → raw | 1184 | 9.97% | 17.40% | 23.82% | 8 759 | 32 691 | 69.26% |
| methods → fusion25 → raw | 1184 | 9.29% | 16.13% | 22.55% | 7 290 | 32 574 | 70.19% |
| methods → fusion50 → raw | 1184 | 9.71% | 17.48% | 24.24% | 6 803 | 31 965 | 68.16% |
| methods → fusion75 → raw | 1184 | 10.05% | 17.31% | 23.99% | 7 437 | 32 150 | 68.50% |

### development/gallery_localized_sage-vitb_historical/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → baseline → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |

### development/gallery_localized_sage-vitb_historical/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → diversity → raw | 751 | 13.85% | 22.24% | 28.89% | 7 016 | 26 498 | 65.91% |

### development/gallery_localized_sage-vitb_historical/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → random → raw | 751 | 14.91% | 24.50% | 31.69% | 5 928 | 25 486 | 64.05% |

### development/gallery_localized_sage-vitb_historical/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → union_larger_budget → raw | 751 | 14.91% | 24.63% | 31.56% | 6 575 | 26 498 | 64.05% |

### development/gallery_localized_sage-vitb_new/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → baseline → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |

### development/gallery_localized_sage-vitb_new/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → diversity → raw | 1184 | 7.09% | 12.33% | 17.74% | 9 861 | 32 860 | 74.49% |

### development/gallery_localized_sage-vitb_new/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → random → raw | 1184 | 6.76% | 11.91% | 16.89% | 10 770 | 33 152 | 77.20% |

### development/gallery_localized_sage-vitb_new/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → union_larger_budget → raw | 1184 | 8.02% | 14.02% | 20.02% | 9 127 | 32 979 | 72.55% |

### development/gallery_localized_sage-vitl_historical/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → baseline → raw | 751 | 16.64% | 25.97% | 34.22% | 4 151 | 25 405 | 58.59% |

### development/gallery_localized_sage-vitl_historical/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → diversity → raw | 751 | 15.85% | 24.77% | 33.42% | 4 192 | 25 734 | 59.79% |

### development/gallery_localized_sage-vitl_historical/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → random → raw | 751 | 17.98% | 28.89% | 38.22% | 2 346 | 24 254 | 55.66% |

### development/gallery_localized_sage-vitl_historical/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → union_larger_budget → raw | 751 | 17.31% | 27.96% | 37.02% | 3 187 | 24 724 | 57.26% |

### development/gallery_localized_sage-vitl_new/baseline/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → baseline → raw | 1184 | 6.42% | 11.66% | 16.39% | 12 675 | 35 206 | 77.79% |

### development/gallery_localized_sage-vitl_new/diversity/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → diversity → raw | 1184 | 8.11% | 14.86% | 20.95% | 9 482 | 32 190 | 71.54% |

### development/gallery_localized_sage-vitl_new/random/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → random → raw | 1184 | 8.11% | 14.36% | 19.43% | 10 447 | 34 550 | 73.65% |

### development/gallery_localized_sage-vitl_new/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → union_larger_budget → raw | 1184 | 9.80% | 17.31% | 23.73% | 8 347 | 31 913 | 69.51% |

### development/gallery_sage-vitb_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → baseline → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → random → raw_top1 | 751 | 15.71% | 25.43% | 32.62% | 5 608 | 25 772 | 62.72% |
| metrics → diversity → raw_top1 | 751 | 14.65% | 22.90% | 29.56% | 7 050 | 26 498 | 65.38% |
| metrics → union_larger_budget → raw_top1 | 751 | 15.71% | 25.30% | 32.36% | 5 772 | 26 375 | 62.85% |

### development/gallery_sage-vitb_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → baseline → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → random → raw_top1 | 1184 | 7.18% | 12.50% | 17.23% | 11 426 | 34 098 | 77.45% |
| metrics → diversity → raw_top1 | 1184 | 7.26% | 12.50% | 17.57% | 10 038 | 32 898 | 74.83% |
| metrics → union_larger_budget → raw_top1 | 1184 | 8.45% | 14.70% | 20.35% | 9 216 | 32 792 | 72.72% |

### development/gallery_sage-vitl_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → baseline → raw_top1 | 751 | 16.38% | 25.43% | 34.09% | 4 236 | 24 540 | 58.19% |
| metrics → random → raw_top1 | 751 | 17.44% | 28.36% | 37.95% | 2 172 | 24 254 | 55.13% |
| metrics → diversity → raw_top1 | 751 | 15.85% | 24.63% | 33.42% | 4 192 | 25 092 | 59.12% |
| metrics → union_larger_budget → raw_top1 | 751 | 17.04% | 27.83% | 37.28% | 2 372 | 24 315 | 56.06% |

### development/gallery_sage-vitl_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → baseline → raw_top1 | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |
| metrics → random → raw_top1 | 1184 | 8.19% | 14.27% | 19.43% | 10 891 | 34 801 | 73.90% |
| metrics → diversity → raw_top1 | 1184 | 8.45% | 15.20% | 20.95% | 9 606 | 32 132 | 71.62% |
| metrics → union_larger_budget → raw_top1 | 1184 | 9.97% | 17.40% | 23.82% | 8 759 | 32 691 | 69.26% |

### development/gallery_top1_sage-vitb_historical/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → union_larger_budget → raw | 751 | 15.71% | 25.30% | 32.36% | 5 772 | 26 375 | 62.85% |

### development/gallery_top1_sage-vitb_new/union_larger_budget/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → union_larger_budget → raw | 1184 | 8.45% | 14.70% | 20.35% | 9 216 | 32 792 | 72.72% |

### development/geographic_sweep_union/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| top1_raw | 1184 | 9.71% | 17.48% | 24.24% | 6 803 | 31 965 | 68.16% |

### development/global_boq_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → boq → raw_top1 | 751 | 11.98% | 20.11% | 25.83% | 7 187 | 26 213 | 67.51% |
| metrics → fusion25 → raw_top1 | 751 | 15.05% | 24.23% | 31.16% | 4 636 | 24 366 | 62.18% |
| metrics → fusion50 → raw_top1 | 751 | 14.51% | 24.37% | 30.36% | 4 231 | 23 430 | 61.78% |
| metrics → fusion75 → raw_top1 | 751 | 13.18% | 22.50% | 28.89% | 6 013 | 24 792 | 64.58% |
| metrics → zscore50 → raw_top1 | 751 | 14.25% | 24.10% | 30.63% | 4 231 | 23 895 | 61.25% |
| metrics → rrf10 → raw_top1 | 751 | 14.25% | 23.44% | 29.69% | 4 551 | 25 119 | 62.18% |
| metrics → rrf60 → raw_top1 | 751 | 14.11% | 23.04% | 29.43% | 4 926 | 25 121 | 62.45% |

### development/global_boq_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → boq → raw_top1 | 1184 | 4.73% | 7.94% | 10.90% | 14 861 | 36 439 | 83.45% |
| metrics → fusion25 → raw_top1 | 1184 | 6.08% | 10.56% | 14.36% | 12 662 | 35 290 | 80.15% |
| metrics → fusion50 → raw_top1 | 1184 | 6.17% | 10.39% | 14.44% | 12 339 | 35 402 | 78.97% |
| metrics → fusion75 → raw_top1 | 1184 | 5.83% | 10.05% | 14.10% | 12 923 | 35 511 | 79.48% |
| metrics → zscore50 → raw_top1 | 1184 | 6.25% | 10.56% | 14.61% | 11 868 | 34 967 | 78.80% |
| metrics → rrf10 → raw_top1 | 1184 | 5.74% | 9.97% | 14.02% | 12 576 | 35 597 | 79.56% |
| metrics → rrf60 → raw_top1 | 1184 | 5.66% | 9.97% | 13.85% | 12 676 | 35 511 | 79.90% |

### development/global_edtformer_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → edtformer → raw_top1 | 751 | 10.92% | 18.11% | 24.63% | 6 391 | 24 641 | 68.84% |
| metrics → fusion25 → raw_top1 | 751 | 14.51% | 23.44% | 30.23% | 5 414 | 24 366 | 63.65% |
| metrics → fusion50 → raw_top1 | 751 | 13.98% | 22.90% | 30.09% | 4 835 | 25 486 | 63.52% |
| metrics → fusion75 → raw_top1 | 751 | 12.52% | 20.37% | 27.16% | 5 325 | 24 761 | 65.91% |
| metrics → zscore50 → raw_top1 | 751 | 14.11% | 22.64% | 29.83% | 5 267 | 25 092 | 63.91% |
| metrics → rrf10 → raw_top1 | 751 | 13.18% | 21.17% | 28.23% | 5 350 | 25 228 | 65.51% |
| metrics → rrf60 → raw_top1 | 751 | 12.52% | 20.37% | 26.90% | 5 830 | 25 548 | 66.98% |

### development/global_sage-full-features_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → sage-full-features → raw_top1 | 751 | 14.38% | 20.77% | 26.10% | 7 597 | 26 370 | 68.58% |
| metrics → fusion25 → raw_top1 | 751 | 15.45% | 24.23% | 31.16% | 5 307 | 25 092 | 62.98% |
| metrics → fusion50 → raw_top1 | 751 | 15.45% | 23.70% | 30.76% | 5 164 | 23 888 | 62.72% |
| metrics → fusion75 → raw_top1 | 751 | 14.91% | 22.64% | 29.43% | 5 897 | 25 438 | 64.85% |
| metrics → zscore50 → raw_top1 | 751 | 15.85% | 24.63% | 31.82% | 5 267 | 23 904 | 62.18% |
| metrics → rrf10 → raw_top1 | 751 | 15.18% | 23.04% | 30.23% | 5 621 | 25 194 | 63.78% |
| metrics → rrf60 → raw_top1 | 751 | 15.05% | 22.64% | 29.83% | 5 591 | 24 951 | 63.91% |

### development/global_sage-full-features_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → sage-full-features → raw_top1 | 1184 | 5.83% | 10.05% | 12.58% | 13 280 | 34 779 | 82.52% |
| metrics → fusion25 → raw_top1 | 1184 | 6.00% | 10.56% | 14.61% | 12 339 | 34 974 | 79.81% |
| metrics → fusion50 → raw_top1 | 1184 | 6.25% | 10.73% | 14.27% | 12 420 | 34 915 | 80.32% |
| metrics → fusion75 → raw_top1 | 1184 | 6.17% | 10.47% | 13.51% | 12 416 | 34 714 | 81.50% |
| metrics → zscore50 → raw_top1 | 1184 | 6.33% | 10.73% | 14.61% | 12 567 | 35 194 | 79.90% |
| metrics → rrf10 → raw_top1 | 1184 | 6.08% | 10.39% | 13.43% | 12 575 | 34 978 | 81.08% |
| metrics → rrf60 → raw_top1 | 1184 | 6.00% | 10.14% | 13.34% | 12 672 | 35 123 | 81.50% |

### development/global_sage-vitb_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → sage-vitb → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → fusion25 → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → fusion50 → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → fusion75 → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → zscore50 → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → rrf10 → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → rrf60 → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |

### development/global_sage-vitl_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → sage-vitl → raw_top1 | 751 | 16.38% | 25.43% | 34.09% | 4 236 | 24 540 | 58.19% |
| metrics → fusion25 → raw_top1 | 751 | 15.58% | 24.37% | 32.22% | 5 098 | 25 496 | 61.92% |
| metrics → fusion50 → raw_top1 | 751 | 16.51% | 25.30% | 33.56% | 3 973 | 24 951 | 59.39% |
| metrics → fusion75 → raw_top1 | 751 | 16.91% | 25.83% | 33.95% | 4 051 | 24 951 | 58.19% |
| metrics → zscore50 → raw_top1 | 751 | 16.51% | 25.30% | 33.29% | 3 808 | 24 761 | 59.52% |
| metrics → rrf10 → raw_top1 | 751 | 15.45% | 24.37% | 32.09% | 4 654 | 25 119 | 61.25% |
| metrics → rrf60 → raw_top1 | 751 | 15.45% | 24.37% | 32.22% | 4 586 | 25 486 | 61.25% |

### development/global_sage-vitl_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| metrics → sage-vitl → raw_top1 | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |
| metrics → fusion25 → raw_top1 | 1184 | 6.17% | 10.73% | 14.95% | 12 568 | 35 838 | 79.65% |
| metrics → fusion50 → raw_top1 | 1184 | 6.59% | 11.91% | 16.13% | 12 299 | 35 515 | 78.21% |
| metrics → fusion75 → raw_top1 | 1184 | 7.01% | 12.08% | 16.22% | 12 033 | 34 817 | 78.12% |
| metrics → zscore50 → raw_top1 | 1184 | 6.76% | 11.91% | 16.30% | 12 299 | 35 184 | 77.96% |
| metrics → rrf10 → raw_top1 | 1184 | 6.67% | 11.91% | 15.88% | 12 559 | 35 714 | 78.80% |
| metrics → rrf60 → raw_top1 | 1184 | 6.67% | 11.91% | 15.79% | 12 547 | 35 887 | 79.05% |

### development/hybrid_context_union_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → fusion50 → raw_top1 | 751 | 17.84% | 28.89% | 37.42% | 2 346 | 24 223 | 56.46% |
| metrics → hybrid_context30_mix0.25 → raw_top1 | 751 | 17.58% | 28.63% | 37.95% | 2 395 | 24 480 | 55.79% |
| metrics → hybrid_context30_mix0.5 → raw_top1 | 751 | 17.84% | 28.50% | 38.08% | 2 221 | 24 298 | 55.13% |
| metrics → hybrid_context30_mix0.75 → raw_top1 | 751 | 17.44% | 28.36% | 37.55% | 2 221 | 24 298 | 54.99% |
| metrics → hybrid_context30_mix1.0 → raw_top1 | 751 | 16.91% | 27.30% | 36.22% | 2 781 | 24 298 | 56.06% |
| metrics → hybrid_context100_mix0.25 → raw_top1 | 751 | 17.84% | 29.03% | 38.48% | 2 217 | 24 254 | 55.39% |
| metrics → hybrid_context100_mix0.5 → raw_top1 | 751 | 18.24% | 29.03% | 38.22% | 2 221 | 24 761 | 54.86% |
| metrics → hybrid_context100_mix0.75 → raw_top1 | 751 | 17.84% | 28.50% | 38.08% | 1 680 | 24 254 | 54.19% |
| metrics → hybrid_context100_mix1.0 → raw_top1 | 751 | 17.31% | 28.10% | 37.28% | 2 346 | 24 480 | 55.39% |

### development/hybrid_context_union_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → fusion50 → raw_top1 | 1184 | 9.71% | 17.48% | 24.24% | 6 803 | 31 965 | 68.16% |
| metrics → hybrid_context30_mix0.25 → raw_top1 | 1184 | 9.88% | 17.91% | 24.83% | 6 637 | 31 518 | 67.31% |
| metrics → hybrid_context30_mix0.5 → raw_top1 | 1184 | 10.30% | 18.07% | 25.17% | 6 474 | 31 317 | 66.64% |
| metrics → hybrid_context30_mix0.75 → raw_top1 | 1184 | 10.22% | 17.99% | 24.75% | 6 997 | 31 127 | 67.31% |
| metrics → hybrid_context30_mix1.0 → raw_top1 | 1184 | 10.05% | 17.82% | 24.49% | 7 721 | 32 149 | 67.91% |
| metrics → hybrid_context100_mix0.25 → raw_top1 | 1184 | 9.97% | 17.82% | 24.75% | 6 454 | 31 518 | 67.23% |
| metrics → hybrid_context100_mix0.5 → raw_top1 | 1184 | 10.39% | 18.07% | 25.00% | 6 474 | 31 192 | 66.55% |
| metrics → hybrid_context100_mix0.75 → raw_top1 | 1184 | 10.39% | 18.07% | 24.92% | 6 997 | 31 561 | 67.15% |
| metrics → hybrid_context100_mix1.0 → raw_top1 | 1184 | 10.14% | 17.74% | 24.32% | 7 636 | 31 257 | 68.33% |

### development/hybrid_geographic_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → fusion50 → raw | 751 | 17.18% | 28.36% | 37.02% | 2 911 | 24 741 | 57.66% |
| methods → hybrid_context30_mix0.25 → raw | 751 | 17.58% | 28.89% | 38.08% | 3 187 | 25 173 | 56.72% |
| methods → hybrid_context30_mix0.5 → raw | 751 | 17.58% | 28.76% | 38.35% | 3 193 | 24 298 | 56.06% |
| methods → hybrid_context30_mix0.75 → raw | 751 | 17.31% | 28.10% | 37.02% | 3 375 | 25 405 | 56.86% |
| methods → hybrid_context30_mix1.0 → raw | 751 | 16.38% | 26.90% | 35.15% | 4 259 | 25 463 | 58.32% |
| methods → hybrid_context100_mix0.25 → raw | 751 | 17.58% | 28.89% | 38.48% | 2 604 | 24 966 | 56.32% |
| methods → hybrid_context100_mix0.5 → raw | 751 | 17.71% | 28.89% | 38.08% | 3 187 | 24 966 | 55.93% |
| methods → hybrid_context100_mix0.75 → raw | 751 | 17.58% | 28.76% | 37.68% | 3 187 | 25 405 | 56.32% |
| methods → hybrid_context100_mix1.0 → raw | 751 | 17.44% | 28.10% | 36.62% | 3 333 | 25 748 | 57.39% |

### development/hybrid_geographic_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → fusion50 → raw | 1184 | 9.46% | 17.15% | 23.82% | 6 855 | 31 965 | 68.33% |
| methods → hybrid_context30_mix0.25 → raw | 1184 | 9.71% | 17.31% | 24.24% | 6 825 | 31 317 | 67.82% |
| methods → hybrid_context30_mix0.5 → raw | 1184 | 10.05% | 17.40% | 24.83% | 6 784 | 31 317 | 66.98% |
| methods → hybrid_context30_mix0.75 → raw | 1184 | 9.80% | 17.23% | 24.16% | 7 195 | 31 750 | 67.99% |
| methods → hybrid_context30_mix1.0 → raw | 1184 | 9.80% | 17.06% | 23.48% | 8 376 | 31 898 | 69.51% |
| methods → hybrid_context100_mix0.25 → raw | 1184 | 9.88% | 17.31% | 24.32% | 6 660 | 31 192 | 67.31% |
| methods → hybrid_context100_mix0.5 → raw | 1184 | 10.14% | 17.48% | 24.75% | 6 838 | 31 317 | 66.55% |
| methods → hybrid_context100_mix0.75 → raw | 1184 | 9.97% | 17.40% | 24.41% | 7 370 | 31 417 | 67.65% |
| methods → hybrid_context100_mix1.0 → raw | 1184 | 9.80% | 16.72% | 23.31% | 8 404 | 31 084 | 69.68% |

### development/hybrid_top1_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → fusion50 → raw | 751 | 17.84% | 28.89% | 37.42% | 2 346 | 24 223 | 56.46% |
| methods → hybrid_context30_mix0.25 → raw | 751 | 17.58% | 28.63% | 37.95% | 2 395 | 24 480 | 55.79% |
| methods → hybrid_context30_mix0.5 → raw | 751 | 17.84% | 28.50% | 38.08% | 2 221 | 24 298 | 55.13% |
| methods → hybrid_context30_mix0.75 → raw | 751 | 17.44% | 28.36% | 37.55% | 2 221 | 24 298 | 54.99% |
| methods → hybrid_context30_mix1.0 → raw | 751 | 16.91% | 27.30% | 36.22% | 2 781 | 24 298 | 56.06% |
| methods → hybrid_context100_mix0.25 → raw | 751 | 17.84% | 29.03% | 38.48% | 2 217 | 24 254 | 55.39% |
| methods → hybrid_context100_mix0.5 → raw | 751 | 18.24% | 29.03% | 38.22% | 2 221 | 24 761 | 54.86% |
| methods → hybrid_context100_mix0.75 → raw | 751 | 17.84% | 28.50% | 38.08% | 1 680 | 24 254 | 54.19% |
| methods → hybrid_context100_mix1.0 → raw | 751 | 17.31% | 28.10% | 37.28% | 2 346 | 24 480 | 55.39% |

### development/hybrid_top1_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → fusion50 → raw | 1184 | 9.71% | 17.48% | 24.24% | 6 803 | 31 965 | 68.16% |
| methods → hybrid_context30_mix0.25 → raw | 1184 | 9.88% | 17.91% | 24.83% | 6 637 | 31 518 | 67.31% |
| methods → hybrid_context30_mix0.5 → raw | 1184 | 10.30% | 18.07% | 25.17% | 6 474 | 31 317 | 66.64% |
| methods → hybrid_context30_mix0.75 → raw | 1184 | 10.22% | 17.99% | 24.75% | 6 997 | 31 127 | 67.31% |
| methods → hybrid_context30_mix1.0 → raw | 1184 | 10.05% | 17.82% | 24.49% | 7 721 | 32 149 | 67.91% |
| methods → hybrid_context100_mix0.25 → raw | 1184 | 9.97% | 17.82% | 24.75% | 6 454 | 31 518 | 67.23% |
| methods → hybrid_context100_mix0.5 → raw | 1184 | 10.39% | 18.07% | 25.00% | 6 474 | 31 192 | 66.55% |
| methods → hybrid_context100_mix0.75 → raw | 1184 | 10.39% | 18.07% | 24.92% | 6 997 | 31 561 | 67.15% |
| methods → hybrid_context100_mix1.0 → raw | 1184 | 10.14% | 17.74% | 24.32% | 7 636 | 31 257 | 68.33% |

### development/localized_boq_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| methods → boq → raw | 751 | 11.72% | 19.57% | 25.70% | 8 174 | 27 183 | 68.31% |
| methods → fusion25 → raw | 751 | 14.11% | 23.17% | 30.36% | 5 222 | 25 171 | 63.91% |
| methods → fusion50 → raw | 751 | 13.72% | 23.44% | 30.09% | 4 636 | 24 340 | 62.85% |
| methods → fusion75 → raw | 751 | 12.65% | 21.97% | 28.50% | 5 682 | 24 949 | 64.85% |
| methods → zscore50 → raw | 751 | 14.25% | 24.10% | 30.63% | 4 231 | 23 895 | 61.25% |
| methods → rrf10 → raw | 751 | 13.58% | 23.17% | 29.83% | 5 417 | 25 526 | 63.52% |
| methods → rrf60 → raw | 751 | 10.52% | 18.91% | 25.97% | 7 129 | 26 755 | 67.51% |

### development/localized_boq_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |
| methods → boq → raw | 1184 | 4.90% | 8.02% | 10.98% | 14 213 | 36 252 | 83.11% |
| methods → fusion25 → raw | 1184 | 5.57% | 10.22% | 13.94% | 12 583 | 35 536 | 79.65% |
| methods → fusion50 → raw | 1184 | 5.74% | 10.39% | 14.36% | 12 469 | 35 154 | 78.55% |
| methods → fusion75 → raw | 1184 | 5.74% | 10.22% | 14.10% | 12 249 | 35 393 | 79.14% |
| methods → zscore50 → raw | 1184 | 6.25% | 10.56% | 14.61% | 11 986 | 34 825 | 78.72% |
| methods → rrf10 → raw | 1184 | 5.66% | 9.88% | 14.19% | 12 545 | 35 402 | 79.05% |
| methods → rrf60 → raw | 1184 | 4.81% | 8.87% | 12.42% | 12 816 | 34 656 | 80.91% |

### development/localized_context_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → full_preencoder → raw | 751 | 14.25% | 20.11% | 25.83% | 7 826 | 27 140 | 68.58% |
| methods → context30_mix0.25 → raw | 751 | 13.98% | 20.11% | 25.57% | 7 482 | 27 513 | 68.44% |
| methods → context30_mix0.5 → raw | 751 | 13.98% | 20.77% | 26.50% | 7 660 | 26 648 | 67.64% |
| methods → context30_mix0.75 → raw | 751 | 14.25% | 21.70% | 27.16% | 7 068 | 26 621 | 66.58% |
| methods → context30_mix1.0 → raw | 751 | 13.58% | 20.91% | 26.23% | 7 693 | 27 649 | 67.38% |
| methods → context100_mix0.25 → raw | 751 | 13.98% | 20.24% | 25.97% | 7 660 | 27 086 | 67.78% |
| methods → context100_mix0.5 → raw | 751 | 13.85% | 20.64% | 26.36% | 7 715 | 26 919 | 67.38% |
| methods → context100_mix0.75 → raw | 751 | 14.11% | 21.30% | 27.43% | 7 068 | 26 468 | 66.18% |
| methods → context100_mix1.0 → raw | 751 | 14.25% | 21.57% | 27.16% | 6 833 | 26 611 | 66.05% |

### development/localized_context_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → full_preencoder → raw | 1184 | 5.74% | 9.80% | 12.33% | 13 275 | 34 674 | 82.85% |
| methods → context30_mix0.25 → raw | 1184 | 5.66% | 9.63% | 12.25% | 13 155 | 34 484 | 82.77% |
| methods → context30_mix0.5 → raw | 1184 | 5.74% | 9.71% | 12.50% | 13 545 | 34 644 | 82.52% |
| methods → context30_mix0.75 → raw | 1184 | 5.74% | 9.88% | 12.58% | 13 825 | 35 194 | 82.52% |
| methods → context30_mix1.0 → raw | 1184 | 5.49% | 9.63% | 12.84% | 13 493 | 35 597 | 82.26% |
| methods → context100_mix0.25 → raw | 1184 | 5.83% | 9.80% | 12.42% | 13 270 | 34 627 | 82.77% |
| methods → context100_mix0.5 → raw | 1184 | 5.83% | 9.88% | 12.75% | 13 155 | 34 674 | 82.18% |
| methods → context100_mix0.75 → raw | 1184 | 5.91% | 9.97% | 12.75% | 13 048 | 34 674 | 82.26% |
| methods → context100_mix1.0 → raw | 1184 | 6.00% | 9.80% | 12.84% | 13 285 | 36 041 | 82.09% |

### development/localized_dba_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| methods → visual_dba3 → raw | 751 | 13.98% | 22.50% | 29.69% | 6 740 | 25 162 | 65.11% |
| methods → visual_dba3_aqe3 → raw | 751 | 13.58% | 21.30% | 27.30% | 8 163 | 25 961 | 68.44% |
| methods → visual_dba10 → raw | 751 | 14.11% | 22.64% | 29.69% | 6 539 | 26 375 | 65.38% |
| methods → visual_dba10_aqe3 → raw | 751 | 13.18% | 20.51% | 26.50% | 8 189 | 27 049 | 69.24% |
| methods → geo_dba3 → raw | 751 | 14.11% | 22.50% | 29.69% | 6 563 | 25 162 | 64.85% |
| methods → geo_dba10 → raw | 751 | 14.51% | 22.90% | 29.83% | 6 740 | 25 836 | 65.11% |

### development/localized_edtformer_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| methods → edtformer → raw | 751 | 9.99% | 17.31% | 24.23% | 6 945 | 25 836 | 69.24% |
| methods → fusion25 → raw | 751 | 13.72% | 23.04% | 29.96% | 5 393 | 24 671 | 64.18% |
| methods → fusion50 → raw | 751 | 12.92% | 21.97% | 29.56% | 5 165 | 25 739 | 64.45% |
| methods → fusion75 → raw | 751 | 11.45% | 19.57% | 27.03% | 5 325 | 25 058 | 66.31% |
| methods → zscore50 → raw | 751 | 14.11% | 22.50% | 29.83% | 5 325 | 25 092 | 63.91% |
| methods → rrf10 → raw | 751 | 11.85% | 19.97% | 27.30% | 5 987 | 25 836 | 66.44% |
| methods → rrf60 → raw | 751 | 9.32% | 17.18% | 24.10% | 7 405 | 26 177 | 69.64% |

### development/localized_exact_faiss_baseline_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |

### development/localized_global_sage-full-features_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| methods → sage-full-features → raw | 751 | 14.25% | 20.11% | 25.83% | 7 826 | 27 140 | 68.58% |
| methods → fusion25 → raw | 751 | 15.18% | 23.97% | 31.42% | 5 164 | 24 254 | 63.25% |
| methods → fusion50 → raw | 751 | 14.91% | 23.83% | 31.16% | 4 723 | 25 194 | 62.58% |
| methods → fusion75 → raw | 751 | 15.05% | 23.04% | 29.96% | 5 897 | 26 090 | 64.18% |
| methods → zscore50 → raw | 751 | 15.85% | 24.63% | 31.82% | 5 267 | 23 904 | 62.18% |
| methods → rrf10 → raw | 751 | 14.51% | 22.24% | 29.83% | 5 327 | 25 836 | 64.31% |
| methods → rrf60 → raw | 751 | 11.45% | 18.24% | 25.03% | 7 388 | 25 836 | 68.97% |

### development/localized_global_sage-full-features_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |
| methods → sage-full-features → raw | 1184 | 5.74% | 9.80% | 12.33% | 13 275 | 34 674 | 82.85% |
| methods → fusion25 → raw | 1184 | 5.91% | 10.47% | 14.44% | 12 127 | 34 218 | 79.65% |
| methods → fusion50 → raw | 1184 | 6.00% | 10.73% | 14.53% | 12 326 | 34 684 | 79.73% |
| methods → fusion75 → raw | 1184 | 5.91% | 10.22% | 13.26% | 12 607 | 34 199 | 81.50% |
| methods → zscore50 → raw | 1184 | 6.33% | 10.73% | 14.61% | 12 565 | 34 974 | 79.90% |
| methods → rrf10 → raw | 1184 | 5.83% | 10.22% | 13.43% | 12 560 | 35 123 | 80.83% |
| methods → rrf60 → raw | 1184 | 4.90% | 8.78% | 11.99% | 12 326 | 34 133 | 82.43% |

### development/localized_sage_vitb_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |

### development/localized_sage_vitl_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| methods → sage-vitl → raw | 751 | 16.64% | 25.97% | 34.22% | 4 151 | 25 405 | 58.59% |
| methods → fusion25 → raw | 751 | 15.18% | 24.10% | 31.96% | 5 098 | 25 836 | 62.32% |
| methods → fusion50 → raw | 751 | 16.25% | 25.30% | 33.29% | 3 948 | 24 966 | 59.79% |
| methods → fusion75 → raw | 751 | 16.91% | 26.10% | 34.22% | 4 224 | 25 486 | 58.59% |
| methods → zscore50 → raw | 751 | 16.38% | 25.03% | 33.02% | 3 808 | 24 761 | 59.79% |
| methods → rrf10 → raw | 751 | 15.05% | 24.23% | 31.56% | 4 926 | 24 761 | 61.38% |
| methods → rrf60 → raw | 751 | 12.38% | 21.57% | 28.89% | 6 562 | 26 301 | 64.58% |

### development/localized_sage_vitl_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage → raw | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |
| methods → sage-vitl → raw | 1184 | 6.42% | 11.66% | 16.39% | 12 675 | 35 206 | 77.79% |
| methods → fusion25 → raw | 1184 | 5.91% | 10.73% | 15.12% | 12 335 | 35 290 | 79.05% |
| methods → fusion50 → raw | 1184 | 6.59% | 11.99% | 16.72% | 11 960 | 34 548 | 77.20% |
| methods → fusion75 → raw | 1184 | 6.84% | 11.99% | 16.55% | 11 766 | 34 152 | 77.53% |
| methods → zscore50 → raw | 1184 | 6.76% | 11.91% | 16.30% | 12 229 | 35 184 | 77.96% |
| methods → rrf10 → raw | 1184 | 6.67% | 11.82% | 16.13% | 12 484 | 35 020 | 78.21% |
| methods → rrf60 → raw | 1184 | 5.66% | 10.81% | 15.20% | 12 335 | 35 138 | 78.63% |

### development/metric_adaptation/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → sage → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → ridge0.01_mix0.1 → raw_top1 | 751 | 14.91% | 22.90% | 29.69% | 7 050 | 26 375 | 65.65% |
| metrics → ridge0.01_mix0.25 → raw_top1 | 751 | 14.51% | 22.24% | 28.76% | 7 123 | 25 836 | 66.44% |
| metrics → ridge0.01_mix0.5 → raw_top1 | 751 | 12.52% | 18.38% | 23.04% | 9 748 | 28 453 | 73.77% |
| metrics → ridge0.1_mix0.1 → raw_top1 | 751 | 14.91% | 22.77% | 29.69% | 7 050 | 26 375 | 65.65% |
| metrics → ridge0.1_mix0.25 → raw_top1 | 751 | 14.51% | 22.24% | 28.76% | 7 123 | 25 772 | 66.44% |
| metrics → ridge0.1_mix0.5 → raw_top1 | 751 | 12.38% | 18.24% | 23.04% | 9 987 | 28 512 | 73.77% |
| metrics → ridge1.0_mix0.1 → raw_top1 | 751 | 14.78% | 22.50% | 29.56% | 7 089 | 25 836 | 65.65% |
| metrics → ridge1.0_mix0.25 → raw_top1 | 751 | 13.98% | 21.84% | 28.10% | 7 745 | 26 375 | 67.24% |
| metrics → ridge1.0_mix0.5 → raw_top1 | 751 | 12.12% | 17.98% | 22.90% | 10 465 | 28 226 | 74.57% |
| metrics → ridge10.0_mix0.1 → raw_top1 | 751 | 14.78% | 22.50% | 29.43% | 7 129 | 25 772 | 65.78% |
| metrics → ridge10.0_mix0.25 → raw_top1 | 751 | 14.11% | 22.24% | 28.36% | 7 356 | 24 761 | 67.24% |
| metrics → ridge10.0_mix0.5 → raw_top1 | 751 | 11.45% | 16.64% | 20.24% | 11 471 | 28 749 | 76.96% |

### development/mode_experiment/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | 751 | 14.11% | 22.50% | 29.69% | 6 354 | 25 162 | 65.11% |
| top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| candidates → legacy_weighted_medoid → raw | 751 | 12.38% | 19.04% | 24.10% | 8 776 | 26 024 | 71.11% |
| candidates → rank_weighted_vote → raw | 751 | 14.78% | 23.30% | 29.43% | 7 129 | 26 375 | 65.78% |
| candidates → sequence_deduplicated_vote → raw | 751 | 14.65% | 23.04% | 29.69% | 7 089 | 26 432 | 65.51% |
| candidates → density_aware_mode_vote → raw | 751 | 14.11% | 22.64% | 29.69% | 6 539 | 25 162 | 64.98% |
| candidates → logistic → raw | 751 | 14.65% | 23.04% | 29.83% | 7 089 | 26 432 | 65.25% |
| candidates → hgb_leaf7 → raw | 751 | 14.38% | 22.77% | 29.29% | 7 246 | 26 375 | 65.65% |
| candidates → hgb_leaf15 → raw | 751 | 14.51% | 22.90% | 29.69% | 7 278 | 26 375 | 65.51% |
| candidates → hgb_leaf31 → raw | 751 | 14.51% | 22.90% | 29.69% | 7 246 | 26 383 | 65.51% |

### development/query_views/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → original → raw_top1 | 751 | 14.78% | 23.04% | 29.83% | 7 123 | 26 038 | 65.38% |
| metrics → rot90 → raw_top1 | 751 | 0.40% | 0.53% | 0.67% | 15 194 | 27 187 | 98.93% |
| metrics → rot180 → raw_top1 | 751 | 1.73% | 2.40% | 2.53% | 15 766 | 28 244 | 96.27% |
| metrics → rot270 → raw_top1 | 751 | 0.13% | 0.27% | 0.40% | 15 188 | 25 634 | 98.93% |
| metrics → center75 → raw_top1 | 751 | 13.98% | 21.44% | 28.36% | 6 851 | 26 717 | 65.78% |
| metrics → upper70 → raw_top1 | 751 | 14.38% | 21.84% | 28.89% | 6 764 | 25 836 | 64.98% |
| metrics → rotation_max → raw_top1 | 751 | 10.65% | 15.58% | 18.64% | 10 987 | 23 937 | 79.09% |
| metrics → original_upper_max → raw_top1 | 751 | 15.05% | 22.64% | 29.43% | 6 562 | 25 496 | 64.98% |
| metrics → all_views_max → raw_top1 | 751 | 10.65% | 15.98% | 20.37% | 10 286 | 24 362 | 77.10% |
| metrics → original_center_mean → raw_top1 | 751 | 14.25% | 22.10% | 28.63% | 6 606 | 25 486 | 64.98% |
| metrics → aqe3 → raw_top1 | 751 | 14.78% | 23.17% | 29.96% | 7 089 | 26 498 | 65.25% |
| metrics → aqe5 → raw_top1 | 751 | 14.78% | 23.17% | 29.83% | 7 123 | 26 375 | 65.38% |

### development/reference_adapted_sage-vitl_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → unadapted → raw_top1 | 751 | 16.38% | 25.43% | 34.09% | 4 236 | 24 540 | 58.19% |
| metrics → step100_strength0.25 → raw_top1 | 751 | 16.38% | 25.70% | 33.82% | 4 051 | 23 885 | 57.92% |
| metrics → step100_strength0.5 → raw_top1 | 751 | 16.25% | 25.03% | 33.42% | 4 551 | 24 254 | 59.25% |
| metrics → step100_strength1.0 → raw_top1 | 751 | 15.71% | 23.44% | 31.56% | 6 005 | 24 575 | 61.38% |
| metrics → step300_strength0.25 → raw_top1 | 751 | 16.64% | 25.57% | 33.69% | 3 808 | 24 540 | 57.92% |
| metrics → step300_strength0.5 → raw_top1 | 751 | 16.91% | 25.30% | 33.16% | 4 355 | 25 082 | 58.85% |
| metrics → step300_strength1.0 → raw_top1 | 751 | 15.18% | 22.64% | 29.56% | 6 815 | 25 982 | 63.65% |
| metrics → step600_strength0.25 → raw_top1 | 751 | 16.64% | 25.83% | 34.09% | 4 167 | 24 951 | 58.06% |
| metrics → step600_strength0.5 → raw_top1 | 751 | 16.64% | 25.03% | 32.76% | 4 676 | 25 128 | 59.25% |
| metrics → step600_strength1.0 → raw_top1 | 751 | 15.71% | 23.30% | 30.76% | 6 561 | 25 430 | 62.32% |

### development/reference_adapted_sage-vitl_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| metrics → unadapted → raw_top1 | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |
| metrics → step100_strength0.25 → raw_top1 | 1184 | 6.67% | 11.82% | 16.30% | 12 880 | 35 584 | 78.04% |
| metrics → step100_strength0.5 → raw_top1 | 1184 | 6.76% | 11.82% | 16.30% | 13 500 | 35 934 | 78.38% |
| metrics → step100_strength1.0 → raw_top1 | 1184 | 6.42% | 11.06% | 15.20% | 13 522 | 35 645 | 80.15% |
| metrics → step300_strength0.25 → raw_top1 | 1184 | 6.76% | 11.99% | 16.39% | 12 637 | 35 584 | 77.87% |
| metrics → step300_strength0.5 → raw_top1 | 1184 | 6.84% | 11.99% | 16.22% | 12 565 | 36 312 | 78.29% |
| metrics → step300_strength1.0 → raw_top1 | 1184 | 6.00% | 10.81% | 14.53% | 13 182 | 36 119 | 81.17% |
| metrics → step600_strength0.25 → raw_top1 | 1184 | 6.59% | 11.91% | 16.30% | 12 805 | 36 115 | 77.96% |
| metrics → step600_strength0.5 → raw_top1 | 1184 | 6.59% | 11.91% | 16.13% | 12 596 | 36 883 | 78.63% |
| metrics → step600_strength1.0 → raw_top1 | 1184 | 5.91% | 10.90% | 14.53% | 13 857 | 37 544 | 80.49% |

### development/top1_sage_vitl_historical/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitl → raw | 751 | 16.38% | 25.43% | 34.09% | 4 236 | 24 540 | 58.19% |

### development/top1_sage_vitl_new/summary.json

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| methods → sage-vitl → raw | 1184 | 6.67% | 11.74% | 16.05% | 12 604 | 35 584 | 78.12% |

## V6: фиксированная gallery scaling

Полная таблица с source counts, sequences, H3, heading, coverage, всеми R@K и positive ranks: [результаты v6](/Users/Daniil/Documents/VSCode/geosnap/docs/gallery_scale_v6_results.md). Четыре варианта: G0, G1_random, G1_smart, G2_smart.

## V7: 20 первоначальных экспериментальных вариантов

Это исходный промежуточный leaderboard: он сохранён как история. Дальнейшие additions и общий карантин перечислены отдельно ниже; итоговый победитель берётся из candidate_frozen.json, а не из поля candidate этого промежуточного файла.

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| scale_mean_context_added (gallery 102944) | 1184 | 14.70% | 24.41% | 31.76% | 4 612 | 30 306 | 60.90% |
| scale_mean_added (gallery 102944) | 1184 | 14.19% | 23.73% | 31.00% | 5 219 | 31 474 | 61.57% |
| query_square_crop_mean3 (gallery 102944) | 1184 | 14.27% | 23.31% | 30.49% | 5 848 | 30 596 | 61.99% |
| gallery_addition (gallery 102944) | 1184 | 14.27% | 23.14% | 30.41% | 5 833 | 30 778 | 62.84% |
| scale_mean_context (gallery 100000) | 1184 | 13.77% | 22.97% | 30.32% | 5 224 | 31 016 | 62.58% |
| query322_504_mean (gallery 100000) | 1184 | 13.26% | 22.30% | 29.48% | 5 622 | 32 039 | 63.18% |
| context (gallery 100000) | 1184 | 13.85% | 22.21% | 29.48% | 6 014 | 30 878 | 63.85% |
| diverse_context (gallery 100000) | 1184 | 13.60% | 22.13% | 29.48% | 6 164 | 30 878 | 64.36% |
| fol_learned_oof (gallery 100000) | 1184 | 13.60% | 21.96% | 29.14% | 5 405 | 30 805 | 63.18% |
| panorama4 (gallery 100000) | 1184 | 13.43% | 21.88% | 28.97% | 6 505 | 31 124 | 64.61% |
| baseline (gallery 100000) | 1184 | 13.43% | 21.88% | 28.97% | 6 505 | 31 124 | 64.61% |
| reference_density (gallery 100000) | 1184 | 13.34% | 21.79% | 28.97% | 6 012 | 29 844 | 64.61% |
| place_equal_blend (gallery 100000) | 1184 | 13.77% | 21.71% | 28.97% | 6 533 | 31 026 | 64.44% |
| reference_centered (gallery 100000) | 1184 | 13.18% | 21.71% | 28.72% | 6 655 | 31 196 | 64.95% |
| query504 (gallery 100000) | 1184 | 12.75% | 21.71% | 28.72% | 5 885 | 31 972 | 63.94% |
| fol_blend (gallery 100000) | 1184 | 13.43% | 21.45% | 28.55% | 5 211 | 30 429 | 63.77% |
| place_prototype (gallery 100000) | 1184 | 13.51% | 21.62% | 28.38% | 6 485 | 31 497 | 64.86% |
| query_square_crop_only (gallery 102944) | 1184 | 13.60% | 21.45% | 27.45% | 7 446 | 32 845 | 66.30% |
| fol_global_in_prefix (gallery 100000) | 1184 | 11.15% | 18.24% | 24.16% | 6 918 | 32 110 | 67.40% |
| fol_mnn (gallery 100000) | 1184 | 9.97% | 16.81% | 22.55% | 6 764 | 30 276 | 68.24% |

| Вариант | R@1 | R@5 | R@10 | R@20 | R@50 | R@100 |
|---|---:|---:|---:|---:|---:|---:|
| scale_mean_context_added | 31.76% | 38.68% | 41.05% | 43.33% | 46.54% | 49.66% |
| scale_mean_added | 31.00% | 38.01% | 40.79% | 42.40% | 46.54% | 49.66% |
| query_square_crop_mean3 | 30.49% | 37.75% | 40.12% | 42.57% | 46.37% | 49.32% |
| gallery_addition | 30.41% | 36.57% | 39.70% | 42.15% | 45.86% | 49.16% |
| scale_mean_context | 30.32% | 37.08% | 38.85% | 41.47% | 43.83% | 47.30% |
| query322_504_mean | 29.48% | 36.40% | 38.60% | 40.54% | 43.83% | 47.30% |
| context | 29.48% | 36.15% | 37.92% | 40.37% | 43.50% | 46.54% |
| diverse_context | 29.48% | 35.81% | 37.92% | 40.54% | 43.50% | 46.54% |
| fol_learned_oof | 29.14% | 35.98% | 37.33% | 40.37% | 43.50% | 46.54% |
| panorama4 | 28.97% | 35.14% | 37.67% | 40.03% | 43.58% | 46.62% |
| baseline | 28.97% | 35.14% | 37.67% | 40.03% | 43.50% | 46.54% |
| reference_density | 28.97% | 36.06% | 38.09% | 39.95% | 44.00% | 46.62% |
| place_equal_blend | 28.97% | 34.80% | 36.99% | 39.53% | 43.16% | 46.11% |
| reference_centered | 28.72% | 35.30% | 37.67% | 39.86% | 43.92% | 46.79% |
| query504 | 28.72% | 36.06% | 38.26% | 40.54% | 43.75% | 46.28% |
| fol_blend | 28.55% | 35.90% | 38.18% | 40.37% | 43.50% | 46.54% |
| place_prototype | 28.38% | 33.61% | 36.23% | 38.09% | 41.22% | 44.76% |
| query_square_crop_only | 27.45% | 34.80% | 36.66% | 39.10% | 43.41% | 47.30% |
| fol_global_in_prefix | 24.16% | 32.94% | 37.16% | 40.37% | 43.50% | 46.54% |
| fol_mnn | 22.55% | 32.52% | 36.15% | 40.37% | 43.50% | 46.54% |

## V7: подтверждённые результаты общего карантина и последних добавлений

RAW ≤100 м для top-1-coordinate estimator совпадает с R@1. Остальные RAW-метрики промежуточных результатов не восстановлены из отсутствующих внешних файлов; они доступны в полном внешнем отчёте. Здесь приведены значения, непосредственно сохранённые в локальной финальной фиксации.

| Вариант | R@1 / RAW100 | R@5 | R@10 | R@20 | R@50 | R@100 | Нет покрытия | Retrieval miss | Неверный top-1 | Правильно |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| quarantine_G2_top1 | 28.97% | 35.22% | 37.67% | 40.03% | 43.50% | 46.54% | 285 | 348 | 208 | 343 |
| quarantine_G2_mean | 29.48% | 36.40% | 38.60% | 40.54% | 43.83% | 47.30% | 285 | 339 | 211 | 349 |
| quarantine_G2_context | 30.32% | 37.08% | 38.85% | 41.47% | 43.83% | 47.30% | 285 | 339 | 201 | 359 |
| quarantine_first_top1 | 30.41% | 36.66% | 39.70% | 42.15% | 45.86% | 49.16% | 270 | 332 | 222 | 360 |
| quarantine_first_mean | 31.00% | 38.01% | 40.79% | 42.40% | 46.54% | 49.66% | 270 | 326 | 221 | 367 |
| quarantine_first_context | 31.76% | 38.68% | 41.05% | 43.33% | 46.54% | 49.66% | 270 | 326 | 212 | 376 |
| quarantine_expanded2_top1 | 32.60% | 38.34% | 40.54% | 43.58% | 46.96% | 50.42% | 266 | 321 | 211 | 386 |
| quarantine_expanded2_mean | 33.19% | 39.44% | 42.15% | 43.75% | 47.89% | 51.10% | 266 | 313 | 212 | 393 |
| quarantine_expanded2_context | 33.61% | 40.20% | 42.31% | 44.34% | 47.89% | 51.10% | 266 | 313 | 207 | 398 |
| licensed_top1 | 29.39% | 35.22% | 37.50% | 40.62% | 44.59% | 48.06% | 284 | 331 | 221 | 348 |
| licensed_mean | 29.90% | 36.15% | 38.94% | 41.39% | 45.19% | 49.24% | 284 | 317 | 229 | 354 |
| licensed_context | 30.15% | 36.91% | 39.10% | 41.55% | 45.19% | 49.24% | 284 | 317 | 226 | 357 |
| tranche3_top1 | 33.36% | 39.27% | 41.81% | 44.34% | 47.72% | 50.93% | 262 | 319 | 208 | 395 |
| tranche3_mean | 34.29% | 40.12% | 42.82% | 44.76% | 48.73% | 51.77% | 262 | 309 | 207 | 406 |
| tranche3_context | 34.71% | 40.96% | 43.50% | 45.19% | 48.73% | 51.77% | 262 | 309 | 202 | 411 |
| licensed_third_top1 | 30.74% | 36.82% | 39.10% | 41.89% | 45.78% | 49.24% | 275 | 326 | 219 | 364 |
| licensed_third_mean | 31.67% | 37.42% | 40.29% | 42.65% | 46.62% | 50.34% | 275 | 313 | 221 | 375 |
| licensed_third_context | 32.09% | 38.34% | 40.54% | 42.57% | 46.62% | 50.34% | 275 | 313 | 216 | 380 |

## Полный RAW итогового кандидата и production

| Вариант / путь к метрике | N | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м |
|---|---:|---:|---:|---:|---:|---:|---:|
| Production, geographic policy | 1184 | 5.57% | 9.63% | 13.77% | 12 885 | 35 266 | 81.08% |
| Production, top-1 | 1184 | 5.74% | 9.80% | 13.85% | 13 423 | 36 306 | 81.42% |
| tranche3_context | 1184 | 17.15% | 26.86% | 34.71% | 3 803 | 30 512 | 58.36% |

## Вспомогательные и географические опыты

- [Сводка multi-photo, включая collage](/Users/Daniil/Documents/VSCode/geosnap/docs/research_summary_20260910.md). Исходный auxiliary report: [multiview_report.md](/Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/multiview_report.md).
- [Уличная метрика: локальный JSON](/Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/map/street_summary.json).
- [Интерактивное сравнение географического покрытия](http://127.0.0.1:57315/comparison.html).

## Источники и проверка

Проверены локальный hash финальной фиксации, численность запросов и декомпозиции, соответствие R@1 правильным ответам, полнота 423 записей v5 и 20 первоначальных ночных вариантов. Большие файлы на отключённом томе заново не проверялись.

- [development_experiment_index.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_research_v5/candidate_reports/development_experiment_index.json>) — SHA-256 `022384a605953980493b6dfb0217892a27785a05f451ccf53fd145d5710888be`.

- [candidate_frozen.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/candidate_frozen.json>) — SHA-256 `6ea6383b8894ec0ef502215c166ec2c688de07980cb6e5f3e85e0b6c3f809e83`.

- [leaderboard.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/leaderboard.json>) — SHA-256 `42c627edaba40d9f8739cc3f52298a1939425df5ac3172088d71935f24c1dbb5`.

- [street_summary.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/map/street_summary.json>) — SHA-256 `b01a81a742955b45ae195047a15e00f31f025a2b9ab354ffdd72ad7519a68029`.

- [comparison.html](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/map/comparison.html>) — SHA-256 `56ae63619ff41d0b1ee7004febc995f580dfc0d6c4d65aede0b4faa213bfe119`.

- [megaloc_development_top50_source.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_real_v3/reports/megaloc_development_top50_source.json>) — SHA-256 `848a4267bf826cb68959e1a965471e3e0f8249c0c8833ef55e7a2b513e77ec66`.

- [salad_development_top50_source.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_real_v3/reports/salad_development_top50_source.json>) — SHA-256 `cc2a8687dc5dd0c49e56a9ff90a792239aa8ba04ed31c4d911cb4cfd02019057`.

- [sage_development_top50_source.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_real_v3/reports/sage_development_top50_source.json>) — SHA-256 `2088c8789b75c5315711fa2d6d5611a33636e0d9b9ce05757febafab7f908955`.

- [selavprplusplus_base_development_top50_source.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_real_v3/reports/selavprplusplus_base_development_top50_source.json>) — SHA-256 `480c6aeaffef6618f2c1e38313ff0e99471e44d74e942add9ae56c4528dd311f`.

- [selavprplusplus_rerank_development_top50_source.json](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_real_v3/reports/selavprplusplus_rerank_development_top50_source.json>) — SHA-256 `4e836871566496487261c06ea8e07823b1337d8b72e70b68e9b5e32c0c31db54`.

- [research_v5.md](</Users/Daniil/Documents/VSCode/geosnap/docs/research_v5.md>) — SHA-256 `2f2db380a890828c4f03febadfa674c38a369b084978899756148deadf584271`.

- [gallery_scale_v6_results.md](</Users/Daniil/Documents/VSCode/geosnap/docs/gallery_scale_v6_results.md>) — SHA-256 `0c44f86cd3685a361a48d9880b50b2f8a0fe823ac908e5d17a36895d0261b166`.

- [night_v7_20260909.md](</Users/Daniil/Documents/VSCode/geosnap/docs/night_v7_20260909.md>) — SHA-256 `301db2492dc8764942c2cba42dcee564899629c2027a4e05c601a439f70de7a1`.

- [multiview_report.md](</Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_night_v7/multiview_report.md>) — SHA-256 `608bfadeeebefc71b71e446213c01fd800f61759d54b92a90dbe259811e1c3d2`.
