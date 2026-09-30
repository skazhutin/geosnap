# GeoSnap: фиксированный SAGE-L, размер и состав галереи

Canonical store: `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/full_mapillary`.

Development: 1 184 неизменённых запроса. Exact cosine, координаты top-1, без abstention. MSLS — research-only; production не изменён. Calibration/final не открывались в этом этапе.

| Метрика | G0 | G1_random | G1_smart | G2_smart |
| --- | --- | --- | --- | --- |
| References | 31084 | 60000 | 60000 | 100000 |
| Mapillary / KartaView / MSLS | 22321 / 8763 / 0 | 22781 / 8768 / 28451 | 23931 / 8770 / 27299 | 24052 / 8780 / 67168 |
| Sequences / H3-9 cells | 21971 / 8489 | 25537 / 8627 | 26708 / 8660 | 26913 / 8660 |
| Median refs/cell / mean heading bins/cell | 2.0 / 2.12 | 3.0 / 2.30 | 3.0 / 2.36 | 3.0 / 2.38 |
| Heading available, % | 99.62 | 99.80 | 99.80 | 99.88 |
| Coverage ≤25 / 50 / 100 m, % | 25.93 / 47.97 / 74.16 | 32.18 / 53.29 / 75.68 | 32.85 / 54.22 / 75.84 | 33.87 / 54.48 / 75.93 |
| Coverage ≤100 m + heading ≤45°, % | 47.47 | 51.44 | 52.79 | 52.96 |
| R@1 / 5 / 10, % | 23.82 / 30.57 / 33.19 | 27.70 / 33.45 / 36.23 | 28.38 / 35.30 / 37.75 | 28.97 / 35.14 / 37.67 |
| R@20 / 50 / 100, % | 35.98 / 40.12 / 43.75 | 38.77 / 42.91 / 46.11 | 40.12 / 44.17 / 47.38 | 40.03 / 43.50 / 46.54 |
| Positive rank median / p75 / p90, all queries | 424.0 / ∞ / ∞ | 237.5 / 49,272.5 / ∞ | 177.0 / 45,891.2 / ∞ | 223.0 / 73,948.0 / ∞ |
| Positive rank median / p75 / p90, covered only | 26.0 / 1,008.0 / 6,398.1 | 16.5 / 1,048.0 / 8,060.0 | 11.5 / 853.8 / 7,435.9 | 12.0 / 1,207.5 / 10,619.6 |
| RAW ≤25 / 50 / 100 m, % | 9.97 / 17.40 / 23.82 | 12.33 / 21.03 / 27.70 | 12.67 / 21.20 / 28.38 | 13.43 / 21.88 / 28.97 |
| Median / p90 error, m | 8759 / 32691 | 6826 / 31399 | 6815 / 31124 | 6505 / 31124 |
| >500 m, % | 69.26 | 65.62 | 65.20 | 64.61 |
| No coverage / retrieval miss / wrong top1 | 306 / 360 / 236 | 288 / 350 / 218 | 286 / 337 / 225 | 285 / 348 / 208 |

∞ включает запросы без nearby reference; они остаются в denominator. Wrong top1 означает положительный reference на позициях 2–100; retrieval miss — положительный reference ниже 100.

Удалено проверенных локальных копий: 53.19 GB (логические байты).

Полные метрики, paired geographic bootstrap, hashes и ограничения: `report.json` рядом с `manifests/`, `descriptors/`, `results/`.

После этого этапа новые модели, confidence, reranking, обучение и production switch не запускались.

Исходный store: {'.zip': 9, '.jpg': 38356, '.csv': 276, '.md5': 1, '.txt': 1}. Точное AOI: 249 190 кадров MSLS в доступных архивах, 225 panorama flags; это число до image/leakage-аудита, а не размер usable pool.

Новые допустимые references после аудита: {'mapillary': 1747, 'kartaview': 22, 'msls': 105560}; G0: {'kartaview': 8763, 'mapillary': 22321}. MSLS помечен research-only в каждой строке.

Проверенные локальные копии удалены из следующих каталогов; старые пути сохранены symlink:

- `/Users/Daniil/Documents/VSCode/geosnap/data/embeddings/moscow` — 0.712 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/embeddings/moscow`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/embeddings/moscow_real_v2` — 1.476 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/embeddings/moscow_real_v2`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/embeddings/moscow_real_v3` — 2.807 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/embeddings/moscow_real_v3`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/embeddings/moscow_research_v5` — 5.130 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/embeddings/moscow_research_v5`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/indexes/moscow` — 0.710 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/indexes/moscow`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/indexes/moscow_real_v2` — 0.736 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/indexes/moscow_real_v2`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/indexes/moscow_real_v3` — 0.758 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/indexes/moscow_real_v3`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/models/research_v5` — 2.279 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/models/research_v5`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/models/selavprplusplus` — 0.997 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/models/selavprplusplus`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/models/cricavpr` — 0.589 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/models/cricavpr`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/evaluation/moscow_research_v5/development` — 2.159 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/legacy_data/data/evaluation/moscow_research_v5/development`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/raw/moscow/images/mapillary` — 5.709 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/full_mapillary/geosnap/mapillary`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/raw/moscow/images/kartaview` — 4.800 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/full_mapillary/geosnap/kartaview`.
- `/Users/Daniil/Documents/VSCode/geosnap/data/raw/moscow/images/msls` — 2.231 GB; canonical `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/full_mapillary/geosnap/msls`.
- `/Users/Daniil/Downloads/An-x6BOZeUDmFXVxCKtosZWgMZzQiLR1iBxkevoDhNKLyXwMULUhqUB5pExN3EDE7RfPewnC6_Omad8kkRV1vwyOhWXqOJp3-3d6hWARSf_41taXCsAlVF1gp5gf2S7W.zip` — 11.034 GB; проверенная копия архива на внешнем томе.
- `/Users/Daniil/Downloads/An9e0HGaRi-9kM8QyF5wNHyA-DVxI_C_aN9rC3iAXHLN9_RoW9P8SUHRR39AeszPqegQnqk-LL49sYIjsAIdS23yl9rwu1NPOdDbjFmvzlTYERwsxv6nAObVUBNOjDrN.zip` — 10.879 GB; проверенная копия архива на внешнем томе.
- `/Users/Daniil/Downloads/An9znN6Evsbp2KNZvdYc0NsYCk961Vy0u_j6ACpZ_QoylW800rBKCSeZQAq765BP03K_qyPpPK8aCNU6wnVa44M6cmx4X-iTJVQ8zCwaH5BJom47I8Xr25XLTw.zip` — 0.181 GB; проверенная копия архива на внешнем томе.

Panorama A/B пропущен: в контрольной галерее подтверждено 43 equirectangular parents, минимум был заранее установлен в 100.

Основная группа ошибок (G2_smart): положительный reference не попадает в top-100 — 348/1184.

Следующий наиболее ценный шаг: Провести один контролируемый тест улучшения retriever на этой фиксированной галерее и development-выборке. Он не запущен.

Manifests: `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/gallery_scale_v6/manifests`; descriptors: `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/gallery_scale_v6/descriptors` и `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/gallery_scale_v6/descriptor_pool.json`; отчёт: `/Users/Daniil/.mounty/Новый том/mac os/GeoSnap/gallery_scale_v6/report.json`.

Парные geographic-bootstrap интервалы (5000 повторов; значения уже в процентных пунктах):

```json
{
  "G0_to_G1_smart": {
    "gain_pp": 4.5608108108108105,
    "ci95_pp": [
      2.9157524288516283,
      6.210063187679836
    ],
    "groups": 80,
    "resamples": 5000
  },
  "G1_random_to_G1_smart": {
    "gain_pp": 0.6756756756756757,
    "ci95_pp": [
      -0.2669277452974072,
      1.6825261785617323
    ],
    "groups": 80,
    "resamples": 5000
  },
  "G1_smart_to_G2_smart": {
    "gain_pp": 0.5912162162162162,
    "ci95_pp": [
      -0.08440072209924565,
      1.4041165022128301
    ],
    "groups": 80,
    "resamples": 5000
  }
}
```
