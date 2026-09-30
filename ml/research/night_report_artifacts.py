"""Render the verified night evidence; never launch or select another experiment."""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import paths


def table(records):
    lines = ["| Вариант | Эталоны | ≤25 м | ≤50 м | ≤100 м | Медиана, м | p90, м | >500 м | R@10 | R@100 |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for label, record in records:
        raw, recall = record["raw"], record["recall_at"]
        values = [label, str(record["gallery_count"]), *[f'{100 * raw[f"accuracy_{m}m"]:.2f}%' for m in (25, 50, 100)],
                  f'{raw["median_error_m"]:.0f}', f'{raw["p90_error_m"]:.0f}',
                  f'{100 * raw["catastrophic_gt500m_rate"]:.2f}%', f'{100 * recall["10"]:.2f}%', f'{100 * recall["100"]:.2f}%']
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def gallery_table(records):
    lines = ["| Галерея | Mapillary / KartaView / MSLS | Серии | H3 cells | Медиана кадров/cell | Направлений/cell | Покрытие 25 / 50 / 100 м | 100 м + heading≤45° |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for label, record in records:
        gallery, coverage = record["gallery"], record["coverage"]
        values = [label, " / ".join(str(gallery["by_source"].get(k, 0)) for k in ("mapillary", "kartaview", "msls")),
            str(gallery["unique_provider_sequences"]), str(gallery["occupied_h3_r9"]),
            f'{gallery["median_references_per_occupied_cell"]:.1f}', f'{gallery["mean_heading_bins_per_occupied_cell"]:.2f}',
            " / ".join(f'{100 * coverage[f"within_{m}m"]:.2f}%' for m in (25, 50, 100)),
            f'{100 * coverage["within_100m_and_heading_45deg"]:.2f}%']
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def rank_table(records, summaries):
    lines = ["| Вариант | R@1/5/10/20/50/100, % | Rank median/p75/p90, все запросы | Rank median/p75/p90, покрытые |",
             "|---|---:|---:|---:|"]
    for label, record in records:
        ranks = summaries.get(record.get("name"), record.get("retrieval"))
        if ranks is None:
            continue
        values = [label, " / ".join(f'{100 * ranks["recall_at"][str(k)]:.2f}' for k in (1, 5, 10, 20, 50, 100))]
        for population in ("all_query", "covered_only"):
            quantiles = ranks[f"positive_rank_quantiles_{population}"]
            values.append(" / ".join("∞" if quantiles[q] is None else f"{quantiles[q]:.1f}" for q in ("0.5", "0.75", "0.9")))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def run():
    _, gc, store, previous, night = paths()
    frozen_path = night / "candidate_frozen.json"
    frozen = json.loads(frozen_path.read_text())
    if (frozen["kind"] != "development_research_candidate_not_production_release"
            or frozen["query_sha256"] != gc["query_sha256"] or frozen["final_opened_this_night"]
            or frozen["calibration_opened_this_night"] or frozen["production_frozen_files_verified"] != 42):
        raise RuntimeError("verified development-only freeze required before final artifact rendering")
    stage = night / "live_expansion2_quarantine"
    evidence = json.loads((stage / "final_evidence.json").read_text())
    best = evidence["selected"]
    if best != frozen["candidate"]:
        raise RuntimeError("final evidence and verified frozen selection differ")
    production = json.loads((night / "production_baseline/report.json").read_text())
    k30 = json.loads((night / "production_baseline/k30_report.json").read_text())
    records = [(
        "Production SAGE-B, top-1", {"raw": k30["canonical_production_raw_top1"], "gallery_count": 20487,
                                     "recall_at": production["retrieval"]["recall_at"]})]
    historical = []
    for name in ("G0", "G1_random", "G1_smart", "G2_smart"):
        record = json.loads((previous / "results" / f"{name}.json").read_text())
        if record["query_sha256"] != gc["query_sha256"]:
            raise RuntimeError("gallery scaling table has a different development population")
        historical.append((name, dict(record, gallery_count=record["gallery"]["references"], recall_at=record["retrieval"]["recall_at"])))
    selected = {r["name"]: r for r in evidence["records"]}
    labels = {"G2": "99 976, общая фильтрация", "first": "102 915, первая добавка", "expanded2": "107 749, вторая добавка"}
    main = []
    for key in labels:
        for arm, title in (("top1", "top-1"), ("mean", "два масштаба"), ("context", "два масштаба + контекст")):
            main.append((f"{labels[key]}: {title}", selected[f"quarantine_{key}_{arm}"]))
    for arm, title in (("top1", "top-1"), ("mean", "два масштаба"), ("context", "два масштаба + контекст")):
        if f"tranche3_{arm}" in selected:
            record = selected[f"tranche3_{arm}"]
            main.append((f'{record["gallery_count"]:,}, третья добавка: {title}', record))
    for arm, label in (("top1", "Допустимые источники: top-1"), ("mean", "Допустимые источники: два масштаба"),
                       ("context", "Допустимые источники: два масштаба + контекст")):
        if f"licensed_{arm}" in selected:
            main.append((label, selected[f"licensed_{arm}"]))
    for arm, title in (("top1", "top-1"), ("mean", "два масштаба"), ("context", "два масштаба + контекст")):
        if f"licensed_third_{arm}" in selected:
            record = selected[f"licensed_third_{arm}"]
            main.append((f'Допустимые источники, {record["gallery_count"]:,}: {title}', record))
    destination = night / "report"
    destination.mkdir(exist_ok=True)
    chart_records = [records[0], ("G2: SAGE-L", selected["quarantine_G2_top1"]),
        ("G2 + scale/context", selected["quarantine_G2_context"]),
        ("First addition", selected["quarantine_first_context"]),
        ("Second addition", selected["quarantine_expanded2_context"])]
    if "tranche3_context" in selected:
        chart_records.append(("Third addition", selected["tranche3_context"]))
    if "licensed_context" in selected:
        chart_records.append(("Allowed sources only", selected["licensed_context"]))
    if "licensed_third_context" in selected:
        chart_records.append(("Allowed sources + third", selected["licensed_third_context"]))
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, axes = plt.subplots(1, 2, figsize=(15, 6.7), constrained_layout=True)
    names = [label for label, _ in chart_records]
    y = np.arange(len(names))
    axes[0].barh(y, [r["raw"]["accuracy_100m"] * 100 for _, r in chart_records], color="#237D99")
    axes[0].set(yticks=y, yticklabels=names, xlim=(0, 100), xlabel="All-query accuracy within 100 m (%)")
    axes[0].invert_yaxis()
    for i, (_, r) in enumerate(chart_records):
        axes[0].text(r["raw"]["accuracy_100m"] * 100 + 1, i, f'{r["raw"]["accuracy_100m"] * 100:.2f}%', va="center")
    axes[0].set_title("Same 1,184 development queries; no abstention")
    detail = [r for _, r in chart_records[1:]]
    left = np.zeros(len(detail))
    for key, label, color in [("correct_top1", "Correct", "#237D99"),
        ("wrong_top1_positive_in_top100", "Wrong top candidate", "#E4BB67"),
        ("retrieval_miss_top100", "Retrieval miss", "#C87957"), ("no_coverage", "No coverage", "#9DABB3")]:
        value = np.array([r["diagnosis"][key] for r in detail]) / 1184 * 100
        axes[1].barh(np.arange(len(detail)), value, left=left, color=color, label=label)
        left += value
    axes[1].set(yticks=np.arange(len(detail)), yticklabels=names[1:], xlim=(0, 100), xlabel="Share of all queries (%)")
    axes[1].invert_yaxis()
    axes[1].set_title("Error decomposition after the common quarantine")
    axes[1].legend(loc="upper center", bbox_to_anchor=(.5, -.13), ncol=2, frameon=False)
    for axis in axes:
        axis.spines[["top", "right"]].set_visible(False)
        axis.grid(axis="x", alpha=.15)
        axis.set_axisbelow(True)
    fig.savefig(destination / "results.png", dpi=180)
    fig.savefig(destination / "results.pdf")
    plt.close(fig)
    paired = frozen["actual_production_baseline_comparison"]["selected_vs_production_geographic"]
    diagnosis = best["diagnosis"]
    additions = []
    for directory in (stage, night / "reference_tranche3"):
        if (directory / "audit.done.json").exists():
            audit = json.loads((directory / "audit.done.json").read_text())
            additions.append(f'{directory.name}: {audit["added"]} новых допустимых изображений, {audit["by_source"]}; '
                             f'галерея {audit["gallery_count"]} эталонов.')
    additions_text = "\n".join(additions)
    gallery_rows = historical + [(label, record) for label, record in main if record["name"].endswith("_top1")]
    smoke = json.loads((night / "fresh_runtime_smoke/report.json").read_text())
    text = f"""# GeoSnap: результаты ночи 9–10 сентября 2026

Выбран `{best['name']}`: **{best['raw']['accuracy_100m'] * 100:.2f}% ≤100 м** на всех 1 184 development-запросах.
Прирост относительно исходного production с его географическим выбором: **{paired['gain_pp']:.2f} п.п.**,
парный пространственный 95% интервал [{paired['ci95_pp'][0]:.2f}; {paired['ci95_pp'][1]:.2f}] п.п.
Исходный production: top-1 {k30['canonical_production_raw_top1']['accuracy_100m'] * 100:.2f}%,
географический выбор {k30['canonical_production_raw_geo']['accuracy_100m'] * 100:.2f}%. Это raw до фильтра уверенности.

Существующий контроль на одинаковых 20 487 production-эталонах и тех же1 184запросах
отдельно проверен без повторного расчёта: SAGE-B→SAGE-L даёт13.85%→16.05%,
+2.20п.п., пространственный95%CI[0.77;3.87]. Более крупная модель сама по себе
объясняет лишь ограниченный выигрыш на старой галерее. Последующие изменения данных
и pipeline взаимодействуют; их эффекты нельзя считать независимыми слагаемыми.
Hashes, совместимость checkpoint/preprocessing и точные метрики: `{night / 'model_gallery_attribution.json'}`.

## Первоначальная проверка размера галереи

{table(historical)}

G0→60k smart: +4.56 п.п., пространственный 95% CI [2.92; 6.21]. Smart против random:
+0.68 п.п., CI [−0.27; 1.68] — убедительного превосходства не установлено.
60k→100k: +0.59 п.п., CI [−0.08; 1.40]; безусловное расширение MSLS до150k/200k
не прошло критерий. Последующие небольшие добавки Mapillary/KartaView оценивались отдельно.

Исторические результаты сохранены. Новое сравнение ниже одинаково исключает 29 эталонов
из вновь подозрительной серии: 24 из G2 и ещё 5 из первой добавки. Состав запросов не менялся.

## Сопоставимые ночные результаты

{table(records + main)}

## Состав и географическое покрытие

{gallery_table(gallery_rows)}

Heading coverage считается на всех запросах; отсутствие heading не исключает запрос из denominator.
Близость координат сама по себе не гарантирует совпадение направления, сезона или видимых объектов.

## Recall и полное распределение positive rank

{rank_table(historical + main, evidence['full_positive_rank_summaries'])}

∞ означает отсутствие географического positive. Полные ranks context-вариантов восстановлены
из точных mean-score ranks после проверки, что context переставляет только прежние первые30.
Гистограммы всех ranks сохранены в final_evidence.json.

У выбранного варианта: нет эталона ≤100 м — {diagnosis['no_coverage']}; эталон существует,
но отсутствует в top-100 — {diagnosis['retrieval_miss_top100']}; правильное место в top-100,
но выбран другой эталон — {diagnosis['wrong_top1_positive_in_top100']}; правильных — {diagnosis['correct_top1']}.

## Данные и воспроизводимость

Canonical physical store: `{store}`.
При первоначальной инвентаризации: 9 ZIP, 38 356 JPG, 276 CSV, MD5/TXT manifests.
В доступных архивах обнаружены 249 190 московских кадров MSLS и225 panorama flags
до image/leakage-аудита. Metadata содержат stable IDs, координаты, серии, headings,
timestamps и source/license; сохранены в исходных manifests. Exact AOI — R102269.
Начальный новый audited pool этапаv6: 1 747 Mapillary,22KartaView,105 560MSLS,
плюс G0; после новых identity evidence сравниваемые галереи прошли общий quarantine.
{additions_text}
Выбранная галерея: `{best['gallery_manifest']}`; {best['gallery_count']} эталонов.
Manifests, descriptors и pool находятся рядом с gallery manifest выбранного этапа;
source-compatible assessments ссылаются на descriptor pool соответствующей общей галереи.
Большие данные и результаты находятся на внешнем томе.
На предыдущем этапе удалено 53.19 GB подтверждённых внутренних копий; перечень и проверки:
`{WORKSPACE / 'docs/gallery_scale_v6_results.md'}`. Повторного удаления этой ночью не было.

Все итоговые raw/ranks/diagnosis проверены по сохранённым предсказаниям; все 42 production-файла
сохранили исходные hashes. Подробные парные интервалы и поправка на число ночных сравнений:
`{stage / 'final_evidence.json'}`. Указанные интервалы приблизительны и не заменяют независимую проверку.
Fresh MPS проверка двух заранее выбранных запросов на галерее107749: verified={smoke['verified']},
top100/порядок и context совпали с кэшем. PeakRSS {smoke['peak_process_rss_bytes'] / 1e9:.2f}GB;
cold model load {smoke['fresh_query']['cold_model_load_seconds']:.2f}s. Это smoke,
а не проверка production latency, всей платформенной совместимости или новой третьей галереи.

## Закрытые гипотезы и несколько фотографий

FoL, прототипы мест, центрирование, коррекция плотности и одиночный масштаб 504 не дали
убедительного выигрыша. Квадратное кадрирование: 27.45%; его добавление к двум масштабам:
30.49% против 31.00%, ветка закрыта по заранее указанному условию.
Четыре направления одной панорамы в основной галерее не изменили raw100.

В отдельной вспомогательной проверке 43 панорам/172 зависимых направлений:
один кадр — 9.88%, объединение четырёх отдельных предсказаний по общему месту — 23.26%,
коллаж 2×2 — 12.21%. Это результат синтетических направлений, а не подтверждённая точность
на реальных сериях пользовательских фотографий. Повторного поиска вариантов коллажа не было.

## Ограничения

Использовалась повторно одна development-выборка; current calibration и final test остались закрыты.
Старые исторические выборки независимыми не объявляются: сохранены сведения о прежних пересечениях,
а также о случайно увиденных исторических summary/config values, не использованных для выбора.
Подробности — `{night / 'legacy_document_scope_disclosure.json'}` и incident receipts.
MSLS допускается только для исследования. Вариант на разрешённых источниках показан отдельно;
это не разрешение на публикацию или утверждение о готовности всего runtime к production.
Порог уверенности и answer rate не оптимизировались. Production не переключён.
Независимый final-test результат автоматически не получался.

Следующий шаг: независимо проверить зафиксированный кандидат на новых реальных
московских фотографиях, заранее отделённых по месту/серии/времени, после отдельной
команды на открытие теста. Текущий выигрыш development не доказывает такую точность
на новых пользовательских снимках; крупные ошибки всё ещё часты.
"""
    (destination / "results.md").write_text(text)
    save(destination / "receipt.json", {"candidate_frozen_sha256": digest(frozen_path),
        "final_evidence_sha256": digest(stage / "final_evidence.json"),
        "source_sha256": digest(Path(__file__)),
        "files": {name: digest(destination / name) for name in ("results.md", "results.png", "results.pdf")}})
    print(str(destination / "results.md"))


if __name__ == "__main__":
    run()
