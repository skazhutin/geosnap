SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

-include .env
export

UV ?= uv
PYTHON ?= .venv/bin/python
PROFILE ?= sample
CITY_ID ?= moscow
RETRIEVER ?= megaloc
TORCH_DEVICE ?= auto
EMBEDDING_BATCH_SIZE ?= 8
RETRIEVAL_TOP_K ?= 20
GEOSNAP_MODEL_CACHE ?= .cache/torch/hub
HF_HOME ?= .cache/huggingface

DATA_ROOT ?= data
RAW_DIR := $(DATA_ROOT)/raw/$(PROFILE)
PROCESSED_DIR := $(DATA_ROOT)/processed/$(PROFILE)
REPORT_DIR := $(PROCESSED_DIR)/reports
EMBEDDING_DIR := $(DATA_ROOT)/embeddings/$(PROFILE)/$(RETRIEVER)
INDEX_DIR := $(DATA_ROOT)/indexes/$(PROFILE)/$(RETRIEVER)
EVAL_ROOT := $(DATA_ROOT)/evaluation/generated/moscow_commons_proxy_v1
EVAL_MODEL ?= $(RETRIEVER)
EVAL_STEM := $(subst -,_,$(EVAL_MODEL))_with_robustness
EVAL_TOP_K ?= 10
EVAL_ESTIMATOR ?= weighted_medoid
EMBED_MANIFEST ?= $(FINAL_MANIFEST)
MOSCOW_QUERY_POINTS_CONFIG ?= configs/moscow_kartaview_areas_v1.json
KARTAVIEW_MIN_REQUEST_INTERVAL_SEC ?= 45
KARTAVIEW_SEQUENCE_MAX_REQUESTS ?= 48
KARTAVIEW_MAX_SELECTED_RECORDS ?= 1200
MOSCOW_MAPILLARY_LIMIT_PER_POINT ?= 25
MOSCOW_MAPILLARY_RADIUS_M ?= 25
DOWNLOAD_MAX_BYTES ?= 6291456
MOSCOW_EVAL_DIR ?= $(DATA_ROOT)/evaluation/moscow_real_v1
MOSCOW_GALLERY_MANIFEST := $(MOSCOW_EVAL_DIR)/gallery.parquet
MOSCOW_CALIBRATION_MANIFEST := $(MOSCOW_EVAL_DIR)/calibration_queries.parquet
MOSCOW_TEST_MANIFEST := $(MOSCOW_EVAL_DIR)/test_queries.parquet
MOSCOW_QUERY_MANIFEST ?= $(MOSCOW_CALIBRATION_MANIFEST)
MOSCOW_MAX_QUERIES ?= 1000
MOSCOW_EVAL_MODEL ?= $(EVAL_MODEL)
MOSCOW_CONFIDENCE_THRESHOLD ?= 0.55
MOSCOW_REPORT_STEM ?= $(subst -,_,$(MOSCOW_EVAL_MODEL))_moscow_real_calibration

MAPILLARY_JSON := $(RAW_DIR)/mapillary_raw.json
KARTAVIEW_JSON := $(RAW_DIR)/kartaview_raw.json
KARTAVIEW_SEQUENCE_JSON := $(RAW_DIR)/kartaview_sequences_raw.json
KARTAVIEW_SEQUENCE_PLAN_JSON := $(RAW_DIR)/kartaview_sequences.plan.json
KARTAVIEW_SEQUENCE_PLAN_STATS := $(RAW_DIR)/kartaview_sequences.plan.stats.json
KARTAVIEW_SEQUENCE_CHECKPOINT := $(RAW_DIR)/kartaview_sequences.checkpoint.json
KARTAVIEW_SEQUENCE_STATS := $(RAW_DIR)/kartaview_sequences.stats.json
KARTAVIEW_SELECTED_JSON := $(RAW_DIR)/kartaview_selected.json
KARTAVIEW_SELECTION_REPORT := $(RAW_DIR)/kartaview_selection.report.json
KARTAVIEW_MERGE_JSON = $(if $(filter moscow,$(PROFILE)),$(KARTAVIEW_SELECTED_JSON),$(KARTAVIEW_JSON))
RAW_MANIFEST := $(RAW_DIR)/manifest.parquet
FINAL_MANIFEST := $(PROCESSED_DIR)/manifest_clean.parquet

.PHONY: setup test ingest-sample ingest-moscow ingest-mapillary ingest-kartaview \
	plan-kartaview-sequences expand-kartaview-sequences select-kartaview-frames \
	merge download prepare-data split-moscow benchmark-moscow benchmark-moscow-models \
	benchmark-moscow-test embed embed-moscow-gallery build-index index-moscow-gallery \
	eval-data eval api frontend smoke compose-config

setup:
	$(UV) sync --extra dev
	npm --prefix apps/frontend ci

test:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m pytest -q
	npm --prefix apps/frontend test -- --run
	npm --prefix apps/frontend run build

ingest-sample:
	$(MAKE) ingest-kartaview PROFILE=sample KARTAVIEW_EXTRA_ARGS="--center-lat 55.7558 --center-lon 37.6173 --radius-override-m 20 --max-tiles 1 --limit-per-tile 5 --max-pages-per-tile 1"
	$(MAKE) merge PROFILE=sample
	$(MAKE) download PROFILE=sample

ingest-moscow:
	@test "$(CONFIRM_LARGE_RUN)" = "1" || { echo "Set CONFIRM_LARGE_RUN=1 after checking disk/RAM; the run is resumable but potentially large." >&2; exit 2; }
	$(MAKE) ingest-kartaview PROFILE=moscow \
		KARTAVIEW_EXTRA_ARGS="--query-points-config $(MOSCOW_QUERY_POINTS_CONFIG) --limit-per-tile 150 --max-pages-per-tile 1"
	$(MAKE) plan-kartaview-sequences PROFILE=moscow
	$(MAKE) expand-kartaview-sequences PROFILE=moscow
	$(MAKE) select-kartaview-frames PROFILE=moscow
	@mapillary_status=skipped; \
	if [ -n "$${MAPILLARY_ACCESS_TOKEN:-}" ]; then \
		set +e; \
		$(MAKE) ingest-mapillary PROFILE=moscow \
			MAPILLARY_EXTRA_ARGS="--query-points-config $(MOSCOW_QUERY_POINTS_CONFIG) --query-radius-override-m $(MOSCOW_MAPILLARY_RADIUS_M) --limit-per-tile $(MOSCOW_MAPILLARY_LIMIT_PER_POINT) --max-pages-per-tile 1"; \
		mapillary_status=$$?; \
		set -e; \
	else \
		echo "MAPILLARY_ACCESS_TOKEN is unset; continuing with KartaView only." >&2; \
	fi; \
	if [ "$$mapillary_status" != skipped ] && [ "$$mapillary_status" -ne 0 ]; then echo "Mapillary ingestion failed; merge will still use any valid source output." >&2; fi; \
	echo "source_status kartaview=ready mapillary=$$mapillary_status"
	$(MAKE) merge PROFILE=moscow
	$(PYTHON) -c 'import sys; import pyarrow.parquet as pq; rows = pq.ParquetFile(sys.argv[1]).metadata.num_rows; print(f"merged_manifest_rows={rows}"); raise SystemExit(0 if rows > 0 else "No valid source rows were produced; refusing to download an empty Moscow dataset.")' "$(DATA_ROOT)/raw/moscow/manifest.parquet"
	$(MAKE) download PROFILE=moscow

ingest-mapillary:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.mapillary_loader \
		--output-json "$(MAPILLARY_JSON)" \
		--checkpoint "$(RAW_DIR)/mapillary.checkpoint.json" \
		--stats "$(RAW_DIR)/mapillary.stats.json" \
		--request-retries 5 --backoff-sec 1.5 --timeout-sec 30 \
		--checkpoint-every-tiles 1 $(MAPILLARY_EXTRA_ARGS)

ingest-kartaview:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.kartaview_loader \
		--output-json "$(KARTAVIEW_JSON)" \
		--checkpoint "$(RAW_DIR)/kartaview.checkpoint.json" \
		--stats "$(RAW_DIR)/kartaview.stats.json" \
		--request-retries 5 --backoff-sec 1.5 --timeout-sec 30 \
		--checkpoint-every-tiles 1 \
		--minimum-request-interval-sec "$(KARTAVIEW_MIN_REQUEST_INTERVAL_SEC)" $(KARTAVIEW_EXTRA_ARGS)

plan-kartaview-sequences:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.kartaview_sequences \
		--discovery-json "$(KARTAVIEW_JSON)" \
		--output-json "$(KARTAVIEW_SEQUENCE_JSON)" \
		--plan-json "$(KARTAVIEW_SEQUENCE_PLAN_JSON)" \
		--stats "$(KARTAVIEW_SEQUENCE_PLAN_STATS)" \
		--max-requests "$(KARTAVIEW_SEQUENCE_MAX_REQUESTS)" \
		--min-request-interval-sec "$(KARTAVIEW_MIN_REQUEST_INTERVAL_SEC)" \
		--plan-only $(KARTAVIEW_SEQUENCE_EXTRA_ARGS)

expand-kartaview-sequences:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.kartaview_sequences \
		--discovery-json "$(KARTAVIEW_JSON)" \
		--output-json "$(KARTAVIEW_SEQUENCE_JSON)" \
		--plan-json "$(KARTAVIEW_SEQUENCE_PLAN_JSON)" \
		--checkpoint "$(KARTAVIEW_SEQUENCE_CHECKPOINT)" \
		--stats "$(KARTAVIEW_SEQUENCE_STATS)" \
		--max-requests "$(KARTAVIEW_SEQUENCE_MAX_REQUESTS)" \
		--min-request-interval-sec "$(KARTAVIEW_MIN_REQUEST_INTERVAL_SEC)" \
		--request-retries 5 --backoff-sec 1.5 --timeout-sec 30 $(KARTAVIEW_SEQUENCE_EXTRA_ARGS)

select-kartaview-frames:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.select_kartaview_frames \
		--input-json "$(KARTAVIEW_SEQUENCE_JSON)" \
		--output-json "$(KARTAVIEW_SELECTED_JSON)" \
		--report "$(KARTAVIEW_SELECTION_REPORT)" \
		--max-records "$(KARTAVIEW_MAX_SELECTED_RECORDS)" $(KARTAVIEW_SELECTION_EXTRA_ARGS)

merge:
	$(PYTHON) -m ml.ingestion.merge_sources \
		--mapillary-json "$(MAPILLARY_JSON)" \
		--kartaview-json "$(KARTAVIEW_MERGE_JSON)" \
		--output-manifest "$(RAW_MANIFEST)" \
		--image-root "$(RAW_DIR)/images" --city-id "$(CITY_ID)"

download:
	$(PYTHON) -m ml.ingestion.download_images \
		--manifest "$(RAW_MANIFEST)" \
		--errors-log "$(RAW_DIR)/download_errors.json" \
		--stats "$(RAW_DIR)/download.stats.json" --workers 4 --retries 3 \
		--max-download-bytes "$(DOWNLOAD_MAX_BYTES)" $(DOWNLOAD_EXTRA_ARGS)

prepare-data:
	mkdir -p "$(PROCESSED_DIR)" "$(REPORT_DIR)"
	$(PYTHON) -m ml.cleaning.clean_images --manifest "$(RAW_MANIFEST)" \
		--output "$(PROCESSED_DIR)/manifest_step1.parquet" \
		--report "$(REPORT_DIR)/cleaning.json" \
		--pipeline-report "$(PROCESSED_DIR)/cleaning_report.json"
	$(PYTHON) -m ml.cleaning.quality_filter \
		--manifest "$(PROCESSED_DIR)/manifest_step1.parquet" \
		--output "$(PROCESSED_DIR)/manifest_step2.parquet" \
		--report "$(REPORT_DIR)/quality.json" \
		--pipeline-report "$(PROCESSED_DIR)/cleaning_report.json"
	$(PYTHON) -m ml.cleaning.deduplicate \
		--manifest "$(PROCESSED_DIR)/manifest_step2.parquet" \
		--output "$(PROCESSED_DIR)/manifest_step3.parquet" \
		--report "$(REPORT_DIR)/dedup.json" \
		--pipeline-report "$(PROCESSED_DIR)/cleaning_report.json"
	$(PYTHON) -m ml.enrichment.h3_assign \
		--manifest "$(PROCESSED_DIR)/manifest_step3.parquet" \
		--output "$(PROCESSED_DIR)/manifest_step4.parquet" \
		--report "$(REPORT_DIR)/h3.json"
	$(PYTHON) -m ml.cleaning.build_final_manifest \
		--manifest "$(PROCESSED_DIR)/manifest_step4.parquet" \
		--output "$(FINAL_MANIFEST)" --report "$(REPORT_DIR)/final.json"
	$(PYTHON) -m ml.cleaning.check_dataset --manifest "$(FINAL_MANIFEST)" \
		--report "$(REPORT_DIR)/dataset.json" --markdown "$(REPORT_DIR)/dataset.md" \
		--scatter "$(REPORT_DIR)/scatter.png" --density "$(REPORT_DIR)/density.png" \
		--source-comparison "$(REPORT_DIR)/sources.png" \
		--preview "$(REPORT_DIR)/preview.jpg" \
		--ingestion-stats "$(RAW_DIR)/kartaview.stats.json" \
		--ingestion-stats "$(RAW_DIR)/mapillary.stats.json" \
		--ingestion-stats "$(RAW_DIR)/download.stats.json" \
		--ingestion-stats "$(PROCESSED_DIR)/cleaning_report.json"

split-moscow:
	$(PYTHON) -m ml.evaluation.moscow_split \
		--manifest "$(FINAL_MANIFEST)" --output-dir "$(MOSCOW_EVAL_DIR)" \
		--max-queries "$(MOSCOW_MAX_QUERIES)" --min-query-spacing-m 20 \
		--positive-distance-m 100 --phash-distance-threshold 4

benchmark-moscow:
	HF_HOME="$(HF_HOME)" $(PYTHON) -m ml.evaluation.moscow_benchmark \
		--gallery-manifest "$(MOSCOW_GALLERY_MANIFEST)" \
		--query-manifest "$(MOSCOW_QUERY_MANIFEST)" \
		--output-dir "$(MOSCOW_EVAL_DIR)/reports" --model "$(MOSCOW_EVAL_MODEL)" \
		--device "$(TORCH_DEVICE)" --cache-dir "$(GEOSNAP_MODEL_CACHE)" \
		--batch-size "$(EMBEDDING_BATCH_SIZE)" --top-k "$(EVAL_TOP_K)" \
		--estimator "$(EVAL_ESTIMATOR)" --confidence-threshold "$(MOSCOW_CONFIDENCE_THRESHOLD)" \
		--report-stem "$(MOSCOW_REPORT_STEM)" $(MOSCOW_BENCHMARK_EXTRA_ARGS)

benchmark-moscow-models:
	$(MAKE) benchmark-moscow MOSCOW_EVAL_MODEL=megaloc \
		MOSCOW_REPORT_STEM=megaloc_moscow_real_calibration
	$(MAKE) benchmark-moscow MOSCOW_EVAL_MODEL=dinov2-salad \
		MOSCOW_REPORT_STEM=dinov2_salad_moscow_real_calibration

benchmark-moscow-test:
	@test "$(CONFIRM_FINAL_TEST)" = "1" || { echo "Set CONFIRM_FINAL_TEST=1 only after model, estimator, and confidence are frozen on calibration." >&2; exit 2; }
	$(MAKE) benchmark-moscow MOSCOW_QUERY_MANIFEST="$(MOSCOW_TEST_MANIFEST)" \
		MOSCOW_REPORT_STEM="$(subst -,_,$(MOSCOW_EVAL_MODEL))_moscow_real_test"

embed:
	$(PYTHON) -m ml.retrieval.embedding_job --manifest "$(EMBED_MANIFEST)" \
		--output-dir "$(EMBEDDING_DIR)" --retriever "$(RETRIEVER)" \
		--device "$(TORCH_DEVICE)" --batch-size "$(EMBEDDING_BATCH_SIZE)" \
		--model-cache "$(GEOSNAP_MODEL_CACHE)" $(EMBED_EXTRA_ARGS)

embed-moscow-gallery:
	$(MAKE) embed PROFILE=moscow EMBED_MANIFEST="$(MOSCOW_GALLERY_MANIFEST)"

build-index:
	$(PYTHON) -m ml.indexing.build_index --embeddings "$(EMBEDDING_DIR)" \
		--output-dir "$(INDEX_DIR)" --index-id "$(PROFILE)" --city-id "$(CITY_ID)"

index-moscow-gallery: embed-moscow-gallery
	$(MAKE) build-index PROFILE=moscow

eval-data:
	$(PYTHON) -m ml.evaluation.commons \
		--config "$(DATA_ROOT)/evaluation/moscow_commons_landmarks.json" \
		--output-dir "$(EVAL_ROOT)" --reuse-existing

eval: eval-data
	HF_HOME="$(HF_HOME)" $(PYTHON) -m ml.evaluation.commons_benchmark \
		--manifest "$(EVAL_ROOT)/manifest.json" \
		--output-dir "$(EVAL_ROOT)/reports" --model "$(EVAL_MODEL)" \
		--device "$(TORCH_DEVICE)" --batch-size "$(EMBEDDING_BATCH_SIZE)" \
		--cache-dir "$(GEOSNAP_MODEL_CACHE)" --top-k "$(EVAL_TOP_K)" \
		--estimator "$(EVAL_ESTIMATOR)" --report-stem "$(EVAL_STEM)" --robustness

api:
	GEOSNAP_INDEX_DIR="$(INDEX_DIR)" RETRIEVER="$(RETRIEVER)" \
		CITY_ID="$(CITY_ID)" INDEX_ID="$(PROFILE)" \
		$(PYTHON) -m uvicorn app.main:app --app-dir apps/backend --host 0.0.0.0 --port 8000

frontend:
	npm --prefix apps/frontend run dev -- --host 0.0.0.0

smoke:
	PYTHONPATH=.:apps/backend GEOSNAP_MODEL_CACHE="$(GEOSNAP_MODEL_CACHE)" \
		$(PYTHON) infra/scripts/smoke_localize.py --index-dir "$(INDEX_DIR)"

compose-config:
	docker compose --env-file /dev/null config --quiet
