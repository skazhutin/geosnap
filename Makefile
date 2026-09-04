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
GEOSNAP_RUNTIME_CONFIG ?=

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
KARTAVIEW_SEQUENCE_MAX_REQUESTS ?=
KARTAVIEW_HOTSPOTS_PER_AREA ?=
KARTAVIEW_SEQUENCES_PER_HOTSPOT ?=
KARTAVIEW_SEQUENCE_LIMIT_ARGS = $(if $(strip $(KARTAVIEW_SEQUENCE_MAX_REQUESTS)),--max-requests "$(KARTAVIEW_SEQUENCE_MAX_REQUESTS)") $(if $(strip $(KARTAVIEW_HOTSPOTS_PER_AREA)),--hotspots-per-area "$(KARTAVIEW_HOTSPOTS_PER_AREA)") $(if $(strip $(KARTAVIEW_SEQUENCES_PER_HOTSPOT)),--sequences-per-hotspot "$(KARTAVIEW_SEQUENCES_PER_HOTSPOT)")
KARTAVIEW_MAX_SELECTED_RECORDS ?=
KARTAVIEW_MAX_PER_SEQUENCE ?=
KARTAVIEW_SELECTION_LIMIT_ARGS = $(if $(strip $(KARTAVIEW_MAX_SELECTED_RECORDS)),--max-records "$(KARTAVIEW_MAX_SELECTED_RECORDS)") $(if $(strip $(KARTAVIEW_MAX_PER_SEQUENCE)),--max-per-sequence "$(KARTAVIEW_MAX_PER_SEQUENCE)")
MOSCOW_MAPILLARY_LIMIT_PER_POINT ?= 25
MOSCOW_MAPILLARY_RADIUS_M ?= 25
MAPILLARY_CITYWIDE_ZOOM ?= 12
MAPILLARY_CITYWIDE_MAX_RECORDS ?= 20000
MAPILLARY_CITYWIDE_MAX_PER_TILE ?= 600
MAPILLARY_CITYWIDE_MAX_PER_SUBCELL ?= 60
MAPILLARY_CITYWIDE_SUBCELLS_PER_AXIS ?= 4
MAPILLARY_CITYWIDE_CANDIDATE_MULTIPLIER ?= 2
MAPILLARY_CITYWIDE_METADATA_BATCH_SIZE ?= 50
MAPILLARY_CITYWIDE_MAX_TILE_BYTES ?= 33554432
MAPILLARY_CITYWIDE_VECTOR_TILE_CACHE_MAX_AGE_SEC ?= 86400
MAPILLARY_CITYWIDE_METADATA_CACHE_MAX_AGE_SEC ?= 3600
DOWNLOAD_MAX_BYTES ?= 6291456
MOSCOW_EVAL_DIR ?= $(DATA_ROOT)/evaluation/moscow_real_v1
MOSCOW_RAW_DIR ?= $(DATA_ROOT)/raw/moscow
MOSCOW_AOI_GEOJSON ?= $(DATA_ROOT)/raw/moscow/moscow_admin_boundary.geojson
MOSCOW_AOI_STATS ?= $(DATA_ROOT)/raw/moscow/moscow_admin_boundary.stats.json
REFRESH_MOSCOW_BOUNDARY ?= 0
MOSCOW_SOURCE_MANIFEST ?= $(DATA_ROOT)/raw/moscow/live_manifest.parquet
MOSCOW_PHYSICAL_MANIFEST ?= $(DATA_ROOT)/processed/moscow/live_physical_manifest.parquet
MOSCOW_PHYSICAL_REPORT ?= $(DATA_ROOT)/processed/moscow/reports/live_physical_validation.json
MOSCOW_SCREEN_PIPELINE_REPORT ?= $(DATA_ROOT)/processed/moscow/reports/live_physical_pipeline.json
MOSCOW_CANONICAL_MANIFEST ?= $(DATA_ROOT)/processed/moscow/canonical_reference_gallery.parquet
MOSCOW_CANONICAL_REPORT_JSON ?= $(DATA_ROOT)/processed/moscow/reports/canonical_reference_gallery.json
MOSCOW_CANONICAL_REPORT_MD ?= $(DATA_ROOT)/processed/moscow/reports/canonical_reference_gallery.md
MOSCOW_CANONICAL_MAPS_DIR ?= $(DATA_ROOT)/processed/moscow/reports/coverage_maps
MOSCOW_FINAL_MANIFEST ?= $(DATA_ROOT)/processed/moscow/manifest_clean.parquet
MOSCOW_GALLERY_MANIFEST := $(MOSCOW_EVAL_DIR)/gallery.parquet
MOSCOW_CALIBRATION_MANIFEST := $(MOSCOW_EVAL_DIR)/calibration_queries.parquet
MOSCOW_TEST_MANIFEST := $(MOSCOW_EVAL_DIR)/test_queries.parquet
MOSCOW_QUERY_MANIFEST ?= $(MOSCOW_CALIBRATION_MANIFEST)
MOSCOW_MAX_QUERIES ?= 1000
MOSCOW_HOLDOUT_CANDIDATE_MULTIPLIER ?= 4
MOSCOW_EVAL_MODEL ?= $(EVAL_MODEL)
MOSCOW_CONFIDENCE_THRESHOLD ?= 0.5548002022369389
MOSCOW_REPORT_STEM ?= $(subst -,_,$(MOSCOW_EVAL_MODEL))_moscow_real_calibration
MOSCOW_CALIBRATION_BENCHMARK_JSON ?= $(MOSCOW_EVAL_DIR)/reports/$(MOSCOW_REPORT_STEM).json
MOSCOW_CONFIDENCE_REPORT_STEM ?= $(subst -,_,$(MOSCOW_EVAL_MODEL))_moscow_confidence_calibration
MOSCOW_VERIFICATION_MAX_QUERIES ?= 100
MOSCOW_VERIFICATION_BACKEND ?= opencv_sift
MOSCOW_VERIFY_TOP_K ?= 10
MOSCOW_VERIFICATION_GEOMETRIC_WEIGHT ?= 0.35
MOSCOW_VERIFICATION_MIN_SEQUENCES ?= 10
MOSCOW_VERIFICATION_MIN_AREAS ?= 4
MOSCOW_VERIFICATION_REPORT_STEM ?= $(subst -,_,$(MOSCOW_EVAL_MODEL))_$(MOSCOW_VERIFICATION_BACKEND)_k$(MOSCOW_VERIFY_TOP_K)_w$(subst .,_,$(MOSCOW_VERIFICATION_GEOMETRIC_WEIGHT))_moscow_verification_ablation
MOSCOW_V2_DIR ?= $(DATA_ROOT)/evaluation/moscow_real_v2
MOSCOW_V2_GENERATED_DIR ?= $(DATA_ROOT)/evaluation/generated/moscow_real_v2
MOSCOW_V2_ACQUISITION_DIR ?= $(MOSCOW_V2_GENERATED_DIR)/acquisition
MOSCOW_V2_SOURCE_MANIFEST ?= $(MOSCOW_V2_GENERATED_DIR)/source_manifest_clean.parquet
MOSCOW_V2_GALLERY ?= $(MOSCOW_V2_DIR)/gallery.parquet
MOSCOW_V2_CALIBRATION ?= $(MOSCOW_V2_DIR)/calibration_queries.parquet
MOSCOW_V2_TEST ?= $(MOSCOW_V2_DIR)/test_queries.parquet
MOSCOW_V2_MODEL ?= megaloc
MOSCOW_V2_TOP_K ?= 50
MOSCOW_V2_QUERY_AGGREGATION ?= single
MOSCOW_V2_EMBEDDINGS ?= $(DATA_ROOT)/embeddings/moscow_real_v2/$(MOSCOW_V2_MODEL)
MOSCOW_V2_REPORT_STEM ?= baseline_megaloc_weighted_medoid_calibration
MOSCOW_V2_REUSE_FROM ?=
MOSCOW_V2_REUSE_ARGS = $(if $(strip $(MOSCOW_V2_REUSE_FROM)),--reuse-from "$(MOSCOW_V2_REUSE_FROM)")

MAPILLARY_JSON := $(RAW_DIR)/mapillary_raw.json
MAPILLARY_CITYWIDE_JSON := $(RAW_DIR)/mapillary_citywide_raw.json
MAPILLARY_CITYWIDE_CACHE := $(RAW_DIR)/mapillary_citywide_cache
MAPILLARY_CITYWIDE_CHECKPOINT := $(RAW_DIR)/mapillary_citywide.checkpoint.json
MAPILLARY_CITYWIDE_STATS := $(RAW_DIR)/mapillary.stats.json
MAPILLARY_MERGE_JSON = $(if $(filter moscow,$(PROFILE)),$(MAPILLARY_CITYWIDE_JSON),$(MAPILLARY_JSON))
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

.PHONY: setup test ingest-sample ingest-moscow fetch-moscow-boundary screen-moscow-images combine-moscow-gallery prepare-moscow-gallery \
	ingest-mapillary ingest-mapillary-citywide ingest-kartaview \
	plan-kartaview-sequences expand-kartaview-sequences select-kartaview-frames \
	merge download prepare-data split-moscow benchmark-moscow benchmark-moscow-models \
	calibrate-moscow-confidence benchmark-moscow-verification benchmark-moscow-test \
	embed embed-moscow-gallery build-index index-moscow-gallery \
	eval-data eval api frontend smoke smoke-moscow-v2 compose-config \
	verify-moscow-production provision-production production-up production-down production-config verify-production \
	plan-moscow-v2-acquisition expand-mapillary-v2 select-kartaview-v2 split-moscow-v2 \
	coverage-moscow-v2 embed-moscow-v2 benchmark-moscow-v2-calibration calibrate-moscow-v2-confidence

setup:
	$(UV) sync --extra dev
	npm --prefix apps/frontend ci

test:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m pytest -q
	npm --prefix apps/frontend test -- --run
	npm --prefix apps/frontend run build

provision-production:
	docker compose --env-file .env -f docker-compose.prod.yml --profile tools run --build --rm artifact-provisioner

production-up:
	docker compose --env-file .env -f docker-compose.prod.yml up -d --build backend proxy telegram-bot

production-down:
	docker compose --env-file .env -f docker-compose.prod.yml down

production-config:
	MAP_TILE_URL="$${MAP_TILE_URL:-https://tiles.example.invalid/{z}/{x}/{y}.png}" \
	MAP_TILE_ORIGIN="$${MAP_TILE_ORIGIN:-https://tiles.example.invalid}" \
	MAP_ATTRIBUTION="$${MAP_ATTRIBUTION:-Configuration validation}" \
		docker compose --env-file /dev/null -f docker-compose.prod.yml config --quiet

verify-production:
	$(PYTHON) infra/scripts/verify_production.py

ingest-sample:
	$(MAKE) ingest-kartaview PROFILE=sample KARTAVIEW_EXTRA_ARGS="--center-lat 55.7558 --center-lon 37.6173 --radius-override-m 20 --max-tiles 1 --limit-per-tile 5 --max-pages-per-tile 1"
	$(MAKE) merge PROFILE=sample
	$(MAKE) download PROFILE=sample

ingest-moscow:
	@test "$(CONFIRM_LARGE_RUN)" = "1" || { echo "Set CONFIRM_LARGE_RUN=1 after checking disk/RAM; the run is resumable but potentially large." >&2; exit 2; }
	$(MAKE) fetch-moscow-boundary
	$(MAKE) ingest-kartaview PROFILE=moscow \
		KARTAVIEW_EXTRA_ARGS="--query-points-config $(MOSCOW_QUERY_POINTS_CONFIG) --limit-per-tile 150"
	$(MAKE) plan-kartaview-sequences PROFILE=moscow
	$(MAKE) expand-kartaview-sequences PROFILE=moscow
	$(MAKE) select-kartaview-frames PROFILE=moscow
	@mapillary_json="$(MOSCOW_RAW_DIR)/.mapillary-unavailable.$$$$.json"; \
	if [ -n "$${MAPILLARY_ACCESS_TOKEN:-}" ]; then \
		$(MAKE) ingest-mapillary-citywide PROFILE=moscow; \
		mapillary_json="$(MOSCOW_RAW_DIR)/mapillary_citywide_raw.json"; \
		echo "source_status kartaview=ready mapillary=ready"; \
	else \
		echo "MAPILLARY_ACCESS_TOKEN is unset; continuing with KartaView only." >&2; \
		echo "source_status kartaview=ready mapillary=skipped"; \
	fi; \
	$(MAKE) merge PROFILE=moscow RAW_MANIFEST="$(MOSCOW_SOURCE_MANIFEST)" MAPILLARY_MERGE_JSON="$$mapillary_json"
	$(PYTHON) -c 'import sys; import pyarrow.parquet as pq; rows = pq.ParquetFile(sys.argv[1]).metadata.num_rows; print(f"merged_manifest_rows={rows}"); raise SystemExit(0 if rows > 0 else "No valid source rows were produced; refusing to download an empty Moscow dataset.")' "$(MOSCOW_SOURCE_MANIFEST)"
	$(MAKE) download PROFILE=moscow RAW_MANIFEST="$(MOSCOW_SOURCE_MANIFEST)"

fetch-moscow-boundary:
	mkdir -p "$(DATA_ROOT)/raw/moscow"
	@if [ "$(REFRESH_MOSCOW_BOUNDARY)" != "1" ] && [ -f "$(MOSCOW_AOI_GEOJSON)" ] && [ -f "$(MOSCOW_AOI_STATS)" ]; then \
		echo "Using pinned Moscow administrative AOI: $(MOSCOW_AOI_GEOJSON)"; \
	else \
		$(PYTHON) -m ml.ingestion.fetch_osm_boundary \
			--output-geojson "$(MOSCOW_AOI_GEOJSON)" --stats "$(MOSCOW_AOI_STATS)"; \
	fi

ingest-mapillary:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.mapillary_loader \
		--output-json "$(MAPILLARY_JSON)" \
		--checkpoint "$(RAW_DIR)/mapillary.checkpoint.json" \
		--stats "$(RAW_DIR)/mapillary.stats.json" \
		--request-retries 5 --backoff-sec 1.5 --timeout-sec 30 \
		--checkpoint-every-tiles 1 $(MAPILLARY_EXTRA_ARGS)

ingest-mapillary-citywide:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.mapillary_citywide \
		--output-json "$(MAPILLARY_CITYWIDE_JSON)" \
		--cache-dir "$(MAPILLARY_CITYWIDE_CACHE)" \
		--checkpoint "$(MAPILLARY_CITYWIDE_CHECKPOINT)" \
		--stats "$(MAPILLARY_CITYWIDE_STATS)" \
		--aoi-geojson "$(MOSCOW_AOI_GEOJSON)" \
		--zoom "$(MAPILLARY_CITYWIDE_ZOOM)" \
		--max-records "$(MAPILLARY_CITYWIDE_MAX_RECORDS)" \
		--max-per-tile "$(MAPILLARY_CITYWIDE_MAX_PER_TILE)" \
		--max-per-subcell "$(MAPILLARY_CITYWIDE_MAX_PER_SUBCELL)" \
		--subcells-per-axis "$(MAPILLARY_CITYWIDE_SUBCELLS_PER_AXIS)" \
		--candidate-multiplier "$(MAPILLARY_CITYWIDE_CANDIDATE_MULTIPLIER)" \
		--metadata-batch-size "$(MAPILLARY_CITYWIDE_METADATA_BATCH_SIZE)" \
		--max-tile-bytes "$(MAPILLARY_CITYWIDE_MAX_TILE_BYTES)" \
		--vector-tile-cache-max-age-sec "$(MAPILLARY_CITYWIDE_VECTOR_TILE_CACHE_MAX_AGE_SEC)" \
		--metadata-cache-max-age-sec "$(MAPILLARY_CITYWIDE_METADATA_CACHE_MAX_AGE_SEC)" \
		--request-retries 5 --backoff-sec 1.5 --timeout-sec 30 \
		--request-interval-sec 0.10 $(MAPILLARY_CITYWIDE_EXTRA_ARGS)

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
		--stats "$(KARTAVIEW_SEQUENCE_PLAN_STATS)" $(KARTAVIEW_SEQUENCE_LIMIT_ARGS) \
		--min-request-interval-sec "$(KARTAVIEW_MIN_REQUEST_INTERVAL_SEC)" \
		--plan-only $(KARTAVIEW_SEQUENCE_EXTRA_ARGS)

expand-kartaview-sequences:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.kartaview_sequences \
		--discovery-json "$(KARTAVIEW_JSON)" \
		--output-json "$(KARTAVIEW_SEQUENCE_JSON)" \
		--plan-json "$(KARTAVIEW_SEQUENCE_PLAN_JSON)" \
		--checkpoint "$(KARTAVIEW_SEQUENCE_CHECKPOINT)" \
		--stats "$(KARTAVIEW_SEQUENCE_STATS)" $(KARTAVIEW_SEQUENCE_LIMIT_ARGS) \
		--min-request-interval-sec "$(KARTAVIEW_MIN_REQUEST_INTERVAL_SEC)" \
		--request-retries 5 --backoff-sec 1.5 --timeout-sec 30 $(KARTAVIEW_SEQUENCE_EXTRA_ARGS)

select-kartaview-frames:
	mkdir -p "$(RAW_DIR)"
	$(PYTHON) -m ml.ingestion.select_kartaview_frames \
		--input-json "$(KARTAVIEW_SEQUENCE_JSON)" \
		--output-json "$(KARTAVIEW_SELECTED_JSON)" \
		--report "$(KARTAVIEW_SELECTION_REPORT)" $(KARTAVIEW_SELECTION_LIMIT_ARGS) $(KARTAVIEW_SELECTION_EXTRA_ARGS)

merge:
	$(PYTHON) -m ml.ingestion.merge_sources \
		--mapillary-json "$(MAPILLARY_MERGE_JSON)" \
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

# Production Moscow publication uses only the deployable Mapillary/KartaView
# imagery that is physically present.  Screen source rows once before the
# exact-AOI publication gate so incomplete resumable downloads cannot block the
# usable gallery.
$(MOSCOW_PHYSICAL_MANIFEST): $(MOSCOW_SOURCE_MANIFEST)
	mkdir -p "$(DATA_ROOT)/processed/moscow/reports"
	rm -f "$(DATA_ROOT)/processed/moscow/cleaning_report.json" "$(DATA_ROOT)/processed/moscow/cleaning_report.md"
	$(PYTHON) -m ml.cleaning.clean_images --manifest "$(MOSCOW_SOURCE_MANIFEST)" \
		--output "$(MOSCOW_PHYSICAL_MANIFEST)" --report "$(MOSCOW_PHYSICAL_REPORT)" \
		--pipeline-report "$(MOSCOW_SCREEN_PIPELINE_REPORT)"

screen-moscow-images: $(MOSCOW_PHYSICAL_MANIFEST)

# The exact OSM administrative polygon is applied before quality scoring,
# deduplication, embeddings, split generation, or API index build.
$(MOSCOW_CANONICAL_MANIFEST): $(MOSCOW_PHYSICAL_MANIFEST) $(MOSCOW_AOI_GEOJSON)
	@test -f "$(MOSCOW_AOI_GEOJSON)" || { echo "Missing exact Moscow AOI: $(MOSCOW_AOI_GEOJSON); run make fetch-moscow-boundary" >&2; exit 2; }
	$(PYTHON) -m ml.ingestion.combine_reference_gallery \
		--input-manifest "$(MOSCOW_PHYSICAL_MANIFEST)" \
		--output-manifest "$(MOSCOW_CANONICAL_MANIFEST)" \
		--report-json "$(MOSCOW_CANONICAL_REPORT_JSON)" \
		--report-markdown "$(MOSCOW_CANONICAL_REPORT_MD)" \
		--maps-dir "$(MOSCOW_CANONICAL_MAPS_DIR)" \
		--require-images --image-base . \
		--aoi-geojson "$(MOSCOW_AOI_GEOJSON)" --filter-outside-aoi \
		--required-source mapillary --required-source kartaview

combine-moscow-gallery: $(MOSCOW_CANONICAL_MANIFEST)

prepare-moscow-gallery: combine-moscow-gallery
	mkdir -p "$(DATA_ROOT)/processed/moscow/reports"
	$(PYTHON) -m ml.cleaning.reporting --report "$(DATA_ROOT)/processed/moscow/cleaning_report.json" \
		--baseline-manifest "$(MOSCOW_CANONICAL_MANIFEST)"
	$(PYTHON) -m ml.cleaning.quality_filter --manifest "$(MOSCOW_CANONICAL_MANIFEST)" \
		--output "$(DATA_ROOT)/processed/moscow/manifest_step2.parquet" \
		--report "$(DATA_ROOT)/processed/moscow/reports/quality.json" \
		--pipeline-report "$(DATA_ROOT)/processed/moscow/cleaning_report.json"
	$(PYTHON) -m ml.cleaning.deduplicate --manifest "$(DATA_ROOT)/processed/moscow/manifest_step2.parquet" \
		--output "$(DATA_ROOT)/processed/moscow/manifest_step3.parquet" \
		--report "$(DATA_ROOT)/processed/moscow/reports/dedup.json" \
		--pipeline-report "$(DATA_ROOT)/processed/moscow/cleaning_report.json"
	$(PYTHON) -m ml.enrichment.h3_assign --manifest "$(DATA_ROOT)/processed/moscow/manifest_step3.parquet" \
		--output "$(DATA_ROOT)/processed/moscow/manifest_step4.parquet" \
		--report "$(DATA_ROOT)/processed/moscow/reports/h3.json"
	$(PYTHON) -m ml.cleaning.build_final_manifest --manifest "$(DATA_ROOT)/processed/moscow/manifest_step4.parquet" \
		--output "$(MOSCOW_FINAL_MANIFEST)" --report "$(DATA_ROOT)/processed/moscow/reports/final.json"
	$(PYTHON) -m ml.cleaning.check_dataset --manifest "$(MOSCOW_FINAL_MANIFEST)" \
		--report "$(DATA_ROOT)/processed/moscow/reports/dataset.json" --markdown "$(DATA_ROOT)/processed/moscow/reports/dataset.md" \
		--scatter "$(DATA_ROOT)/processed/moscow/reports/scatter.png" --density "$(DATA_ROOT)/processed/moscow/reports/density.png" \
		--source-comparison "$(DATA_ROOT)/processed/moscow/reports/sources.png" \
		--preview "$(DATA_ROOT)/processed/moscow/reports/preview.jpg" \
		--ingestion-stats "$(DATA_ROOT)/raw/moscow/kartaview.stats.json" \
		--ingestion-stats "$(DATA_ROOT)/raw/moscow/mapillary.stats.json" \
		--ingestion-stats "$(DATA_ROOT)/raw/moscow/live_download.stats.json" \
		--ingestion-stats "$(DATA_ROOT)/processed/moscow/cleaning_report.json"

split-moscow:
	$(PYTHON) -m ml.evaluation.moscow_split \
		--manifest "$(MOSCOW_FINAL_MANIFEST)" --output-dir "$(MOSCOW_EVAL_DIR)" \
		--max-queries "$(MOSCOW_MAX_QUERIES)" --min-query-spacing-m 20 \
		--positive-distance-m 100 --phash-distance-threshold 4 \
		--holdout-candidate-multiplier "$(MOSCOW_HOLDOUT_CANDIDATE_MULTIPLIER)"

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
		MOSCOW_REPORT_STEM=megaloc_moscow_real_calibration MOSCOW_CONFIDENCE_THRESHOLD=0.0
	$(MAKE) benchmark-moscow MOSCOW_EVAL_MODEL=dinov2-salad \
		MOSCOW_REPORT_STEM=dinov2_salad_moscow_real_calibration MOSCOW_CONFIDENCE_THRESHOLD=0.0

calibrate-moscow-confidence:
	$(PYTHON) -m ml.evaluation.calibrate_confidence \
		--benchmark-json "$(MOSCOW_CALIBRATION_BENCHMARK_JSON)" \
		--output-dir "$(MOSCOW_EVAL_DIR)/reports" \
		--report-stem "$(MOSCOW_CONFIDENCE_REPORT_STEM)"

benchmark-moscow-verification:
	HF_HOME="$(HF_HOME)" $(PYTHON) -m ml.evaluation.moscow_verification_ablation \
		--gallery-manifest "$(MOSCOW_GALLERY_MANIFEST)" \
		--query-manifest "$(MOSCOW_CALIBRATION_MANIFEST)" \
		--output-dir "$(MOSCOW_EVAL_DIR)/reports" --model "$(MOSCOW_EVAL_MODEL)" \
		--device "$(TORCH_DEVICE)" --cache-dir "$(GEOSNAP_MODEL_CACHE)" \
		--batch-size "$(EMBEDDING_BATCH_SIZE)" --top-k "$(EVAL_TOP_K)" \
		--max-queries "$(MOSCOW_VERIFICATION_MAX_QUERIES)" \
		--estimator "$(EVAL_ESTIMATOR)" --confidence-threshold "$(MOSCOW_CONFIDENCE_THRESHOLD)" \
		--verification-backend "$(MOSCOW_VERIFICATION_BACKEND)" \
		--verify-top-k "$(MOSCOW_VERIFY_TOP_K)" \
		--geometric-weight "$(MOSCOW_VERIFICATION_GEOMETRIC_WEIGHT)" \
		--minimum-sequences-for-enablement "$(MOSCOW_VERIFICATION_MIN_SEQUENCES)" \
		--minimum-areas-for-enablement "$(MOSCOW_VERIFICATION_MIN_AREAS)" \
		--report-stem "$(MOSCOW_VERIFICATION_REPORT_STEM)"

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
	$(if $(strip $(GEOSNAP_RUNTIME_CONFIG)),GEOSNAP_RUNTIME_CONFIG="$(GEOSNAP_RUNTIME_CONFIG)",GEOSNAP_INDEX_DIR="$(INDEX_DIR)" RETRIEVER="$(RETRIEVER)" CITY_ID="$(CITY_ID)" INDEX_ID="$(PROFILE)") \
		$(PYTHON) -m uvicorn app.main:app --app-dir apps/backend --host 0.0.0.0 --port 8000

frontend:
	npm --prefix apps/frontend run dev -- --host 0.0.0.0

smoke:
	PYTHONPATH=.:apps/backend GEOSNAP_MODEL_CACHE="$(GEOSNAP_MODEL_CACHE)" \
		$(PYTHON) infra/scripts/smoke_localize.py --index-dir "$(INDEX_DIR)" $(SMOKE_EXTRA_ARGS)

smoke-moscow-v2:
	GEOSNAP_RUNTIME_CONFIG="configs/moscow_real_v2_frozen.json" RETRIEVAL_TOP_K=50 \
		$(MAKE) smoke INDEX_DIR="data/indexes/moscow_real_v2/megaloc"

verify-moscow-production:
	PYTHONPATH=.:apps/backend GEOSNAP_MODEL_CACHE="$(GEOSNAP_MODEL_CACHE)" \
		$(PYTHON) -m ml.production_verify --config configs/moscow_production_frozen.json

compose-config:
	docker compose --env-file /dev/null config --quiet

plan-moscow-v2-acquisition:
	$(PYTHON) -m ml.ingestion.moscow_acquisition_plan \
		--gallery-manifest "$(DATA_ROOT)/evaluation/moscow_real_v1/gallery.parquet" \
		--mapillary-json "$(DATA_ROOT)/raw/moscow/mapillary_citywide_raw.json" \
		--mapillary-json "$(DATA_ROOT)/raw/moscow/mapillary_admin_outer_raw_v2.json" \
		--kartaview-json "$(DATA_ROOT)/raw/moscow/kartaview_sequences_raw.json" \
		--aoi-geojson "$(MOSCOW_AOI_GEOJSON)" \
		--historical-calibration-manifest "$(DATA_ROOT)/evaluation/moscow_real_v1/calibration_queries.parquet" \
		--historical-per-query-jsonl "$(DATA_ROOT)/evaluation/generated/product_quality_diagnosis/per_query.jsonl" \
		--output-json "$(MOSCOW_V2_ACQUISITION_DIR)/acquisition_plan.json"

expand-mapillary-v2:
	$(PYTHON) -m ml.ingestion.mapillary_reference_expansion \
		--discovery-json "$(DATA_ROOT)/raw/moscow/mapillary_citywide_raw.json" \
		--discovery-json "$(DATA_ROOT)/raw/moscow/mapillary_admin_outer_raw_v2.json" \
		--output-json "$(MOSCOW_V2_ACQUISITION_DIR)/mapillary_expanded.json" \
		--checkpoint "$(MOSCOW_V2_ACQUISITION_DIR)/mapillary_expansion.checkpoint.json" \
		--cache-dir "$(MOSCOW_V2_ACQUISITION_DIR)/mapillary_cache" \
		--stats "$(MOSCOW_V2_ACQUISITION_DIR)/mapillary_expansion.stats.json" \
		--aoi-geojson "$(MOSCOW_AOI_GEOJSON)" \
		--acquisition-plan "$(MOSCOW_V2_ACQUISITION_DIR)/acquisition_plan.json"

select-kartaview-v2:
	$(PYTHON) -m ml.ingestion.select_kartaview_frames \
		--input-json "$(DATA_ROOT)/raw/moscow/kartaview_sequences_raw.json" \
		--output-json "$(MOSCOW_V2_ACQUISITION_DIR)/kartaview_targeted.json" \
		--report "$(MOSCOW_V2_ACQUISITION_DIR)/kartaview_targeted.report.json" \
		--target-plan "$(MOSCOW_V2_ACQUISITION_DIR)/acquisition_plan.json" \
		--max-records 400 --max-per-sequence 5

split-moscow-v2:
	$(PYTHON) -m ml.evaluation.moscow_split \
		--manifest "$(MOSCOW_V2_SOURCE_MANIFEST)" --output-dir "$(MOSCOW_V2_DIR)" \
		--seed 20260901 --max-queries 1000 --min-query-spacing-m 20 \
		--positive-distance-m 100 --phash-distance-threshold 4 \
		--holdout-candidate-multiplier 4 --representative-balance

coverage-moscow-v2:
	$(PYTHON) -m ml.evaluation.v2_coverage \
		--gallery "$(MOSCOW_V2_GALLERY)" --calibration "$(MOSCOW_V2_CALIBRATION)" \
		--test "$(MOSCOW_V2_TEST)" --source-manifest "$(MOSCOW_V2_SOURCE_MANIFEST)" \
		--output-json "$(MOSCOW_V2_DIR)/coverage/coverage_grid.json" \
		--output-png "$(MOSCOW_V2_DIR)/coverage/coverage_diversity_grid.png"

embed-moscow-v2:
	$(PYTHON) -m ml.retrieval.embedding_job --manifest "$(MOSCOW_V2_GALLERY)" \
		--output-dir "$(MOSCOW_V2_EMBEDDINGS)" --retriever "$(MOSCOW_V2_MODEL)" \
		--device "$(TORCH_DEVICE)" --batch-size "$(EMBEDDING_BATCH_SIZE)" \
		--model-cache "$(GEOSNAP_MODEL_CACHE)" $(MOSCOW_V2_REUSE_ARGS)

benchmark-moscow-v2-calibration:
	HF_HOME="$(HF_HOME)" $(PYTHON) -m ml.evaluation.moscow_benchmark \
		--gallery-manifest "$(MOSCOW_V2_GALLERY)" --query-manifest "$(MOSCOW_V2_CALIBRATION)" \
		--output-dir "$(MOSCOW_V2_DIR)/reports" --model "$(MOSCOW_V2_MODEL)" \
		--device "$(TORCH_DEVICE)" --cache-dir "$(GEOSNAP_MODEL_CACHE)" \
		--batch-size "$(EMBEDDING_BATCH_SIZE)" --top-k "$(MOSCOW_V2_TOP_K)" \
		--estimator weighted_medoid --confidence-threshold 0 \
		--query-aggregation "$(MOSCOW_V2_QUERY_AGGREGATION)" \
		--gallery-embedding-dir "$(MOSCOW_V2_EMBEDDINGS)" \
		--report-stem "$(MOSCOW_V2_REPORT_STEM)"

calibrate-moscow-v2-confidence:
	$(PYTHON) -m ml.evaluation.calibrate_confidence \
		--benchmark-json "$(MOSCOW_V2_DIR)/reports/$(MOSCOW_V2_REPORT_STEM).json" \
		--output-dir "$(MOSCOW_V2_DIR)/reports" \
		--report-stem "$(MOSCOW_V2_REPORT_STEM)_confidence" \
		--minimum-conditional-accuracy-100m-wilson-lower-95 0.90
