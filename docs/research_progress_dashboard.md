# GeoSnap research progress dashboard

Open [the local dashboard](http://127.0.0.1:8771/) in Chrome. The **Update every** selector offers 0.5, 1, 2, 5, 10, 30 or 60 seconds; it defaults to 0.5 seconds and saves the choice between visits. Hidden tabs wait at least 5 seconds between requests. **Pause updates** stops requests until **Resume updates** is pressed, and the choice survives a reload. The server caches status within each 0.5-second interval. A per-user macOS LaunchAgent (`com.geosnap.research-dashboard`) keeps it available. The server binds only to `127.0.0.1` and is read-only. It does not modify the gallery, production system, predictions or research results.

The current 112,163-image quality run is detected directly from its committed 500-image chunk files. The count rises only when a chunk and its checksum receipt are present. The speed and ETA use recent committed chunks, so the estimate can change. Once the scan completes, the dashboard follows candidate selection, two VLM review passes, and the final review receipt. The photo strip is a sample from the most recent committed chunk; selecting a square shows a sample from that earlier chunk. It is not a frame-by-frame video of an unfinished chunk. Clicking a photo opens its deterministic measurements and strict defect flags. If the VLM has reviewed that photo, the same panel shows each recorded model pass and raw answer. Most photos will have no VLM answer during the technical scan because only suspicious photos proceed to VLM review; the panel says so explicitly.

Other research jobs can publish the same simple progress record without adding a dashboard dependency to their inference path:

```python
from ml.research.run_dashboard.publish import publish

publish("my-run", title="Descriptor extraction", phase="Encoding", completed=500,
        total=10000, unit="снимков", rate_per_second=12.3)
publish("my-run", title="Descriptor extraction", phase="Encoding", completed=10000,
        total=10000, unit="снимков", state="complete")
```

Or call `python -m ml.research.run_dashboard.publish --id my-run --title 'Descriptor extraction' --phase Encoding --completed 500 --total 10000 --unit снимков` after a checkpoint. Each update atomically replaces only `data/evaluation/research_run_dashboard/runs/my-run.json`. Research Python processes that do not publish progress appear separately with their command but no invented percentage.

To run manually:

```bash
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.run_dashboard.server --port 8771
```

The service was installed from `ml/research/run_dashboard/com.geosnap.research-dashboard.plist` into `~/Library/LaunchAgents/`. Manage it with:

```bash
launchctl print gui/$(id -u)/com.geosnap.research-dashboard
launchctl kickstart -k gui/$(id -u)/com.geosnap.research-dashboard
launchctl bootout gui/$(id -u)/com.geosnap.research-dashboard
```

If the repository or its Python runtime moves, update the absolute paths in the LaunchAgent plist and bootstrap it again. Server logs live under `data/evaluation/research_run_dashboard/`.
