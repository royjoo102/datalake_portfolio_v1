# M5: Read-only dashboard

M5 adds an English Streamlit dashboard over the existing MinIO lake. Refresh is manual, and the app does not produce Kafka messages, run Spark, modify rules, deduplicate events, or write lake data.

## Start

From the repository root:

```bash
source datalake_env/bin/activate
python -m pip install -r requirements-dashboard.txt
./scripts/run-dashboard.sh
```

Open `http://127.0.0.1:8501`. Keep the process running; Ctrl+C stops the dashboard. It binds to loopback and disables Streamlit usage statistics. Set `DASHBOARD_PORT` to use another port and `DASHBOARD_LAKE_ID` to change the initial lake. The default is `m4-gce-june`. MinIO must be running and M4 must have published a Gold snapshot. Kafka is optional for event browsing; its status is shown as unavailable if the broker cannot be queried.

The first session load reads data once. **Refresh data** reloads source data and operational measurements. Changing dates, replay runs, explorer controls, or tabs only uses the session's in-memory snapshot. Editing Lake ID takes effect only when Refresh data is selected. A failed refresh retains the previous successful snapshot with an explicit warning; a failed initial load shows an error without invented metrics. A new browser session loads a new snapshot.

## Views and scope

| Area | Evidence | Scope |
| --- | --- | --- |
| Headline counts | Gold data-quality Parquet | Selected event date range and replay run |
| Overview | Daily Silver volume, timestamp quality, rejection reasons and details | Same event filters |
| Event explorer | Silver events, either event-type field, channel and application-version distributions | Event filters plus local channel selection |
| Pipeline | Kafka end offsets, committed MinIO offsets, Spark progress logs, replay checkpoints | Whole selected lake at refresh time |
| Storage | Commit-manifest file metadata and optional M4 comparison report | Whole published Gold snapshot; comparison has its own fixed baseline |

Dates use America/Los_Angeles. Unknown dates can be included with dated events, excluded, or selected alone. Unknown dates are independent of the entered date range. All runs includes events whose run cannot be identified. Reset event filters restores all runs and the default date coverage/range.

Event classification can use `event_type` or `attributes_event_type`; neither is treated as authoritative over the other. Charts show the top 20 categories where applicable. The records table displays 100, 500, or 1,000 rows ordered by UTC event time, then source ID, with null times last. Users can choose columns. CSV export includes exactly the displayed rows and columns, not all matching records. Both source event IDs and Kafka coordinates remain available for inspection. Business duplicates are retained.

## Snapshot consistency and metric definitions

At refresh, `dashboard/data.py` captures Gold's `latest.json` pointer first, then reads only the immutable source commits listed in that pointer. `lake.query.connection` now accepts an optional explicit Gold snapshot so a pointer update during loading cannot mix newer Gold with older Silver. The loader reconciles Gold Raw/Silver/Rejected totals with the covered source files and Gold volume with Silver before accepting a snapshot. A newer committed batch not yet in Gold produces a visible coverage warning. Refresh loads the new Gold version once M4 has published it.

- Rejected ratio = summed Rejected count / summed Raw count.
- Null timestamp ratio = summed null Silver timestamp count / summed Silver count.
- A zero denominator is N/A, not zero percent.
- Multiple rejection reasons can count the same event more than once; their total need not equal Rejected events.
- Dates with no stored events are not filled in with fabricated observations.
- KPI dates describe event occurrence; snapshot and load timestamps describe freshness.

Pipeline offset gap is Kafka's end offset minus the highest committed MinIO end offset for that partition. It is an offset distance, not consumer-group lag or a guaranteed record count. A partition without a committed end has an unavailable gap; negative gaps are shown rather than clamped, indicating a possible source reset. This operational view includes commits newer than Gold and is intentionally independent of the event filters.

Spark's `numInputRows` may count source rereads within `foreachBatch`. Batch throughput instead uses the source start/end offset difference divided by trigger duration, and is labeled offsets per second. Missing start offsets or duration produce unavailable values. Duplicate progress reports for the same query/batch are collapsed. Logs are historical execution observations, not live process-health checks. M4's availableNow job normally exits after its captured range finishes.

Replay progress shows the last acknowledged CSV row and whether EOF was reached. A percentage is not displayed without a verified total row count. End-to-end processing latency is not instrumented; historical event timestamps are never subtracted from today's time as a latency metric. CTR, conversion, and business deduplication metrics remain undefined and are not fabricated.

The storage comparison is a separately labeled M4 validation baseline from `verification.json`: the same input rows, compression and row-group settings, comparing 53 versus 24 business columns. It is not recomputed for event filters and is not a claim about total lake savings. The physical-size view includes the source data files covered by Gold, excluding older Gold versions and orphan uploads.

## Implementation and limits

- `dashboard/app.py`: Streamlit layout, English labels, controls, charts, records, and export.
- `dashboard/data.py`: consistent source loading, scope filtering, rate calculations, and operational reads.
- `requirements-dashboard.txt`: dashboard dependencies separate from the Spark image.
- `scripts/run-dashboard.sh`: local server launcher.
- `tests/test_dashboard.py`: offline metric and application-state tests.
- `scripts/test-dashboard-integration.py`: real MinIO data and Streamlit AppTest acceptance checks.

The dashboard uses existing DuckDB/Arrow queries and holds each session's selected lake snapshot in memory. It does not create a local event database. This local M5 implementation refuses snapshots with more than 100,000 Silver or Rejected rows rather than silently truncating aggregates; larger workloads require server-side filtering and pagination. Credentials stay in the local environment and server-side clients. No background refresh or cloud publication is configured.

## Validation

```bash
python -m unittest discover -s tests -v
python scripts/test-dashboard-integration.py
```

AppTest checks metric totals, run selection and restoration, unknown dates, filter reset, classification selection, and explicit refresh behavior. Offline tests also cover weighted rates, zero denominators, service errors with old-snapshot retention, and Spark reread accounting. The integration report is written to `data/m5/acceptance.json` (ignored by Git). Local server logs are in `logs/m5/` when launched with output redirection.

### Verified local result (2026-09-14)

- Full unit suite: **34 tests passed**, including eight new dashboard tests.
- Real MinIO/Streamlit AppTest: **12,614 Raw, 12,609 Silver, 5 Rejected**, matching published Gold and its source coverage. No null Silver timestamps in this main lake snapshot.
- Kafka partitions 0/1/2 had end offsets **12610/1/3**, matching the committed MinIO boundaries at verification time.
- Run filtering, unknown-only selection, reset, classification switching, and manual-only refresh passed. Service-failure and empty-result behaviors were checked offline.
- Headless local Chrome opened all four tabs without page errors. Desktop (1440 × 1050) and narrow (390 × 844) captures were inspected; charts rendered with readable labels and responsive metric stacking. Detailed record views and the CSV download were exercised; the downloaded selection contained exactly 100 rows.
- Evidence: `data/m5/acceptance.json`, `logs/m5/browser-check.json`, and screenshots under `logs/m5/`. Runtime artifacts are ignored by Git. Browser checks used Playwright 1.55.0 with the installed Chrome; Playwright is a verification tool, not a dashboard runtime dependency.

M5 provides the agreed initial browsing surface. It does not add a scheduler, business deduplication, business-specific conversion definitions, a verified total-CSV progress percentage, or distributed query scaling.
