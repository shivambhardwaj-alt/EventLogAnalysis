<<<<<<< HEAD
# EventLogAnalysis
Making the project for the event log analysis 
=======
# Event Funnel Analytics - v2

A production-pattern **data engineering pipeline** that turns raw application event logs into clean events, user sessions and funnel metrics. On top of that it:

- **detects incidents automatically**: a conversion drop on the day it starts, and new undeclared fields in the data;
- **finds the root cause segment**, for example a specific app release;
- **raises alerts**;
- **explains the biggest drop with an AI model** whose numbers are fact-checked;
- **serves a live dashboard + JSON API**.

Everything is **Python + SQL**, uses the **standard library only** (nothing to `pip install`), and runs on Windows, macOS and Linux.

---

## Run it (no code to write)

You need **Python 3.11 or newer**. Check with `python --version`, or `python3 --version` on macOS/Linux. If it's older, install the latest from https://www.python.org/downloads/; on Windows, tick "Add Python to PATH".

**Step 1.** Unzip the project and open a terminal inside the folder:

```
cd event-funnel-analytics
```

**Step 2.** Run everything:

| Windows | macOS / Linux |
|---|---|
| `python run_all.py` | `python3 run_all.py` |

This takes about 20 seconds and runs the whole story:
1. Clean start.
2. Generate 7 days of messy logs.
3. Load them **one day at a time**, the way a daily job would.
4. Rerun to prove nothing changes.
5. Run the 50 tests and the edge-case demo.

**Step 3.** Open the live dashboard:

| Windows | macOS / Linux |
|---|---|
| `python -m funnel serve` | `python3 -m funnel serve` |

Then open **http://127.0.0.1:8050** in your browser. Press `Ctrl+C` in the terminal to stop it.

Shortcut: `python run_all.py --serve` does steps 2 and 3 together.

The static report is also written to `output/report.html` (double-click to open). CSV exports are in `output/exports/`.

### What you will see in step 2

```
 day file(s)                                          rows  accepted  quarant.   dups  users  largest drop
 1   events_day_01.jsonl + partner_export_day_01.csv  7,573     7,288       149    136  1,261  add_to_cart -29%
 ...
 5   events_day_05.jsonl                             7,123     6,797       112    214  1,508  add_to_cart -26%
      ALERT [critical] funnel_conversion_drop: Conversion into 'add_payment_info' fell from 83.8% to 38.6% (on 2026-10-09); it started 2026-10-09 ...
 6   events_day_06.jsonl                             7,267     6,956       147    164  1,473  add_payment_info -29%
 7   events_day_07.jsonl                             6,757     6,446       117    194  1,351  add_payment_info -33%
      ALERT [warning] schema_drift: Undeclared field 'experiment_id' appeared in events_day_07.jsonl ...
```

The buggy app release ships on day 5. The anomaly detector raises the alert **the same day**, even though the overall "largest drop" only switches to the payment step a day later. That gap is why monitoring daily trends beats looking at totals.

---

## Optional: turn on a real LLM, or Slack alerts

Without any setup, explanations come from an offline rule-based explainer and alerts are printed and stored.

1. Copy `.env.example` to `.env`. On Windows: `copy .env.example .env`. On macOS/Linux: `cp .env.example .env`.
2. Edit `.env`:
   - **Free LLM (Groq):** create a key at https://console.groq.com/keys and set `GROQ_API_KEY=gsk_...`
   - **Fully local LLM:** install https://ollama.com, run `ollama pull llama3.2`, and set `AI_PROVIDER=ollama`
   - **Alerts to Slack/Discord/Teams:** set `ALERT_WEBHOOK_URL=` to your channel's incoming-webhook URL
3. Run `python run_all.py` again. The explanation card now shows "AI".

`.env` is git-ignored. Keys are never logged, printed, stored or shown in error messages.

---

## All commands

| Command | What it does |
|---|---|
| `python run_all.py [--quick] [--serve]` | The full story from a clean state |
| `python -m funnel serve [--port 8050]` | Live dashboard + JSON API |
| `python -m funnel generate --clean` | Write 7 days of synthetic logs into `data/incoming/` |
| `python -m funnel run` | Incremental run: new files only → clean → sessions → funnel → alerts → report |
| `python -m funnel run --full-refresh` | Rebuild all derived tables from the raw layer |
| `python -m funnel run --gap-minutes 15` | Use a different session gap (triggers an automatic rebuild) |
| `python -m funnel watch --interval 30` | Micro-batch mode: process new files automatically as they land |
| `python -m funnel forget-user u_00042` | Right to be forgotten: erase a user from every layer |
| `python -m funnel benchmark --users 20000` | Throughput test on generated data |
| `python -m funnel status` | Row counts per table and run history |
| `python -m funnel query "SELECT * FROM daily_funnel"` | Ad-hoc read-only SQL |
| `python -m funnel demo-edge-cases` | Failure and edge-case demonstration |
| `python -m unittest discover -s tests -t .` | 50 tests (or `pytest`) |
| `python -m funnel reset --yes` | Delete the warehouse and reports |

---

## What it does

```
data/incoming/*.jsonl|*.csv
   │ ingest.py           sha256 per file (idempotent), one transaction per file, erased users filtered
   ▼
BRONZE  raw_events                    every line exactly as received (replayable)
   │ 01_validate.sql     parse JSON, cast types, normalise, 9 rejection rules ──► quarantine_events
   │ 02_dedupe.sql       exact + cross-batch + conflicting duplicates        ──► duplicate_events
   ▼
SILVER  clean_events                  event_id PRIMARY KEY
   │ 03_near_duplicates  double clicks flagged (LAG window)
   │ 04_sessionize       30-min inactivity gap (LAG + running SUM) - only affected users recomputed
   ▼
GOLD    sessions · funnel_session_steps · funnel_metrics · funnel_segments · daily_funnel
   │ 11_schema_drift     fields outside the data contract
   │ 08_quality_checks   10 invariants (every raw row accounted for, no gap > 30 min in a session …)
   │ anomaly detection   day vs. healthy baseline, two-proportion z-test
   │ largest drop        + segment root cause (z-tests) + AI explanation (guardrailed, cached)
   │ alerts              deduplicated, log + webhook
   ▼
report.html · report.json · exports/*.csv · live dashboard (/api/*)
```

### Production features and where they live

| Feature | Why real teams need it | Where |
|---|---|---|
| Medallion layers (bronze/silver/gold) | Raw data is never lost, so everything can be rebuilt | `sql/schema.sql` |
| Idempotent ingestion | Reruns and retries are safe | `ingest.py` (content hash) |
| Incremental processing | Only new rows and affected users are recomputed | `pipeline.py`, `_batch`, `_affected_users` |
| Quarantine with reasons | Bad data neither crashes nor disappears | `01_validate.sql` |
| Duplicate handling | Exact, cross-day, conflicting ids, double clicks | `02_dedupe.sql`, `03_near_duplicates.sql` |
| Sessionization in SQL | Window functions, late events merge sessions | `04_sessionize.sql` |
| Data-quality assertions | Like dbt tests, run after every load | `08_quality_checks.sql` |
| **Anomaly detection** | Catches a regression on the day it starts | `metrics.detect_anomalies` |
| **Root-cause segments** | e.g. *app_version = 2.4.0* continues at 12% vs 84% | `metrics.segment_findings` |
| **Data contract / schema drift** | Producers adding fields silently is a top cause of breakage | `11_schema_drift.sql`, `[contract]` in config |
| **Alerting** | Stable keys so each problem alerts once; Slack/Discord/Teams webhook | `alerts.py` |
| **Run lock** | Two runs can never write at once; stale locks recover | `lock.py` |
| **Schema migrations** | Upgrading the code rebuilds derived tables from raw automatically | `db.apply_schema` |
| **Observability** | Every task timed and stored per run (`task_runs`) | `pipeline.TaskTracker` |
| **Right to be forgotten** | GDPR / India DPDP: erase everywhere, block re-ingestion | `privacy.py` |
| **Live dashboard + API** | Filter the funnel live by device / version / date; user lookup | `server.py`, `web/dashboard.html` |
| **Micro-batch watch mode** | Continuous processing without an orchestrator | `funnel watch` |
| AI with guardrails | No PII sent; invented numbers or dates are rejected; fallback; cache | `ai/` |
| Config-change detection | Changing the session gap rebuilds automatically | `config.processing_fingerprint` |

### Dashboard API

| Endpoint | Returns |
|---|---|
| `/api/funnel?device=mobile&app_version=2.4.0&date_from=2026-10-09` | Funnel computed live in SQL for any filter |
| `/api/daily` | Daily conversion per step |
| `/api/anomalies` | Flagged days |
| `/api/segments?step=4` | Segment breakdown for one step |
| `/api/users/u_00042` | One user's sessions and events |
| `/api/alerts` | Latest alerts |
| `/api/runs` | Run history with task timings |
| `/api/health` | Liveness and last run status |
| `/api/dimensions` | Values available for each filter |
| `/api/quality` | Data-quality summary from the last run |
| `/api/explanation` | Largest drop and its explanation from the last run |

All endpoints are read-only and every filter is a bound SQL parameter, so injection is impossible (a test checks this).

---

## Tests (50) and the edge-case demo

| File | Covers |
|---|---|
| `test_ingestion_and_cleaning.py` | Idempotent ingestion, renamed/changed files, CSV typing, empty files, all 9 rejection reasons, 5 timestamp formats |
| `test_dedup_and_sessions.py` | Exact, cross-batch and conflicting duplicates; double clicks; **30:00 vs 30:01 boundary**; out-of-order events; late event merging sessions; gap change → rebuild |
| `test_funnel.py` | Hand-calculated conversion math, strict ordering, no data, segment attribution |
| `test_ai.py` | Guardrails, fallback on error/crash/hallucination, key hygiene, HTTP clients against a fake server, cache |
| `test_production_features.py` | Anomaly flagged from day 1 of a regression and **not** for noise; alert dedup; webhook (+ failure not fatal); schema drift; run lock; schema migration; **forget-user across all layers + blocked re-ingestion**; dashboard API + injection safety |
| `test_end_to_end.py` | Finds **exactly** what the generator planted (quarantine counts, duplicates, release day, drift fields, alerts); rerun is a no-op; **day-by-day incremental == full rebuild** |

`python -m funnel demo-edge-cases` walks through each of these, line by line:
- 21 crafted bad rows and what happened to each
- a rerun that changes nothing
- a late event merging two sessions
- empty and broken files
- an AI provider that is down
- an AI that invents numbers
- an invalid config
- a gap change triggering a rebuild

## Performance (measured)

`python -m funnel benchmark --users 50000` on a laptop-class cloud VM:

| Measure | Result |
|---|---|
| Raw rows | 536,756 |
| Full pipeline | 74 s (~7,000 rows/s) |
| No-op rerun | 6 s |
| Warehouse size | 0.55 GB |

The slowest stages are sessionize, validate and dedupe. That is the point where you would move to DuckDB, Snowflake or BigQuery. The SQL is standard (CTEs, window functions, JSON) and ports with small dialect changes.

## Honest limits and next steps

| Here | In a large company |
|---|---|
| SQLite | Snowflake / BigQuery / Databricks |
| `watch` or cron | Airflow / Dagster |
| SQL files | dbt models |
| Webhook | PagerDuty |
| Synthetic data | Real PII with access control |

The patterns are the same; that is the point of the project.

## Project layout

```
event-funnel-analytics/
├── run_all.py                  ONE command: the whole story
├── config/pipeline.toml        all settings (no secrets)
├── .env.example                secrets template → copy to .env (git-ignored)
├── funnel/
│   ├── sql/                    schema + 12 SQL stages / checks / API queries
│   ├── ingest.py  pipeline.py  metrics.py  quality.py  alerts.py  lock.py  privacy.py
│   ├── server.py  web/dashboard.html          live dashboard + API
│   ├── report.py  generator.py  demo.py  cli.py  config.py  db.py  env.py
│   └── ai/   base.py (interface) · providers.py · rule_based.py · guardrails.py · service.py
├── tests/                      50 tests
└── Makefile · Dockerfile · pyproject.toml · .github/workflows/ci.yml (Windows + Linux)
```
>>>>>>> cbf0c60 (Making the project and it is not working till now)
