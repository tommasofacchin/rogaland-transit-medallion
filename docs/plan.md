# Project Plan: Public Transport Reliability in Rogaland (Entur)

Plan for the DAT535 Data Engineering project (Bronze → Silver → Gold). Group: **Tommaso Facchin** and **Oliwier Trawinski**. The structure follows [Guideline.md](Guideline.md): sections 1-6 are the project definition for the check-in (Silver most detailed, then Bronze, then Gold), sections 7-11 are the execution plan.

## Deadlines

| Date | Event |
|---|---|
| Fri 13 Nov | Presentation for the Cloud course (other course, takes time that week) |
| **Mon 16 Nov** | **Data Engineering presentation** |
| ~Thu 26 - Fri 27 Nov | Final report, about 1.5 weeks after the presentation (exact date to confirm) |

Only about 3 weeks remain before the presentation, so the pipeline must run end to end by 12 November. The window after the presentation is for evidence, performance measurements and reflection.

---

## 1. Project statement (SCOPE)

**What we build.** A batch pipeline that collects real-time public transport data from Entur (SIRI estimated timetable) every few minutes and turns it into trusted, documented datasets for analysing punctuality. Optional extras: GTFS static timetable and MET Norway weather.

**Problem.** The Entur feed only shows the current state. It has no history, it is nested XML, and the same trip is repeated in every snapshot with updated times. Nobody can answer "which lines, stops and hours are unreliable?" from the raw feed without first collecting, flattening and deduplicating it.

**Intended outcome.** A Gold table with punctuality (share of calls on time) and average delay per line, stop and time of day, ready for a dashboard, a chart or a simple SQL query.

**Users.** Passengers and planners in the Stavanger/Rogaland area. To keep the scope bounded we focus on one operator (KOL) or on a bounded area, depending on what the filter test shows (see open items).

**Intentionally out of scope.**
- Streaming and real-time dashboards (we poll in batch).
- Delay prediction or any ML model.
- A full web application (Gold gets a minimal consumer for the demo only).
- Operators or regions outside Rogaland.

---

## 2. Dataset (SOURCE AND SUITABILITY)

**Source and format.** Entur real-time API, SIRI estimated timetable (XML / SIRI Lite). No API key, but an `ET-Client-Name` header identifying us is required. Observed rate limit: 5 requests per minute.

**Structure of one response.** A snapshot of many vehicle journeys. For each journey:
- `RecordedCalls`: stops already passed, with aimed time and actual time.
- `EstimatedCalls`: stops still to come, with aimed time and expected time.
- Journey-level fields: line, direction, operator, vehicle mode, cancellation flag, extra-journey flag.

**Refresh pattern.** The feed changes continuously. We poll on a schedule (start with every 15 minutes, move to 5 if the retention test shows finished trips disappear quickly).

**Volume and suitability for distributed processing.** One snapshot is small, but snapshots accumulate: with 15-minute polling that is 96 files per day, about 2000 files over 3 weeks, each holding hundreds of journeys with many stop calls each. After flattening, this becomes millions of stop-call rows with heavy duplication (every snapshot repeats ongoing trips). The dedup (window/aggregation over a trip key), the XML flattening and the joins are the kind of shuffle-heavy work where Spark is justified. We will state honestly in the report where the data is small enough that Spark is not strictly needed, and use the growing history to show scaling behaviour.

**Reference data.**
- Required: none (the SIRI data carries line, stop and aimed times).
- Optional: GTFS static timetable (lines, stops, trips) for enrichment; MET Norway weather by hour.

**Known limitations and quality risks.**
- No history: collection must start in week 1, and gaps in collection are permanent.
- The `datasource=KOL` parameter did not filter SIRI Lite results (only 1 of 50 journeys was KOL); the correct filter is still to be found.
- Cancellation fields are not filled by every operator.
- Past stops carry actual times, future stops only predictions; mixing them naively biases delay statistics.
- Predicted times change between snapshots; only the last snapshot of a finished call is close to the truth.
- Possible schema differences between operators and over time.

**Access, licensing, reproducibility.** Entur data is open (NLOD licence, to be confirmed and cited in the report). The client name header and rate limit are documented in the README. Because the API has no history, reproducibility rests on the Bronze archive: Bronze files are immutable, so Silver and Gold can be rebuilt from them at any time. A small sample of Bronze files is committed (or stored separately) for tests and as a demo fallback.

---

## 3. Bronze layer (INGEST AND PRESERVE)

**What is preserved unchanged.** The raw HTTP response body of every call, byte for byte, with no parsing or cleaning.

**Ingestion method and schedule.** A Python ingestion script calls the Entur endpoint once per run and writes the raw response to storage. It is triggered by a GitHub Actions workflow with a cron schedule. Batch only. GitHub cron can be delayed or skipped under load, so the actual run time is recorded and Silver must tolerate gaps.

**Layout.**
```
bronze/entur_siri/dt=YYYY-MM-DD/hour=HH/entur_siri_<run_id>.xml
bronze/entur_siri/_metadata/   (one small JSON/row per run)
bronze/gtfs/dt=YYYY-MM-DD/     (optional, periodic download of the static feed)
```
Partitioned by dataset and date/hour, so Silver can read only the periods it needs.

**Traceability metadata (per file / per run).**
- `source` and `endpoint` (full URL with parameters)
- `dataset_type`
- `ingestion_time` (UTC), `run_id` (GitHub run id)
- HTTP status, response size, checksum (to detect identical re-ingestion)
- rate-limit headers seen

**Duplicates, late data, schema changes.**
- *Duplicate ingestion:* the run id is part of the filename and the checksum is stored; a rerun of the same run id overwrites nothing and is detected.
- *Late data:* the feed is a snapshot, so there is no "late file"; delays show up later as updated times in a later snapshot. Silver handles this with the latest-version rule.
- *Schema changes:* Bronze stores raw XML, so it never breaks; schema checks happen in Silver.
- *Failures:* retry with backoff on 5xx/timeouts, respect the rate limit, and log failed runs to the metadata area so gaps are visible.

**Testing and deployment.** Unit tests for the request builder, filename/partition logic and metadata. The workflow runs tests on every pull request. The same script is deployed to dev (writes to a dev bucket/folder, manual or less frequent trigger) and prod (scheduled), selected by configuration.

**Outcome.** Raw data is reproducibly ingested, traceable to its source, and ready for controlled processing.

---

## 4. Silver layer (PRIMARY FOCUS)

### 4.1 Data understanding
- One Bronze file = one snapshot of many vehicle journeys at one moment.
- One *journey* = one trip of a vehicle on a line on a service date.
- One *call* = one stop of one journey, either recorded (past) or estimated (future).
- For each field we document: essential (journey id, line, stop id, stop order, aimed time), optional (actual/expected time, platform, destination), derived (delay, service date, call status) and unreliable (expected times, cancellation flag, extra-journey flag).
- This is written up as a short data dictionary in the repo and used in the report.

### 4.2 Data model
- **Grain:** one stop call of one trip on one service date.
- **Key:** `journey_id + service_date + stop_order`.
- **Main table `silver.stop_calls`:** key, line, direction, operator, mode, stop id and name, aimed arrival/departure, actual or expected arrival/departure, call status (`recorded` / `estimated`), cancelled flag, extra-journey flag, `snapshot_time` of the kept version, `run_id` for lineage.
- **Reference tables:** `silver.stops`, `silver.lines` (from the SIRI data first, enriched from GTFS if time allows).
- **History:** we keep only the latest known version of each call, plus its snapshot time. Keeping every version is possible but explodes volume; we document this decision and its trade-off (we lose the history of how predictions changed).
- **Rejected records:** a separate table `silver.rejected_calls` with the reason code, so rejection is visible and countable.

### 4.3 Cleaning rules
Each rule is explicit, named, tested, and has a decided action.

| Rule | Action |
|---|---|
| Missing aimed time | Reject (cannot compute delay) |
| Missing actual/expected time | Keep, delay = null, flag |
| Unparseable timestamp | Reject |
| Delay outside a plausible range (e.g. < -10 min or > +3 h, thresholds to be tuned from the data) | Flag as outlier, excluded from Gold |
| Cancelled journey | Keep, flagged, excluded from delay statistics but counted in a cancellation rate |
| Extra journey (not in timetable) | Keep, flagged, excluded from punctuality |
| Exact duplicate rows | Drop |
| Same key in several snapshots | Keep latest version (see dedup) |
| Missing line or stop id | Reject |
| Stop name encoding problems | Normalise to UTF-8, flag if still broken |

### 4.4 Standardization
- Flatten XML to one row per call.
- Timestamps converted to UTC; original offset kept in a separate column; service date derived in local time (Europe/Oslo) so night trips belong to the correct day.
- Consistent column names (snake_case) and explicit types (no inferred schema).
- Enumerations (mode, call status) mapped to controlled values.
- Explicit schema defined in code and checked in tests.

### 4.5 Deduplication
The same call appears in every snapshot until the trip ends. Rule: for each key keep the row from the latest snapshot; prefer a `recorded` call with an actual time over an `estimated` one. Implemented with a window function (`row_number()` over key ordered by snapshot time) and compared against a `groupBy` + max-struct approach in the performance section.

### 4.6 Enrichment
- Derived columns: arrival/departure delay in seconds, hour of day, weekday, weekend flag, rush-hour flag.
- Optional join with GTFS (route names, stop coordinates) and weather (hourly). Treated as enrichment, not a dependency.

### 4.7 Quality evidence
Measured before and after Silver and stored in a quality table per run:
- rows read, rows accepted, rows rejected (per reason)
- missing values per essential column
- duplicate count before/after dedup
- invalid/outlier delays
- snapshot count and gaps in collection time
- schema check result

This table is what we show in the report and demo as "proof of improvement".

### 4.8 Implementation (Spark)
- Basic transformations and actions first (`select`, `filter`, `withColumn`, `explode`, `groupBy`, window functions), to show understanding of narrow vs wide transformations and when an action triggers execution.
- High-level DataFrame and Spark SQL for readability where appropriate.
- XML parsing: first test whether the available Spark version reads XML natively; otherwise the `spark-xml` package or a Python parse in a mapped step. Decided in the first Silver task on a single file.
- Output as Parquet, partitioned by service date.

### 4.9 Performance
Candidates to measure (not assume):
- XML read and flatten (many small files vs fewer larger ones).
- Dedup (window vs `groupBy`/aggregation), the main shuffle.
- Join with reference data (broadcast vs shuffle join).
- Partitioning by service date, early filtering, partition pruning.
- Skew on busy lines/stops.

For each: baseline timing, Spark UI evidence (stages, shuffle read/write, spill), the change, and the timing after. We only keep changes backed by numbers and report the ones that did not help.

### 4.10 Lifecycle
- Develop on feature branches, merge through pull requests reviewed by the other person.
- Unit tests for each cleaning rule and for the schema in GitHub Actions on every pull request.
- Deploy to dev first, then prod, with separate config.
- Rerun: Silver is a deterministic function of Bronze, so a full rebuild or a rebuild of selected dates is always possible.
- Monitor: quality table and run log per execution.

### 4.11 Scalability
- More volume: partition by date, incremental processing of new Bronze partitions instead of full rebuild, more executors.
- Late data / collection gaps: handled by latest-version rule; gaps recorded, not hidden.
- Schema evolution: explicit schema plus a schema check that fails loudly (or quarantines) instead of silently dropping fields.
- Honest limit: the current design is batch and single-source; streaming ingestion and a table format with merge support (Delta/Iceberg) would be the next step.

**Outcome.** A trusted, validated, documented and reusable dataset that can support multiple downstream uses.

---

## 5. Gold layer (SERVE THE OUTCOME)

**Consumer need.** "Which lines, stops and times of day in Rogaland are least punctual?" Consumed as a chart or SQL query in the demo.

**Why different from Silver.** Silver keeps every call (millions of rows, reusable). Gold is small and purpose-specific: pre-aggregated, with only the measures the consumer needs.

**Structure.**
- `gold.punctuality_by_line_hour`: line, service date (or weekday), hour of day, number of calls, share on time, average delay (s), p90 delay, cancellation rate.
- `gold.punctuality_by_stop`: stop, same measures.
- Definition of "late": delay above a chosen threshold (e.g. 3 minutes, configurable and documented). Delay = actual (or expected) minus aimed, in seconds.
- Only non-cancelled, non-extra, non-outlier calls enter delay measures; cancellations reported separately.

**Correctness.**
- Gold call counts reconcile with Silver counts for the same filters.
- Spot checks of a few lines against raw Bronze data.
- Minimum sample size per group (groups with very few calls are hidden or flagged).

**Refresh and readiness.** Rebuilt after each successful Silver run (daily in prod). A `_SUCCESS`/status record with timestamp and quality status tells consumers it is ready.

**Failure behaviour.** If quality checks fail (too many rejected rows, missing hours of collection, schema check failed) Gold is not published; the previous version stays in place and the failure is logged.

**Outcome.** A curated dataset optimised for a defined dashboard/report.

---

## 6. End-to-end delivery (OPERATE THE LIFECYCLE)

- **Reproducible:** repo contains source, tests, config, workflows and a README so another engineer can clone, configure and run it.
- **Environments:** `config/dev.yaml` and `config/prod.yaml`; secrets only in GitHub Secrets; dev uses less frequent runs and a small data window.
- **Automation:** GitHub Actions for (1) tests/lint on pull requests, (2) deploy on merge, (3) scheduled ingestion, (4) scheduled Silver → Gold run.
- **Monitoring:** run status, failures, rejected-record counts and quality results written to a log/quality table and surfaced in the workflow summary.
- **Operating lifecycle:** Develop (branches + PRs) → Validate (CI) → Deploy (dev, prod) → Run (schedule) → Learn (logs, failure analysis, performance reflection).
- **Documentation of decisions:** assumptions, trade-offs and limits are written in the repo as we go, so the report is not written from memory.

---

## 7. Open items to settle first

| Item | Why it matters | Owner |
|---|---|---|
| Check-in date for the 1-2 page definition, exact report date, whether the demo is also on 16 Nov | Fixes the milestones below | Both |
| Cloud platform and storage for dev/prod | Needed for the cloud setup deliverable | Tommaso |
| Correct operator filter for Rogaland | Controls volume and scope | Tommaso |
| How long finished trips stay in the feed | Decides the polling interval (15 vs 5 min) | Tommaso |
| Entur terms: `ET-Client-Name`, licence, rate limit | Needed in the report | Tommaso |
| Does our Spark version read XML natively, or is a package needed | Decides the Silver parsing approach | Oliwier |
| How `ServiceJourney` ids map to GTFS `trip_id` | Needed for the timetable join | Oliwier |
| Plausible delay thresholds (outliers, "late") | Needed for cleaning rules and Gold | Oliwier |

---

## 8. Work packages

### WP0: Setup (24-27 Oct)
- GitHub repo, branch rules (feature branches + pull requests), folder structure (`src/`, `tests/`, `config/`, `docs/`, `.github/workflows/`).
- Separate dev and prod configuration; secrets in GitHub Secrets.
- Agree on Bronze layout and metadata columns.
- Decide cloud platform/storage.

### WP1: Bronze (24-30 Oct)
- Ingestion script storing the raw response unchanged.
- Actions workflow on a schedule (15 minutes first).
- Retries, rate-limit handling, duplicate detection, per-run metadata and logging.
- Feed-retention test (how long finished trips stay visible) and operator-filter test.
- Optional: periodic GTFS static download.

### WP2: Silver (27 Oct - 12 Nov, primary focus)
1. *First task:* parse one raw file in Spark, decide the XML approach.
2. Data understanding and data dictionary.
3. Flatten to one row per call, standardize timestamps and types, explicit schema.
4. Dedup (latest version per key).
5. Cleaning rules with accept/reject/flag, rejected table.
6. Quality metrics table (before/after).
7. Unit tests for rules and schema, run in CI.
8. Performance profiling and evidence-based tuning.
9. Optional: GTFS / weather enrichment.

### WP3: Gold (5-12 Nov)
- Delay measures and "late" threshold.
- Aggregations by line / stop / hour.
- Reconciliation with Silver; publish gating on quality checks.
- Minimal consumer for the demo (one query or chart).

### WP4: Delivery (27 Oct - 12 Nov, in parallel)
- CI on every pull request.
- Deployment to dev and prod; scheduled end-to-end run Bronze → Gold.
- Visibility of run status, failures, rejected records, quality results.
- README with clone-and-run instructions.

### WP5: Presentation, demo and report
- 10-minute presentation (16 Nov): main decisions, evidence, selected lessons.
- Live demo plan with stored-data fallback.
- Report (8-10 pages), see section 11.

---

## 9. Task division

Silver is the main focus, so it is split along a clear line: Tommaso owns getting data in and keeping the system running; Oliwier owns what the data means.

| Area | Tommaso (pipeline and platform) | Oliwier (data and analysis) |
|---|---|---|
| WP0 Setup | Repo, branches, environments, secrets, cloud | Project statement and dataset sections of the check-in |
| WP1 Bronze | **Owner.** Ingestion, Actions schedule, layout, retries, metadata | Reviews PRs; helps with the retention test |
| WP2 Silver | Parsing, flattening, timestamps and types, dedup, Silver tests | **Owner of model and rules.** Data understanding, cleaning rules, quality metrics, enrichment, performance profiling |
| WP3 Gold | Publish gating, refresh and readiness signal | **Owner.** Delay measures, aggregation, validation, demo consumer |
| WP4 Delivery | **Owner.** CI, deployment, monitoring, README | Quality results visible in logs |
| WP5 Report | Bronze, architecture, delivery and reflection | Dataset, Silver, Gold and results |
| Presentation / demo | Pipeline and deployment part | Data and results part |

Working rules:
- Every change goes through a pull request reviewed by the other person (this is the guideline's "develop and validate" step and keeps both of us able to explain the whole pipeline).
- Short weekly sync to compare progress with the timeline.
- Both of us must be able to answer questions on all layers in the demo.
- Decisions and measurements are noted in the repo as they happen.

---

## 10. Timeline

| Dates | Milestone |
|---|---|
| 24-27 Oct | Repo and environments ready; first Bronze ingestion on a schedule; collection started |
| 28-30 Oct | Bronze stable with metadata and failure handling; 1-2 page definition written; retention and filter tests done |
| 31 Oct - 4 Nov | First Silver version (parsed, one row per call, dedup, timestamps); data dictionary; CI running tests |
| 5-9 Nov | Cleaning rules and quality metrics; first Gold table; scheduled run in prod |
| 10-12 Nov | Gold validated and published through the pipeline; performance profiling; enrichment only if time allows; **pipeline frozen on 12 Nov** |
| 13 Nov | Cloud course presentation (limited time for this project); prepare slides |
| 14-15 Nov | Demo rehearsal with stored-data fallback; slides final |
| **16 Nov** | **Presentation (10 min) and demo** |
| 17-25 Nov | Report: results, quality and performance evidence, reflection; pipeline keeps collecting |
| ~26-27 Nov | Final report and repository |

Scope cut for the short schedule: first Silver and Gold use only the aimed, actual and expected times already in the SIRI data. GTFS and weather are optional.

---

## 11. Report and deliverables

### Report outline (8-10 pages, organised by decisions, not chronology)
1. Problem, scope and architecture overview (1 page)
2. Data source and understanding, limitations (1 page)
3. Bronze: ingestion, preservation, traceability (1 page)
4. Silver: model, cleaning rules, quality evidence (2-3 pages)
5. Silver: Spark implementation and performance evidence (1-2 pages)
6. Gold: consumer, measures, validation (1 page)
7. Delivery: CI/CD, environments, scheduling, monitoring (1 page)
8. Reflection: what worked, what did not, why, what to improve (1 page)

Evidence to collect along the way: quality table snapshots, before/after counts, Spark UI screenshots and timings, a few short code excerpts, test and workflow run screenshots, run logs.

### Deliverables checklist

| # | Deliverable | Done when |
|---|---|---|
| 01 | Report | 8-10 pages following the outline |
| 02 | Presentation | 10 minutes, rehearsed, main decisions + evidence |
| 03 | Cloud setup | Dev and prod environments functioning |
| 04 | GitHub repository | Code, tests, workflows, config, README |
| 05 | Demonstration | End-to-end run, deployment, schedule, Gold output |

---

## 12. Risks

| Risk | Mitigation |
|---|---|
| Collection starts late and the dataset is too small (about 3 weeks before 16 Nov) | Start Bronze by 27 Oct; wider area or shorter interval if volume is low; keep collecting until the report |
| Cloud presentation on 13 Nov takes time before ours | Freeze pipeline on 12 Nov; slides and rehearsal on 14-15 Nov |
| Cron runs delayed or skipped | Record run time in Bronze; Silver tolerates gaps; document in report |
| API changes or is down during the demo | Stored sample of collected data as fallback |
| Feed drops finished trips quickly | Measure retention; shorten interval if needed |
| XML parsing slow or fiddly in Spark | Test on one file as the first Silver task |
| Data too small to show Spark benefits | Be explicit about it; show behaviour as history grows; focus on engineering decisions as the guideline asks |
| GTFS join harder than expected | Use aimed times in SIRI; GTFS is enrichment, not a dependency |
| Free GitHub Actions minutes run out (private repo) | Public repo or longer interval; check current limits |
| One person unavailable | PR reviews and shared ownership keep both able to run everything |
