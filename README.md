# Rogaland Transit Medallion

How punctual are the buses in Rogaland? Entur only shows what is happening *right now*, so we collect the live feed ourselves and build up a history we can actually analyse.

This is our project for DAT535 (Data Engineering) at UiS, by **Tommaso Facchin** and **Oliwier Trawinski** (group 5).

## The idea

Entur publishes real-time departures as SIRI XML. Every snapshot is a messy pile of nested trips, and the same trip shows up again and again with slightly updated times. There's no history, so you can't ask "which lines are usually late, and when?".

So we:

1. Poll the Entur API every 5 minutes and keep every raw response.
2. Clean it up into one row per stop of each trip.
3. Aggregate it into punctuality numbers per line, hour and stop.

We use the medallion setup for this:

| Layer | What it holds |
|---|---|
| **Bronze** | Raw XML responses (gzipped), untouched, plus one metadata line per run |
| **Silver** | Parsed, validated and deduplicated stop calls, plus rejected rows and a quality table. This is where most of the work is |
| **Gold** | Two small tables with punctuality and delay per line/hour and per stop |

Processing is done with PySpark. GitHub Actions runs the tests, deploys the code and runs Silver and Gold every night.

## Repo layout

```
config/          dev.yaml and prod.yaml (where the data goes, Entur settings)
jobs/            run_pipeline.py (one entry point for every step), bronze_summary.py
src/entur_pipeline/
  bronze/        fetch one snapshot and write it
  silver/        schema, flatten, standardize, rules, dedup, quality, build
  reference/     line numbers from Entur Journey Planner
  gold/          the two Gold tables
scripts/         cron wrapper, reports, charts, small checks
tests/           pytest, runs on a local Spark
docs/            plan, decisions, data dictionary
```

## How it runs

We have two OpenStack VMs, `dat535-dev` and `dat535-prod`. Both have a self-hosted GitHub Actions runner (labels `dev` and `prod`), the code in `~/app` and a virtualenv in `~/venv`. Prod has a 200 GB volume mounted on `/data`.

- **Bronze**: cron on prod runs [scripts/run_bronze_cron.sh](scripts/run_bronze_cron.sh) every 5 minutes. We don't use a GitHub schedule here because those runs are often late by a lot.
- **Silver, lines, Gold**: [pipeline.yml](.github/workflows/pipeline.yml) runs on the prod runner every night at 03:17 UTC and processes yesterday. In practice GitHub often starts it hours later, which is fine for a daily batch. At the end it writes a short summary on the run page and goes red if Gold failed or if Bronze is missing too many snapshots.
- **Deploy**: a push to `main` updates prod, a push to `dev` updates the dev VM ([deploy.yml](.github/workflows/deploy.yml)).
- **Tests**: ruff and pytest on GitHub's own machines for every push and PR ([tests.yml](.github/workflows/tests.yml)), so PR code never runs on our VMs.

The data ends up under `data_root` from the config (`/data/entur` on prod, `/data/entur-dev` on dev):

```
bronze/entur_siri/dt=YYYY-MM-DD/hour=HH/*.xml.gz
bronze/entur_siri/_metadata/dt=YYYY-MM-DD.jsonl
silver/stop_calls/          partitioned by service_date
silver/rejected_calls/
silver/quality/
reference/line_names/
gold/punctuality_by_line_hour/
gold/punctuality_by_stop/
gold/_status/status.jsonl
```

All times in Bronze and Silver are UTC. Gold uses Norway time for the hour.

## Running it yourself

You need Python 3.10+ and Java 17+ (for PySpark). Linux is easiest; the cron script uses `flock`.

```bash
git clone https://github.com/tommasofacchin/rogaland-transit-medallion.git
cd rogaland-transit-medallion
python -m venv ~/venv
~/venv/bin/pip install -e ".[spark,dev]"
```

### Config

Copy `config/dev.yaml` and change `data_root` to a folder you can write to. If you're not us, also change `client_name`: Entur wants every client to send its own `ET-Client-Name`.

```yaml
env: dev
interval_min: 5
compress: true
data_root: /home/you/entur-data
entur:
  et_url: https://api.entur.io/realtime/v1/rest/et
  dataset_id: KOL
  client_name: yourname-test
  timeout_s: 60
```

### Collect Bronze

One snapshot:

```bash
~/venv/bin/python jobs/run_pipeline.py bronze --config config/dev.yaml
```

To collect for real, add this to `crontab -e` (the script reads `config/<env>.yaml` and logs to `~/logs/`):

```
*/5 * * * * $HOME/app/scripts/run_bronze_cron.sh dev
```

The script expects the repo in `~/app` and the venv in `~/venv`; set `APP_DIR` if your repo is somewhere else. Let it run for at least a full day before going on, because Silver reads one extra day on each side so trips that cross midnight are complete.

### Silver, lines, Gold

The same three steps the nightly workflow runs, for one day or a range:

```bash
PY=~/venv/bin/python
$PY jobs/run_pipeline.py silver --config config/dev.yaml --from 2026-11-10 --to 2026-11-10
$PY jobs/run_pipeline.py lines  --config config/dev.yaml
$PY jobs/run_pipeline.py gold   --config config/dev.yaml --from 2026-11-10 --to 2026-11-10
```

Rerunning a day overwrites only that day's partitions, so it's safe to run again. One Silver day takes about 5 minutes on our VMs (8 GB RAM; Spark gets 4 GB). Gold takes under a minute.

`lines` asks Entur's Journey Planner for the public line number (e.g. `X39`) of any line it hasn't seen before. If it fails, Gold still works, the lines just have no name.

### Looking at the results

```bash
$PY scripts/gold_report.py   --config config/dev.yaml                    # main answers, as Spark SQL
$PY scripts/show_quality.py  --config config/dev.yaml --day 2026-11-10   # Silver quality numbers
$PY scripts/run_summary.py   --config config/dev.yaml --day 2026-11-10   # same table as the run page
$PY jobs/bronze_summary.py   --config config/dev.yaml                    # runs per day and gaps
$PY scripts/make_charts.py   --config config/dev.yaml --out charts/      # charts for the report
```

`make_charts.py` needs matplotlib (in the `dev` extras). For the stop map it also uses `contextily` and `folium` if they are installed.

### Tests

```bash
ruff check src tests
pytest -q
```

The tests start a local Spark, so Java is needed here too.

## Running it on our VMs

If you have access to the repo and the VMs, you don't need most of the above:

- Merge to `dev` to deploy on the dev VM, merge to `main` to deploy on prod.
- To run the pipeline by hand, go to Actions → *Daily pipeline* → *Run workflow*, pick a day (empty = yesterday) and a target (`prod` or `dev`).
- *Bronze ingest* can also be started by hand, but only for testing: cron already collects every 5 minutes.

## Data

Entur real-time API (SIRI ET), dataset `KOL`. No key needed, but you have to send an `ET-Client-Name` header, and you shouldn't poll much more than a few times a minute. The data is open under the Norwegian Licence for Open Government Data (NLOD).

The API has no history, so anything we didn't collect is gone. Our archive starts on 27 October 2026.