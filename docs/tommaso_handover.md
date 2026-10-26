Hi Tommaso, here's where the project stands and what you could pick up next.

**Done (26 Oct)**
- PR `feat/bronze-ingest` → `dev` is up: Bronze ingest (`src/entur_pipeline/bronze/ingest.py`), runner `jobs/run_pipeline.py bronze`, Spark summary `jobs/bronze_summary.py`, 8 tests, ruff clean. Background and numbers are in `docs/decisions.md`.
- What Bronze does: it fetches the Entur SIRI ET feed for Kolumbus (`datasetId=KOL`) and stores the raw XML unchanged, gzip-compressed, under `/data/entur/bronze/entur_siri/dt=YYYY-MM-DD/hour=HH/`. Each run, including failed ones, adds one JSON line to `_metadata/dt=....jsonl` with Lab 2-style `_bronze_*` fields.
- We poll every **5 min**. A retention test showed that finished trips vanish from the feed 4–11 min after their last stop, so 15 min would lose most final arrival times.
- Gzip is about 15× (9 MB → 0.6 MB per snapshot). Prod `/data` is now a new **200 GB volume** (`dat535-data2`), because the old 50 GB one got stuck in OpenStack. There was a collection gap on 26 Oct from 18:30 to 19:15 UTC.
- **Prod is collecting right now** via cron: `*/5 * * * * ~/app/scripts/run_bronze_cron.sh prod`. `~/app` is a clone of `feat/bronze-ingest`, and the log is in `~/logs/bronze-prod.log`. Check it with `cd ~/app && ~/venv/bin/python jobs/bronze_summary.py --config config/prod.yaml`.

**What you could work on**
1. **Review and merge the PR** into `dev`, then into `main`.
2. **Repo settings** (you have access): branch protection on `main`/`dev`, and environments `dev`/`prod` with a required reviewer on prod.
3. **`ingest.yml`**: schedule `"*/5 * * * *"` (5 fields; the draft had 6), `runs-on: [self-hosted, prod]`, command `~/venv/bin/python jobs/run_pipeline.py bronze --config config/prod.yaml`, plus `concurrency: bronze-ingest` so runs never overlap. GitHub's scheduled runs can be a few minutes late, so watch the "gaps" table in `bronze_summary.py`. **When it runs, tell me and I remove the cron line**, otherwise every snapshot is fetched twice.
4. **`deploy.yml`**: on push to `main`, update the code on prod (`git pull` + `pip install -e .` in the app folder). On `dev`, do the same on the dev VM.
5. **Silver** (next real code, per plan.md): read the `.xml.gz` with Spark's XML reader, turn it into one row per (journey, stop), deduplicate across snapshots keeping the latest one, and compute delay = actual − aimed. Then `pipeline.yml` runs it daily. I can take Gold/analysis once Silver has a schema.

**Safety rules I suggest we keep (tell me if you disagree)**
- Self-hosted runners never run `pull_request` workflows (`ci.yml` stays on `ubuntu-latest`).
- Never commit `settings/` (keys/tokens) or `probe_*.xml`.
- Prod data lives only in `/data/entur` and Bronze files are never overwritten or edited.
