# Decisions

Short notes on the choices we made, newest at the bottom.
(O) = Oliwier, (T) = Tommaso, (O+T) = both.

## 2026-10-24 Medallion pipeline on Entur real-time data (O+T)
We use the Entur SIRI ET feed for Kolumbus (Rogaland) and build Bronze, Silver and Gold
with PySpark. The question is: which lines, stops and hours are late most often?
Entur only shows "now" and has no history, so we have to collect the data ourselves.
That is why Bronze had to start first.

## 2026-10-25 Two OpenStack VMs: dev and prod (O)
We made two VMs, `dat535-dev` for testing and `dat535-prod` for the real collection.
We only have one floating IP, so prod has it and we reach dev through prod (SSH ProxyJump).
Prod has an extra volume mounted on `/data`, so the data is not on the system disk.

## 2026-10-25 Self-hosted GitHub runners on the VMs (T)
Each VM has a GitHub Actions runner (labels `dev` and `prod`), so workflows can run on our
own machines.

## 2026-10-26 Tests run on GitHub's machines, not on our VMs (O)
`tests.yml` runs ruff and pytest on `ubuntu-latest` for every push and pull request.
This way code from a pull request never runs on our VMs.

## 2026-10-26 Use datasetId=KOL (O)
A test request with `datasetId=KOL` gave 679 trips, all from Kolumbus. One response is
about 9 MB. (`datasource=KOL` did not filter anything.)

## 2026-10-26 Bronze keeps the raw response and one metadata line per run (O)
Every response is saved exactly as we got it. Each run, also a failed one, adds one JSON
line to `_metadata/` (time, status, size, sha256, error). Broken XML is still saved, but
marked `parse_error`. Files are never overwritten. This way nothing is lost and every
Silver row can be traced back to a file.

## 2026-10-26 Poll every 5 minutes (O)
We tested on dev how long a finished trip stays in the feed (32 snapshots, 5 min apart).
Trips disappeared 4 to 11 minutes after their last stop. With 15 minutes we would miss
the real arrival time of most trips, so we poll every 5 minutes.

## 2026-10-26 Save Bronze as gzip (O)
At 5 minutes we get 288 files a day, about 2.6 GB a day. Gzip makes them about 15 times
smaller (around 85 MB a day), and Spark can read `.xml.gz` directly. Gzip does not change
the data, so Bronze is still raw.

## 2026-10-26 New 200 GB volume on prod (O)
Resizing the first 50 GB volume failed and it got stuck in OpenStack ("Detaching").
We made a new 200 GB volume `dat535-data2` and mounted it on `/data`. There is a gap in
the data on 26 Oct from 18:30 to 19:15 UTC, but the real collection started on 27 Oct,
so it does not matter.

## 2026-10-26 Collect with cron on prod (O)
Cron runs `scripts/run_bronze_cron.sh prod` every 5 minutes. `flock` makes sure two runs
never overlap.

## 2026-10-27 No schedule in GitHub Actions for Bronze (T)
We first added a 5 minute schedule to `ingest.yml`, but together with cron every snapshot
was fetched twice, and GitHub's scheduled runs are often late. So the schedule was removed.
Cron does the collection, and `ingest.yml` can only be started by hand.

## 2026-10-27 Deploy with GitHub Actions (T)
When something is merged to `main`, `deploy.yml` updates the code on prod (`git pull` +
`pip install`). When something is merged to `dev`, it updates the dev VM.

## 2026-10-27 Read the XML with Python, not with Spark's XML reader (T)
Spark's XML reader lost the time zone offset (+01:00) in the times, and recorded and
estimated stops came out with different structures. So `flatten.py` reads each file with
Python (ElementTree) and Spark runs it on many files at the same time.

## 2026-10-27 Silver: one row per stop, times in UTC (T)
One row = one stop of one trip on one service date. The key is
`journey_id + service_date + stop_order`. All times are converted to UTC. `service_date`
comes from the feed (`DataFrameRef`), so a trip after midnight still belongs to the right
day. Delay = actual (or expected) time minus planned time, in seconds.

## 2026-11-02 Dedup: keep the best version of each stop (T)
The same stop shows up in every snapshot until the trip ends. We keep one row: a recorded
(real) time beats an estimated one, and otherwise the newest snapshot wins. Silver is saved
as Parquet, split by `service_date`.

## 2026-11-04 Cleaning rules: reject only what we can't use (O)
A row is rejected only if it has no key, no line or stop, or no planned time at all,
because then we can't compute a delay. Rejected rows go to `silver/rejected_calls` with a
reason code, so we can count them. Other problems only get a flag and the row stays:
no actual time, delay outlier (below -10 min or above 3 h, a first guess), broken stop
name, cancelled trip. Gold leaves out rows with `exclude_from_punctuality`, but cancelled
trips are still used for the cancellation rate.
The rules run before dedup, so a bad row can't win over a good one.

## 2026-11-04 Same result on every rerun (O)
- If the same snapshot was fetched twice (cron and Actions on 27 Oct), dedup picked one
  row at random. Now `run_id` is the last sort column, so reruns give the same result.
- `--from/--to` now mean service dates. Silver reads one extra Bronze day on each side and
  only writes dates inside the range. Before, rerunning one day could replace the day
  before with only its night trips.

## 2026-11-04 Quality table for every Silver run (O)
Every run saves its numbers to `silver/quality`: rows read, rejected per reason, empty
values, duplicates before and after dedup, and a schema check. Flags are counted after
dedup, so a stop seen in 10 snapshots counts once.

First run on dev (the 33 snapshots from the retention test):
446,363 rows became 33,077 stops after dedup, 0 rows rejected, 0 duplicates left.
The 1,149 empty planned arrivals and departures are the first and last stops of the trips,
so that is normal.
