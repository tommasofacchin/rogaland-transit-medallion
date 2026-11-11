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
so that is normal. 30 stops were delay outliers and 92 were left out of punctuality (the outliers plus
stops on cancelled trips).

## 2026-11-07 Bronze clean-up (O)
Removed two metadata fields nobody used (`_bronze_params`, `duration_s`) and made the
config check and comments simpler. `bronze_summary.py` now compares each day with 288 runs
instead of working out the exact collection window. The first and last day show missing
runs because they are not full days; the gaps table shows the real gaps.
All folder and file times in Bronze are UTC (Norway is UTC+1 now), so 22:45 in Norway
is in `hour=21`. Gold has to convert to Norway time before it groups by hour.

## 2026-11-07 First Silver run on prod (O)
The first run crashed with OutOfMemory: Spark uses 1 GB by default, and the files were split
into only 4 big tasks. With 4 GB and one task per file, one day takes about 5.5 minutes.
We backfilled 27 Oct - 6 Nov by hand: about 154k stops on weekdays, 87k on Saturday and 55k
on Sunday (weekend timetable).

## 2026-11-07 Delay limits stay at -10 min and 3 h (O)
99.8% of stops are between -5 and 60 min (median +2.2 min). Below -10 min there were 702
stops, and the worst were times from the day before. Above 3 h there were only 17 stops,
from 2 trips shifted by 4-11 hours on every stop, so these are feed errors and not real delays.

## 2026-11-07 Gold: two tables with counts, hour in Norway time (O)
`punctuality_by_line_hour` and `punctuality_by_stop`, one row per service date and group.
On time = from 1 min early to 3 min late. We store the counts (calls, on time, late, early,
cancelled) and not only the shares, so the demo can add up any period and still get the right
share. The hour is converted from UTC to Europe/Oslo before grouping, otherwise "8 o'clock"
would be 9 in Norway. Stops are grouped by `stop_id`, because some lines send no stop names.

## 2026-11-07 Gold is only written when Silver looks complete (O)
Before writing, Gold checks that every day has at least 20,000 Silver rows (a Sunday has about
55k) and that the calls in each Gold table add up to the Silver rows. If not, nothing is written,
the old Gold stays, the step exits with an error and the run is logged as `failed` in
`gold/_status/status.jsonl`. That file also tells a consumer when Gold was last updated.

## 2026-11-07 Schedule: cron for Bronze, GitHub Actions for Silver and Gold (O)
Bronze needs exact 5 minute timing, so it stays on cron. Silver and Gold run once a day, where a
late start does not matter, so they can run in GitHub Actions on the prod runner. That gives us
run history, logs and a red/green status. The days before the schedule were run by hand once.

## 2026-11-07 Line numbers from Entur Journey Planner (O)
Kolumbus sends only ids like `KOL:Line:8_2043` in the SIRI feed; `PublishedLineName` is empty.
Nobody can read that in a chart. So the step `lines` asks Entur's Journey Planner API (GraphQL)
for every line id in Silver and saves the public number (e.g. `X39`) in `reference/line_names`.
Gold joins it with a left join, so a line without a name still counts. The table is small
(a few hundred rows), so Spark sends it to every task (broadcast join) instead of shuffling.

## 2026-11-11 Quality rows know their day (O)
The quality table only had `run_id` and `run_time`, and all hand runs were called `local`,
so we could not say which day a row was for. Now every row has `date_from` and `date_to`
(the service dates of the run). Old rows have no dates; read the table with `mergeSchema`
(`scripts/show_quality.py` does this).
A Bronze file that cannot be read (bad number, broken gzip) is now skipped and counted
instead of stopping the whole Silver run.

## 2026-11-11 Predicted stops stay in Gold (O)
A stop that never became a RecordedCall keeps its predicted (Expected) time and is counted
as measured. On 10 Nov that was 1,014 of 153,724 measured stops (0.66%, at most 4.7% just
after midnight). Leaving them out changes the on-time share by about 0.05 points, so Gold
keeps them. Silver now counts them every run as `measured_estimated_only`.

## 2026-11-11 Line names are only fetched for new lines (O)
At first the `lines` step asked Journey Planner about every line each night and overwrote
the table. That needs an outside API every night for names that hardly ever change, and a
line that stops running would lose its name in old Gold rows. Now the step keeps the old rows
and only asks about line ids that are new in Silver. Most nights it makes no API call.

## 2026-11-11 Run summary, Bronze check and dev runs in the daily pipeline (O)
The nightly run was green or red, but the numbers were only in files on the VM. Now the
last step writes a summary to the run page: Bronze runs that day, Silver rows and the Gold
status. With fewer than 270 of 288 valid Bronze snapshots the run turns red on prod, because
Gold's 20k row check would let a half day through. The `lines` step only gives a warning when
Entur's API fails, since Gold can use the old names. The schedule moved from 04:00 to 03:17
UTC: the first scheduled runs started about 7.5 hours late, and GitHub delays runs on the
full hour the most. Started by hand, the workflow can also run on the dev VM (`target: dev`)
with `config/dev.yaml` and the dev test data, so a change can be tried on dev before prod.
`test-runner.yml` was only a first runner test and is removed.