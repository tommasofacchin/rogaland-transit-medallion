# Data dictionary: Silver and Gold

Source: Entur SIRI ET (`/realtime/v1/rest/et?datasetId=KOL`), one snapshot every 5 min.
One Bronze file is one snapshot. In it, one `EstimatedVehicleJourney` is one trip, and every
`RecordedCall` (already passed) or `EstimatedCall` (still to come) is one stop of that trip.

Grain: **one stop of one trip on one service date.**
Key: `journey_id + service_date + stop_order`.

Categories (plan 4.1):
- **essential**: the row is rejected without it
- **optional**: kept when present, null otherwise
- **derived**: computed by us
- **unreliable**: present, but we don't fully trust it

The "% present" column is measured on all of Silver after the rebuild on 11 Nov
(27 Oct - 10 Nov, 1,974,429 rows: R = 1,960,059 recorded, E = 14,370 estimated).
"Present" means not null, so for `cancelled` it does not say how many trips were cancelled.

## Trip columns

| Column | SIRI field | Type | Category | % present (R / E) | Notes |
|---|---|---|---|---|---|
| journey_id | FramedVehicleJourneyRef/DatedVehicleJourneyRef | string | essential | 100 / 100 | e.g. `KOL:ServiceJourney:6_1001` |
| service_date | FramedVehicleJourneyRef/DataFrameRef | date | essential | 100 / 100 | operating day, so a trip at 00:30 can belong to the day before |
| line | LineRef | string | essential | 100 / 100 | e.g. `KOL:Line:2_6` |
| direction | DirectionRef | string | optional | 100 / 100 | |
| operator | OperatorRef | string | optional | 100 / 100 | |
| mode | VehicleMode | string | optional | 99.9 / 97.4 | lower-cased by the rules (`bus`, `water`, ...) |
| cancelled | Cancellation | boolean | unreliable | 0.6 / 14.5 | mostly only sent when it is set, so null means "not said" |

## Stop columns

| Column | SIRI field | Type | Category | % present (R / E) | Notes |
|---|---|---|---|---|---|
| stop_order | Order | int | essential | 100 / 100 | position in the trip, starts at 1 |
| stop_id | StopPointRef | string | essential | 100 / 100 | NSR quay id, e.g. `NSR:Quay:4111` |
| stop_name | StopPointName | string | optional | 99.4 / 97.8 | æ ø å; flagged if it looks broken |
| aimed_arrival | AimedArrivalTime | timestamp UTC | essential* | 96.4 / 97.4 | missing on the first stop |
| aimed_departure | AimedDepartureTime | timestamp UTC | essential* | 96.5 / 79.1 | missing on the last stop |
| actual_or_expected_arrival | ActualArrivalTime (R) / ExpectedArrivalTime (E) | timestamp UTC | optional | 96.4 / 97.4 | |
| actual_or_expected_departure | ActualDepartureTime (R) / ExpectedDepartureTime (E) | timestamp UTC | optional | 96.5 / 79.1 | |
| call_status | RecordedCall or EstimatedCall | string | derived | 100 / 100 | `recorded` / `estimated` |

\* at least one of the two aimed times must be there, otherwise the row is rejected.

Expected times are predictions, so they are unreliable by nature. A stop that is still
`estimated` in the last snapshot never got a real time, and its delay is only a forecast.

## Derived and lineage columns

| Column | How | Type | Notes |
|---|---|---|---|
| arrival_delay_s | actual_or_expected_arrival - aimed_arrival | int | null if one of the times is missing |
| departure_delay_s | actual_or_expected_departure - aimed_departure | int | |
| snapshot_time | ResponseTimestamp of the snapshot the kept row comes from | timestamp UTC | |
| run_id | taken from the Bronze file name | string | links the row back to a Bronze file |

## Flags (added by `silver/rules.py`)

| Flag | True when |
|---|---|
| is_missing_actual | no actual or expected time at all |
| is_delay_outlier | a delay is below -10 min or above 3 h (checked on 11 days of prod data) |
| is_bad_stop_name | the stop name contains "Ã", which means a broken æ/ø/å |
| exclude_from_punctuality | cancelled, outlier or missing actual time; Gold skips these rows |

## Rejected rows (`silver/rejected_calls`)

Same columns plus `reason_code`: `MISSING_KEY`, `MISSING_LINE_OR_STOP` or `MISSING_AIMED_TIME`.
Rows whose times can't be parsed never get this far; they are only counted as
`rows_unparseable` in the quality table (the raw data is still in Bronze).

## Not in Silver yet

`ExtraJourney`, cancellation per stop and the original UTC offset are in the feed but not
stored. `PublishedLineName` is empty in the Kolumbus feed, so line numbers come from
`reference/line_names` instead.

## Things we noticed in the raw data

- Finished trips leave the feed 4-11 min after their last stop (retention test, 26 Oct).
  That is why we poll every 5 min.
- Every snapshot repeats all running trips, so one stop shows up about 22 times
  (2 Nov: 3,401,155 stop rows in the snapshots, 153,568 after dedup).
- 99.3% of the stops in Silver end as `recorded`. The other 0.7% (14,370) are still
  `estimated` in the last snapshot that had the trip, mostly just after midnight
  (10 Nov: up to 4.7% of the stops at 00h, 0.4% at 15h). Their delay is a forecast.
- Recorded stops practically always have an actual time when they have the aimed time (96.4% / 96.5%
  for both). The missing 3.5% are the first stop (no arrival) and the last stop (no departure).
- Estimated rows miss the departure much more often (79.1%): many of them are the last
  stops of a trip, which only have an arrival.
- `PublishedLineName` is empty for Kolumbus, and Journey Planner usually gives the same
  text as the public number, so a line is shown as "X39", not with a route name.
- One line id can have several route variants. X39 has three: Stavanger - Ålgård
  (57 stops, 44 trips), Gjesdal - Jernbaneveien (58 stops, 33 trips, 05-07) and a short
  Gjesdal - Jernbaneveien (34 stops, 22 trips, 14-15). So `stop_order` only means the same
  stop inside one variant.
- Spark's own XML reader dropped the +01:00 offset of the times, so we parse with
  ElementTree and convert to UTC ourselves.

# Gold (`gold/`)

Built by `gold/build.py` from `silver/stop_calls`, partitioned by `service_date`.
Both tables have the same measures:

| Column | Meaning |
|---|---|
| calls | all stops in the group, also cancelled and excluded ones |
| cancelled_calls | stops of cancelled trips |
| measured_calls | stops used for punctuality (`exclude_from_punctuality` is false) |
| on_time_calls | measured stops with delay from -1 min to +3 min |
| late_calls | measured stops more than 3 min late |
| early_calls | measured stops more than 1 min early |
| avg_delay_s | average delay of measured stops, seconds |
| p90_delay_s | 90% of measured stops are less late than this, seconds |
| on_time_share | on_time_calls / measured_calls (null if nothing was measured) |
| cancelled_share | cancelled_calls / calls |

Delay = departure delay, or arrival delay at the last stop (it has no departure).

## punctuality_by_line_hour
One row per `service_date`, `line`, `hour`. `hour` is the hour of the planned departure in
Norway time (Europe/Oslo), not UTC. 
`public_code` and `line_name` come from `reference/line_names` (null if the line was not found there).

## punctuality_by_stop
One row per `service_date`, `stop_id`. `stop_name` is any name seen for the stop, because some
lines send no names.

## _status/status.jsonl
One JSON line per Gold run: run_id, run_time, dates, status (`ok`/`failed`), problems and row
counts. Gold is only written when status is `ok`.

# Reference (`reference/`)

## line_names
Built by `reference/lines.py` (step `lines`) from Entur's Journey Planner API, for every line id
in Silver. Only line ids not yet in the table are looked up, so most nights there is no API
call. Ids that Journey Planner does not know are saved with empty names, so we don't ask again.

| Column | Meaning |
|---|---|
| line | line id, same as in Silver, e.g. `KOL:Line:8_2043` |
| public_code | the number on the bus, e.g. `X39` |
| line_name | name from Journey Planner (often the same as public_code) |
| transport_mode | `bus`, `water`, ... |
| fetched_at | when we asked the API (UTC) |