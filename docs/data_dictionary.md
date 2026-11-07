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

The "% present" column is filled in from `scripts/explore_snapshot.py` on real snapshots
(R = recorded calls, E = estimated calls).

## Trip columns

| Column | SIRI field | Type | Category | % present | Notes |
|---|---|---|---|---|---|
| journey_id | FramedVehicleJourneyRef/DatedVehicleJourneyRef | string | essential | | e.g. `KOL:ServiceJourney:6_1001` |
| service_date | FramedVehicleJourneyRef/DataFrameRef | date | essential | | operating day, so a trip at 00:30 can belong to the day before |
| line | LineRef | string | essential | | e.g. `KOL:Line:2_6` |
| direction | DirectionRef | string | optional | | |
| operator | OperatorRef | string | optional | | |
| mode | VehicleMode | string | optional | | lower-cased by the rules (`bus`, `water`, ...) |
| cancelled | Cancellation | boolean | unreliable | | we don't know yet how fast Kolumbus sets it |

## Stop columns

| Column | SIRI field | Type | Category | % present (R / E) | Notes |
|---|---|---|---|---|---|
| stop_order | Order | int | essential | | position in the trip, starts at 1 |
| stop_id | StopPointRef | string | essential | | NSR quay id, e.g. `NSR:Quay:4111` |
| stop_name | StopPointName | string | optional | | æ ø å; flagged if it looks broken |
| aimed_arrival | AimedArrivalTime | timestamp UTC | essential* | | missing on the first stop |
| aimed_departure | AimedDepartureTime | timestamp UTC | essential* | | missing on the last stop |
| actual_or_expected_arrival | ActualArrivalTime (R) / ExpectedArrivalTime (E) | timestamp UTC | optional | | |
| actual_or_expected_departure | ActualDepartureTime (R) / ExpectedDepartureTime (E) | timestamp UTC | optional | | |
| call_status | RecordedCall or EstimatedCall | string | derived | | `recorded` / `estimated` |

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

`PublishedLineName`, `ExtraJourney`, cancellation per stop and the original UTC offset are in
the feed but not stored. Add them if Gold needs them.

## Things we noticed in the raw data

(fill in from explore_snapshot.py: how often recorded stops miss an actual time, whether
`Order` always starts at 1, whether aimed times change between snapshots, ...)

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

## punctuality_by_stop
One row per `service_date`, `stop_id`. `stop_name` is any name seen for the stop, because some
lines send no names.

## _status/status.jsonl
One JSON line per Gold run: run_id, run_time, dates, status (`ok`/`failed`), problems and row
counts. Gold is only written when status is `ok`.
