"""Retention test (guide step 5): how long does a finished trip stay in the Entur feed?

For every journey that disappeared during the test, compare the end of the trip (the latest
arrival/departure time of its calls in the last snapshot that still contained it) with:
  - last_seen: time of that last snapshot            -> it stayed at least this long after the end
  - gone_by:   time of the next snapshot (without it) -> it was gone at most this long after the end

Usage: python scripts/retention_check.py /data/entur-dev/bronze/entur_siri
"""
import gzip
import statistics
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

TIME_TAGS = ["ActualArrivalTime", "ExpectedArrivalTime", "AimedArrivalTime",
             "ActualDepartureTime", "ExpectedDepartureTime", "AimedDepartureTime"]


def snapshot_time(path):
    # entur_siri_20261026T163502Z_local-1.xml -> 2026-10-26 16:35:02 UTC
    stamp = path.name.split("_")[2]
    return datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def journey_end_times(path):
    """{journey_id: latest call time} for one snapshot. '{*}' in findall ignores the SIRI namespace."""
    ends = {}
    data = path.read_bytes()
    if path.suffix == ".gz":
        data = gzip.decompress(data)
    for journey in ET.fromstring(data).findall(".//{*}EstimatedVehicleJourney"):
        ref = journey.find(".//{*}DatedVehicleJourneyRef")
        if ref is None or not ref.text:
            continue
        times = [datetime.fromisoformat(el.text)
                 for tag in TIME_TAGS for el in journey.findall(".//{*}" + tag) if el.text]
        if times:
            ends[ref.text] = max(times)
    return ends


def minutes(delta):
    return delta.total_seconds() / 60


def main(bronze_dir):
    # Skip leftover '_tmp_' files from an interrupted write.
    files = sorted(p for p in Path(bronze_dir).glob("dt=*/hour=*/entur_siri_*")
                   if p.name.endswith((".xml", ".xml.gz")))
    if len(files) < 3:
        sys.exit(f"Need at least 3 snapshots, found {len(files)}")
    print(f"Reading {len(files)} snapshots ({snapshot_time(files[0]):%H:%M} to "
          f"{snapshot_time(files[-1]):%H:%M} UTC)...")

    last_seen = {}  # journey -> (index of last file containing it, end time in that file)
    for i, path in enumerate(files):
        for journey, end in journey_end_times(path).items():
            last_seen[journey] = (i, end)

    disappeared = []  # (stayed, gone_by) in minutes after the trip end
    for i, end in last_seen.values():
        if i == len(files) - 1:
            continue  # still in the newest snapshot: not gone yet
        disappeared.append((minutes(snapshot_time(files[i]) - end),
                            minutes(snapshot_time(files[i + 1]) - end)))

    if not disappeared:
        sys.exit("No journey has disappeared yet; let the test run longer.")

    still_running = sum(1 for stayed, _ in disappeared if stayed < 0)
    print(f"Journeys seen: {len(last_seen)}, disappeared: {len(disappeared)}")
    print(f"  of these, {still_running} vanished BEFORE their last planned stop "
          "(cancelled or dropped from the feed)")
    # Only trips that had ended when last seen; gone_by is taken from the same trips.
    finished_pairs = [(s, g) for s, g in disappeared if s >= 0]
    finished = sorted(s for s, _ in finished_pairs)
    if finished:
        print("Finished trips, minutes their last snapshot came after the trip end:")
        print(f"  min {finished[0]:.0f}, median {statistics.median(finished):.0f}, "
              f"max {finished[-1]:.0f}")
        print(f"  gone at most {statistics.median(g for _, g in finished_pairs):.0f} "
              "min after the end (median)")
        for interval in (5, 15):
            share = sum(1 for m in finished if m >= interval) / len(finished)
            print(f"  stayed >= {interval:2d} min after the end: {share:.0%}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/data/entur-dev/bronze/entur_siri")
