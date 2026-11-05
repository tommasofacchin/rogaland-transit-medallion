#!/usr/bin/env python3
"""Quick look at raw SIRI ET snapshots before we design Silver.

Plain Python (ElementTree), no Spark, so it runs anywhere in a few seconds.

    python scripts/explore_snapshot.py FILE              # what is in one snapshot
    python scripts/explore_snapshot.py FILE_A FILE_B     # + how calls change from A to B

Use two snapshots from the same day, 5-30 min apart.
"""
import gzip
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

JOURNEY_FIELDS = ["LineRef", "DirectionRef", "DataFrameRef", "DatedVehicleJourneyRef",
                  "VehicleMode", "PublishedLineName", "OperatorRef", "DataSource",
                  "Cancellation", "ExtraJourney", "DestinationName", "IsCompleteStopSequence"]
CALL_FIELDS = ["StopPointRef", "Order", "StopPointName",
               "AimedArrivalTime", "ActualArrivalTime", "ExpectedArrivalTime",
               "AimedDepartureTime", "ActualDepartureTime", "ExpectedDepartureTime",
               "ArrivalStatus", "DepartureStatus", "Cancellation", "PredictionInaccurate"]


def text(el, tag):
    # {*} = any namespace, SIRI puts everything in its own
    found = el.find(".//{*}" + tag)
    return found.text if found is not None else None


def load(path):
    data = Path(path).read_bytes()
    if str(path).endswith(".gz"):
        data = gzip.decompress(data)
    return ET.fromstring(data)


def calls_of(journey):
    for kind, tag in (("recorded", "RecordedCall"), ("estimated", "EstimatedCall")):
        for call in journey.findall(".//{*}" + tag):
            yield kind, call


def pct(n, total):
    return f"{100 * n / total:5.1f}%" if total else "  -  "


def describe(path):
    root = load(path)
    journeys = root.findall(".//{*}EstimatedVehicleJourney")
    print(f"\n=== {Path(path).name}")
    print(f"journeys: {len(journeys)}")

    present = Counter()
    values = {f: Counter() for f in ["VehicleMode", "DataSource", "Cancellation", "ExtraJourney"]}
    for j in journeys:
        for f in JOURNEY_FIELDS:
            # direct children only, otherwise Cancellation of a call would count for the journey
            v = j.find("{*}" + f)
            if v is None and f in ("DataFrameRef", "DatedVehicleJourneyRef"):
                v = j.find("{*}FramedVehicleJourneyRef/{*}" + f)
            if v is not None and v.text:
                present[f] += 1
                if f in values:
                    values[f][v.text] += 1
    print("\njourney fields (share of journeys that have it):")
    for f in JOURNEY_FIELDS:
        print(f"  {f:<24} {pct(present[f], len(journeys))}")
    for f, c in values.items():
        print(f"  values {f}: {dict(c.most_common(8))}")

    kinds = Counter()
    call_present = {"recorded": Counter(), "estimated": Counter()}
    statuses = Counter()
    offsets = Counter()
    orders_ok = 0
    for j in journeys:
        orders = []
        for kind, call in calls_of(j):
            kinds[kind] += 1
            for f in CALL_FIELDS:
                v = call.find("{*}" + f)
                if v is not None and v.text:
                    call_present[kind][f] += 1
                    if f.endswith("Time"):
                        offsets[v.text[19:] or "none"] += 1
            statuses[text(call, "ArrivalStatus")] += 1
            orders.append(int(text(call, "Order") or -1))
        if orders == sorted(orders) and len(orders) == len(set(orders)):
            orders_ok += 1

    print(f"\ncalls: recorded {kinds['recorded']}, estimated {kinds['estimated']}")
    print("call fields (share of calls that have it):")
    print(f"  {'':<24} {'recorded':>8} {'estimated':>9}")
    for f in CALL_FIELDS:
        print(f"  {f:<24} {pct(call_present['recorded'][f], kinds['recorded']):>8} "
              f"{pct(call_present['estimated'][f], kinds['estimated']):>9}")
    print(f"ArrivalStatus values: {dict(statuses.most_common(8))}")
    print(f"time zone offsets in timestamps: {dict(offsets)}")
    print(f"journeys with Order strictly increasing: {pct(orders_ok, len(journeys))}")

    print("\nexamples:")
    for j in journeys[:3]:
        first = next(calls_of(j), (None, None))[1]
        print(f"  journey {text(j, 'DatedVehicleJourneyRef')}  line {text(j, 'LineRef')} "
              f"({text(j, 'PublishedLineName')})  date {text(j, 'DataFrameRef')}  "
              f"first stop {text(first, 'StopPointRef') if first is not None else None} "
              f"{text(first, 'StopPointName') if first is not None else ''}")
    return root


def call_table(root):
    """{(journey, order): (kind, aimed, actual or expected)} using arrival, else departure."""
    out = {}
    for j in root.findall(".//{*}EstimatedVehicleJourney"):
        jid = text(j, "DatedVehicleJourneyRef")
        for kind, call in calls_of(j):
            aimed = text(call, "AimedArrivalTime") or text(call, "AimedDepartureTime")
            best = (text(call, "ActualArrivalTime") or text(call, "ExpectedArrivalTime")
                    or text(call, "ActualDepartureTime") or text(call, "ExpectedDepartureTime"))
            out[(jid, text(call, "Order"))] = (kind, aimed, best)
    return out


def compare(root_a, root_b):
    a, b = call_table(root_a), call_table(root_b)
    both = a.keys() & b.keys()
    changed = sum(1 for k in both if a[k][2] != b[k][2])
    became_recorded = sum(1 for k in both if a[k][0] == "estimated" and b[k][0] == "recorded")
    aimed_changed = sum(1 for k in both if a[k][1] != b[k][1])
    print("\n=== A -> B")
    print(f"calls in A: {len(a)}, in B: {len(b)}, in both: {len(both)} "
          f"(only A: {len(a.keys() - b.keys())}, only B: {len(b.keys() - a.keys())})")
    print(f"of the calls in both: actual/expected time changed {changed}, "
          f"estimated -> recorded {became_recorded}, aimed time changed {aimed_changed}")
    for k in [k for k in both if a[k][2] != b[k][2]][:3]:
        print(f"  {k}: {a[k]}  ->  {b[k]}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    roots = [describe(p) for p in sys.argv[1:3]]
    if len(roots) == 2:
        compare(*roots)
