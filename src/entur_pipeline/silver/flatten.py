"""Silver layer, step 1: flatten one Bronze snapshot into one dict per stop call.

Reads the raw SIRI ET XML (plain or .gz) with the standard library, no Spark. Values stay as
the strings found in the XML (times, dates); `standardize.py` converts them. Column names are
the ones in `schema.py`.
"""
import gzip
import xml.etree.ElementTree as ET

NS = "{http://www.siri.org.uk/siri}"

# (call_status, container tag, call tag)
CALL_KINDS = [
    ("recorded", "RecordedCalls", "RecordedCall"),
    ("estimated", "EstimatedCalls", "EstimatedCall"),
]


def _text(element, tag):
    """Text of the direct child `tag`, or None if missing."""
    if element is None:
        return None
    child = element.find(NS + tag)
    return child.text if child is not None else None


def _open(path):
    return gzip.open(path, "rb") if str(path).endswith(".gz") else open(path, "rb")


def flatten_snapshot(path, run_id=None):
    """Return one dict per stop call (recorded and estimated) found in the snapshot."""
    with _open(path) as f:
        root = ET.parse(f).getroot()
    snapshot_time = root.findtext(f"{NS}ServiceDelivery/{NS}ResponseTimestamp")

    rows = []
    for journey in root.iter(NS + "EstimatedVehicleJourney"):
        framed = journey.find(NS + "FramedVehicleJourneyRef")
        cancelled = _text(journey, "Cancellation")
        base = {
            "journey_id": _text(framed, "DatedVehicleJourneyRef"),
            "service_date": _text(framed, "DataFrameRef"),
            "line": _text(journey, "LineRef"),
            "direction": _text(journey, "DirectionRef"),
            "operator": _text(journey, "OperatorRef"),
            "mode": _text(journey, "VehicleMode"),
            "cancelled": None if cancelled is None else cancelled == "true",
            "snapshot_time": snapshot_time,
            "run_id": run_id,
        }
        for status, container, tag in CALL_KINDS:
            for call in journey.iterfind(f"{NS}{container}/{NS}{tag}"):
                order = _text(call, "Order")
                rows.append({
                    **base,
                    "stop_order": None if order is None else int(order),
                    "stop_id": _text(call, "StopPointRef"),
                    "stop_name": _text(call, "StopPointName"),
                    "aimed_arrival": _text(call, "AimedArrivalTime"),
                    "aimed_departure": _text(call, "AimedDepartureTime"),
                    "actual_or_expected_arrival": (
                        _text(call, "ActualArrivalTime") or _text(call, "ExpectedArrivalTime")
                    ),
                    "actual_or_expected_departure": (
                        _text(call, "ActualDepartureTime") or _text(call, "ExpectedDepartureTime")
                    ),
                    "call_status": status,
                })
    return rows
