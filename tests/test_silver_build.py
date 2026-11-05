import shutil
from datetime import date
from pathlib import Path

from entur_pipeline.silver.build import list_bronze_files, read_rows, run_id_from_path

FIXTURE = Path(__file__).parent / "fixtures" / "siri_sample.xml"

# Two calls of one journey; the first has no <Order>, so its key is incomplete
NO_ORDER_XML = """<Siri xmlns="http://www.siri.org.uk/siri"><ServiceDelivery>
<ResponseTimestamp>2026-10-27T14:00:02+01:00</ResponseTimestamp>
<EstimatedTimetableDelivery><EstimatedJourneyVersionFrame><EstimatedVehicleJourney>
<LineRef>L1</LineRef>
<FramedVehicleJourneyRef><DataFrameRef>2026-10-27</DataFrameRef>
<DatedVehicleJourneyRef>J1</DatedVehicleJourneyRef></FramedVehicleJourneyRef>
<EstimatedCalls>
<EstimatedCall><StopPointRef>S1</StopPointRef></EstimatedCall>
<EstimatedCall><StopPointRef>S2</StopPointRef><Order>2</Order></EstimatedCall>
</EstimatedCalls>
</EstimatedVehicleJourney></EstimatedJourneyVersionFrame></EstimatedTimetableDelivery>
</ServiceDelivery></Siri>"""


def _bronze_file(root, day, hour, name):
    folder = root / "bronze" / "entur_siri" / f"dt={day}" / f"hour={hour}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(b"")
    return path


# run_id_from_path

def test_run_id_from_cron_file():
    assert run_id_from_path("entur_siri_20261027T140002Z_local-1.xml.gz") == "local-1"


def test_run_id_from_actions_file():
    assert run_id_from_path("entur_siri_20261027T140002Z_12345-1.xml.gz") == "12345-1"


def test_run_id_from_uncompressed_file():
    assert run_id_from_path("entur_siri_20261027T140002Z_local-1.xml") == "local-1"


# list_bronze_files

def _three_days(tmp_path):
    _bronze_file(tmp_path, "2026-10-26", "10", "entur_siri_20261026T100000Z_a-1.xml.gz")
    _bronze_file(tmp_path, "2026-10-27", "10", "entur_siri_20261027T100000Z_b-1.xml.gz")
    _bronze_file(tmp_path, "2026-10-28", "10", "entur_siri_20261028T100000Z_c-1.xml.gz")


def test_lists_everything_in_order_without_dates(tmp_path):
    _three_days(tmp_path)
    names = [p.name for p in list_bronze_files(tmp_path)]
    assert names == [
        "entur_siri_20261026T100000Z_a-1.xml.gz",
        "entur_siri_20261027T100000Z_b-1.xml.gz",
        "entur_siri_20261028T100000Z_c-1.xml.gz",
    ]


def test_date_range_is_inclusive(tmp_path):
    _three_days(tmp_path)
    paths = list_bronze_files(tmp_path, date(2026, 10, 27), date(2026, 10, 27))
    assert [p.parent.parent.name for p in paths] == ["dt=2026-10-27"]


def test_from_and_to_cut_each_side(tmp_path):
    _three_days(tmp_path)
    assert len(list_bronze_files(tmp_path, date_from=date(2026, 10, 27))) == 2
    assert len(list_bronze_files(tmp_path, date_to=date(2026, 10, 27))) == 2


def test_temp_files_and_metadata_are_ignored(tmp_path):
    _bronze_file(tmp_path, "2026-10-27", "10", "entur_siri_20261027T100000Z_a-1.xml.gz")
    _bronze_file(tmp_path, "2026-10-27", "10", "_tmp_entur_siri_20261027T100500Z_b-1.xml.gz")
    meta = tmp_path / "bronze" / "entur_siri" / "_metadata"
    meta.mkdir(parents=True)
    (meta / "dt=2026-10-27.jsonl").write_text("{}\n")
    names = [p.name for p in list_bronze_files(tmp_path)]
    assert names == ["entur_siri_20261027T100000Z_a-1.xml.gz"]


# read_rows

def test_read_rows_flattens_and_standardizes(tmp_path):
    path = tmp_path / "entur_siri_20261027T130002Z_test-1.xml"
    shutil.copy(FIXTURE, path)
    rows, skipped, dropped = read_rows([path])
    assert len(rows) == 6
    assert skipped == []
    assert dropped == 0
    assert rows[0]["run_id"] == "test-1"
    assert rows[0]["service_date"] == date(2026, 10, 27)  # a date, not a string


def test_unreadable_files_are_skipped_not_fatal(tmp_path):
    good = tmp_path / "entur_siri_20261027T130002Z_test-1.xml"
    shutil.copy(FIXTURE, good)
    bad_xml = tmp_path / "entur_siri_20261027T130502Z_bad-1.xml"
    bad_xml.write_bytes(b"not xml")
    bad_gz = tmp_path / "entur_siri_20261027T131002Z_bad-2.xml.gz"
    bad_gz.write_bytes(b"not gzip")
    rows, skipped, _ = read_rows([good, bad_xml, bad_gz])
    assert len(rows) == 6
    assert sorted(Path(p).name for p, _ in skipped) == [bad_xml.name, bad_gz.name]


def test_row_with_missing_key_reaches_the_rules(tmp_path):
    # no <Order>: not dropped here, the MISSING_KEY rule rejects it later
    path = tmp_path / "entur_siri_20261027T130002Z_drop-1.xml"
    path.write_text(NO_ORDER_XML)
    rows, skipped, dropped = read_rows([path])
    assert sorted(r["stop_order"] or 0 for r in rows) == [0, 2]
    assert dropped == 0
    assert skipped == []


# build_silver end to end

def test_build_silver_twice_gives_the_same_table(spark, tmp_path):
    from entur_pipeline.silver.build import build_silver

    folder = tmp_path / "bronze" / "entur_siri" / "dt=2026-10-27" / "hour=13"
    folder.mkdir(parents=True)
    shutil.copy(FIXTURE, folder / "entur_siri_20261027T130002Z_a-1.xml")
    shutil.copy(FIXTURE, folder / "entur_siri_20261027T130502Z_b-1.xml")  # same data again
    cfg = {"data_root": str(tmp_path)}

    first = build_silver(spark, cfg, run_id="r1")
    second = build_silver(spark, cfg, run_id="r2")

    silver = spark.read.parquet(str(tmp_path / "silver" / "stop_calls"))
    assert silver.count() == first["rows_silver"] == second["rows_silver"] == 6
    assert first["duplicate_keys_after_dedup"] == 0
    quality = spark.read.parquet(str(tmp_path / "silver" / "quality"))
    assert quality.select("run_id").distinct().count() == 2


def _journey_xml(snapshot, service_date, journey_id):
    """One snapshot with one journey that has one recorded stop."""
    return f"""<Siri xmlns="http://www.siri.org.uk/siri"><ServiceDelivery>
<ResponseTimestamp>{snapshot}</ResponseTimestamp>
<EstimatedTimetableDelivery><EstimatedJourneyVersionFrame><EstimatedVehicleJourney>
<LineRef>L1</LineRef>
<FramedVehicleJourneyRef><DataFrameRef>{service_date}</DataFrameRef>
<DatedVehicleJourneyRef>{journey_id}</DatedVehicleJourneyRef></FramedVehicleJourneyRef>
<RecordedCalls><RecordedCall><StopPointRef>S1</StopPointRef><Order>1</Order>
<AimedArrivalTime>{snapshot}</AimedArrivalTime><ActualArrivalTime>{snapshot}</ActualArrivalTime>
</RecordedCall></RecordedCalls>
</EstimatedVehicleJourney></EstimatedJourneyVersionFrame></EstimatedTimetableDelivery>
</ServiceDelivery></Siri>"""


def test_partial_rerun_keeps_night_trips(spark, tmp_path):
    from entur_pipeline.silver.build import build_silver

    day = _bronze_file(tmp_path, "2026-10-27", "20", "entur_siri_20261027T200000Z_a-1.xml")
    day.write_text(_journey_xml("2026-10-27T20:00:00+00:00", "2026-10-27", "DAY"))
    # trip of the 27th that is still running after midnight UTC
    night = _bronze_file(tmp_path, "2026-10-28", "00", "entur_siri_20261028T000500Z_b-1.xml")
    night.write_text(_journey_xml("2026-10-28T00:05:00+00:00", "2026-10-27", "NIGHT"))
    cfg = {"data_root": str(tmp_path)}

    build_silver(spark, cfg, run_id="r1")
    # rerun only the 28th: the 27th must not be touched
    build_silver(spark, cfg, date(2026, 10, 28), date(2026, 10, 28), run_id="r2")
    rows = spark.read.parquet(str(tmp_path / "silver" / "stop_calls")).collect()
    assert sorted(r.journey_id for r in rows) == ["DAY", "NIGHT"]

    # rerun only the 27th: the night trip in the 28th's files is still found
    build_silver(spark, cfg, date(2026, 10, 27), date(2026, 10, 27), run_id="r3")
    rows = spark.read.parquet(str(tmp_path / "silver" / "stop_calls")).collect()
    assert sorted(r.journey_id for r in rows) == ["DAY", "NIGHT"]
