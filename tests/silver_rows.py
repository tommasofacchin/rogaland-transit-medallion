"""Hand-made rows in the schema.py columns, for the rule and quality tests."""
from datetime import date, datetime

from entur_pipeline.silver.build import INPUT_SCHEMA

T0 = datetime(2026, 10, 27, 7, 0, 0)


def call(**overrides):
    """A valid recorded call, 90 s late on arrival. Override any column."""
    row = {f.name: None for f in INPUT_SCHEMA.fields}
    row.update(journey_id="KOL:ServiceJourney:6_1001", service_date=date(2026, 10, 27),
               stop_order=2, line="KOL:Line:2_6", mode="bus", cancelled=False,
               stop_id="NSR:Quay:4120", stop_name="Kannik",
               aimed_arrival=T0, actual_or_expected_arrival=datetime(2026, 10, 27, 7, 1, 30),
               arrival_delay_s=90, call_status="recorded", snapshot_time=T0, run_id="local-1")
    row.update(overrides)
    return row


def df_of(spark, *rows):
    return spark.createDataFrame([tuple(r[f.name] for f in INPUT_SCHEMA.fields) for r in rows],
                                 INPUT_SCHEMA)
