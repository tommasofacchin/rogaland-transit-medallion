"""Silver layer: explicit schema of `silver.stop_calls`, one row per stop call of one trip.

Grain: one stop (call) of one trip on one service date.
Key:   journey_id + service_date + stop_order.

Times are UTC timestamps. Arrival and departure columns are nullable: the first stop of a
trip has no arrival and the last has no departure. `*_actual_or_expected_*` holds the real
time for recorded calls and the prediction for estimated calls (see `call_status`).
"""
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STOP_CALLS_SCHEMA = StructType([
    # key
    StructField("journey_id", StringType(), nullable=False),    # DatedVehicleJourneyRef
    StructField("service_date", DateType(), nullable=False),    # DataFrameRef
    StructField("stop_order", IntegerType(), nullable=False),   # Order
    # journey
    StructField("line", StringType(), nullable=True),           # LineRef
    StructField("direction", StringType(), nullable=True),      # DirectionRef
    StructField("operator", StringType(), nullable=True),       # OperatorRef
    StructField("mode", StringType(), nullable=True),           # VehicleMode
    StructField("cancelled", BooleanType(), nullable=True),     # Cancellation
    # stop
    StructField("stop_id", StringType(), nullable=True),        # StopPointRef
    StructField("stop_name", StringType(), nullable=True),      # StopPointName
    # times (UTC)
    StructField("aimed_arrival", TimestampType(), nullable=True),
    StructField("aimed_departure", TimestampType(), nullable=True),
    StructField("actual_or_expected_arrival", TimestampType(), nullable=True),
    StructField("actual_or_expected_departure", TimestampType(), nullable=True),
    StructField("call_status", StringType(), nullable=False),   # recorded / estimated
    # derived
    StructField("arrival_delay_s", IntegerType(), nullable=True),
    StructField("departure_delay_s", IntegerType(), nullable=True),
    # lineage
    StructField("snapshot_time", TimestampType(), nullable=False),
    StructField("run_id", StringType(), nullable=True),
])
