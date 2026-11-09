"""Silver cleaning rules: reject rows we cannot use, flag rows that Gold should skip.

A rejected row gets one reason code (the first rule it fails) and goes to rejected_calls.
A flagged row stays in Silver.
"""
from pyspark.sql import functions as F

MISSING_KEY = "MISSING_KEY"
MISSING_LINE_OR_STOP = "MISSING_LINE_OR_STOP"
MISSING_AIMED_TIME = "MISSING_AIMED_TIME"
REJECT_CODES = [MISSING_KEY, MISSING_LINE_OR_STOP, MISSING_AIMED_TIME]

# checked on 11 days of prod data (27 Oct - 6 Nov): 99.8% of stops are between -5 and 60 min
MIN_DELAY_S = -10 * 60
MAX_DELAY_S = 3 * 60 * 60

FLAGS = ["is_missing_actual", "is_delay_outlier", "is_bad_stop_name", "exclude_from_punctuality"]


def reject_reason():
    """Reason code of the first rule the row fails, or null if the row is ok."""
    missing_key = (F.col("journey_id").isNull() | F.col("service_date").isNull()
                   | F.col("stop_order").isNull())
    missing_line_or_stop = F.col("line").isNull() | F.col("stop_id").isNull()
    # without any aimed time we cannot compute a delay
    missing_aimed = F.col("aimed_arrival").isNull() & F.col("aimed_departure").isNull()

    return (F.when(missing_key, MISSING_KEY)
            .when(missing_line_or_stop, MISSING_LINE_OR_STOP)
            .when(missing_aimed, MISSING_AIMED_TIME))


def is_outlier(delay_col):
    too_big = (F.col(delay_col) < MIN_DELAY_S) | (F.col(delay_col) > MAX_DELAY_S)
    return F.coalesce(too_big, F.lit(False))  # null delay is not an outlier


def add_flags(df):
    no_actual = (F.col("actual_or_expected_arrival").isNull()
                 & F.col("actual_or_expected_departure").isNull())
    outlier = is_outlier("arrival_delay_s") | is_outlier("departure_delay_s")
    # "Ã" in a name usually means UTF-8 was read as latin-1 (Ã¸ instead of ø)
    bad_name = F.coalesce(F.col("stop_name").contains("Ã"), F.lit(False))
    cancelled = F.coalesce(F.col("cancelled"), F.lit(False))

    return (df.withColumn("is_missing_actual", no_actual)
            .withColumn("is_delay_outlier", outlier)
            .withColumn("is_bad_stop_name", bad_name)
            # cancelled trips stay in Silver for the cancellation rate, but not in delay numbers
            .withColumn("exclude_from_punctuality", cancelled | outlier | no_actual))


def apply_rules(df):
    """Split df into (clean, rejected). Rejected has an extra reason_code column."""
    df = df.withColumn("mode", F.lower(F.trim("mode")))
    df = df.withColumn("reason_code", reject_reason())

    rejected = df.filter(F.col("reason_code").isNotNull())
    clean = df.filter(F.col("reason_code").isNull()).drop("reason_code")
    return add_flags(clean), rejected
