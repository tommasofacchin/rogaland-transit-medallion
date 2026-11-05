"""One test per cleaning rule, on small hand-made rows."""
import pytest
from silver_rows import call, df_of

from entur_pipeline.silver import rules


def _reasons(spark, *rows):
    clean, rejected = rules.apply_rules(df_of(spark, *rows))
    return clean.count(), sorted(r.reason_code for r in rejected.collect())


def test_valid_row_passes(spark):
    assert _reasons(spark, call()) == (1, [])


def test_missing_key_is_rejected(spark):
    assert _reasons(spark, call(), call(stop_order=None)) == (1, [rules.MISSING_KEY])
    assert _reasons(spark, call(service_date=None)) == (0, [rules.MISSING_KEY])


@pytest.mark.parametrize("column", ["line", "stop_id"])
def test_missing_line_or_stop_is_rejected(spark, column):
    assert _reasons(spark, call(**{column: None})) == (0, [rules.MISSING_LINE_OR_STOP])


def test_aimed_time_needs_arrival_or_departure(spark):
    only_departure = call(stop_order=3, aimed_arrival=None, aimed_departure=call()["aimed_arrival"])
    no_aimed = call(stop_order=4, aimed_arrival=None)
    assert _reasons(spark, only_departure, no_aimed) == (1, [rules.MISSING_AIMED_TIME])


def test_first_failing_rule_wins(spark):
    # missing key and missing line: one reason only, so the counts per reason add up
    assert _reasons(spark, call(stop_order=None, line=None)) == (0, [rules.MISSING_KEY])


def test_mode_is_lower_cased(spark):
    clean, _ = rules.apply_rules(df_of(spark, call(mode=" Bus ")))
    assert clean.first().mode == "bus"


def test_missing_actual_is_kept_and_flagged(spark):
    clean, rejected = rules.apply_rules(
        df_of(spark, call(actual_or_expected_arrival=None, arrival_delay_s=None)))
    [row] = clean.collect()
    assert row.is_missing_actual and row.exclude_from_punctuality
    assert rejected.count() == 0


@pytest.mark.parametrize("delay_s, outlier", [(90, False), (-11 * 60, True), (4 * 3600, True)])
def test_delay_outlier_flag(spark, delay_s, outlier):
    clean, _ = rules.apply_rules(df_of(spark, call(arrival_delay_s=delay_s)))
    assert clean.first().is_delay_outlier is outlier


def test_broken_stop_name_is_flagged(spark):
    df = df_of(spark, call(stop_name="Tjensvoll"), call(stop_order=3, stop_name="BrÃ¸dregata"))
    clean, _ = rules.apply_rules(df)
    assert [r.is_bad_stop_name for r in clean.orderBy("stop_order").collect()] == [False, True]


def test_cancelled_is_kept_but_excluded(spark):
    clean, rejected = rules.apply_rules(df_of(spark, call(), call(stop_order=3, cancelled=True)))
    rows = clean.orderBy("stop_order").collect()
    assert [r.exclude_from_punctuality for r in rows] == [False, True]
    assert rejected.count() == 0
