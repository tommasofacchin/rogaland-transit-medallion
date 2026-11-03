from entur_pipeline.silver.schema import STOP_CALLS_SCHEMA

KEY = ["journey_id", "service_date", "stop_order"]


def test_key_columns_are_not_nullable():
    fields = {f.name: f for f in STOP_CALLS_SCHEMA.fields}
    for name in KEY:
        assert not fields[name].nullable


def test_column_names_are_unique_snake_case():
    names = STOP_CALLS_SCHEMA.fieldNames()
    assert len(names) == len(set(names))
    assert all(n == n.lower() and " " not in n for n in names)
