"""Small Gold consumer for the demo: the answers to our question, as Spark SQL queries.

python scripts/gold_report.py --config config/prod.yaml
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from entur_pipeline.config import load_config

# groups with fewer measured stops than this are left out, the share is too random
MIN_LINE_CALLS = 500
MIN_STOP_CALLS = 200

QUERIES = {
    "Status of the last Gold runs": None,
    "Whole period": """
        SELECT min(service_date) AS first_day, max(service_date) AS last_day,
               sum(calls) AS calls, sum(measured_calls) AS measured,
               round(try_divide(sum(on_time_calls), sum(measured_calls)), 3) AS on_time_share,
               round(try_divide(sum(cancelled_calls), sum(calls)), 4) AS cancelled_share
        FROM line_hour""",
    "10 least punctual lines": f"""
        SELECT line, sum(measured_calls) AS measured,
               round(try_divide(sum(on_time_calls), sum(measured_calls)), 3) AS on_time_share,
               round(try_divide(sum(late_calls), sum(measured_calls)), 3) AS late_share
        FROM line_hour GROUP BY line
        HAVING sum(measured_calls) >= {MIN_LINE_CALLS}
        ORDER BY on_time_share LIMIT 10""",
    "On time share per hour (Norway time), weekdays only": """
        SELECT hour, sum(measured_calls) AS measured,
               round(try_divide(sum(on_time_calls), sum(measured_calls)), 3) AS on_time_share
        FROM line_hour WHERE dayofweek(service_date) BETWEEN 2 AND 6
        GROUP BY hour ORDER BY hour""",
    "Weekday vs weekend": """
        SELECT CASE WHEN dayofweek(service_date) IN (1, 7) THEN 'weekend' ELSE 'weekday' END
                   AS day_type,
               sum(measured_calls) AS measured,
               round(try_divide(sum(on_time_calls), sum(measured_calls)), 3) AS on_time_share
        FROM line_hour GROUP BY 1 ORDER BY 1""",
    "10 least punctual stops": f"""
        SELECT stop_id, max(stop_name) AS stop_name, sum(measured_calls) AS measured,
               round(try_divide(sum(on_time_calls), sum(measured_calls)), 3) AS on_time_share
        FROM stop GROUP BY stop_id
        HAVING sum(measured_calls) >= {MIN_STOP_CALLS}
        ORDER BY on_time_share LIMIT 10""",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    cfg = load_config(parser.parse_args().config)
    gold = Path(cfg["data_root"]) / "gold"

    spark = SparkSession.builder.appName("gold-report").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.read.parquet(str(gold / "punctuality_by_line_hour")).createOrReplaceTempView("line_hour")
    spark.read.parquet(str(gold / "punctuality_by_stop")).createOrReplaceTempView("stop")

    for title, sql in QUERIES.items():
        print(f"\n== {title}")
        if sql is None:
            print("".join((gold / "_status" / "status.jsonl").read_text().splitlines(True)[-3:]))
        else:
            spark.sql(sql).show(24, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()