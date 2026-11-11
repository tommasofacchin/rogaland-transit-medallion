"""Can we trust Gold? Checks on Silver for one line, and one trip compared with raw Bronze.

python check_gold.py [line] [data_root]
"""
import gzip
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LINE = sys.argv[1] if len(sys.argv) > 1 else "KOL:Line:8_2043"
ROOT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("/data/entur")
S = "{http://www.siri.org.uk/siri}"

spark = (SparkSession.builder.appName("check-gold").config("spark.driver.memory", "4g")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.parquet(str(ROOT / "silver" / "stop_calls"))
      .filter(~F.col("exclude_from_punctuality"))
      .withColumn("delay_min", F.coalesce("departure_delay_s", "arrival_delay_s") / 60))
on_time = F.col("delay_min").between(-1, 3).cast("int")
recorded = (F.col("call_status") == "recorded").cast("int")

print("\n1. Real times (recorded) vs predictions (estimated), all lines")
(df.groupBy("call_status")
   .agg(F.count("*").alias("stops"), F.round(F.avg(on_time), 3).alias("on_time_share"),
        F.percentile_approx("delay_min", 0.5).alias("median_delay_min"))
   .show())

line = df.filter(F.col("line") == LINE)
print(f"\n2. {LINE}: delay along the route")
(line.groupBy("stop_order")
     .agg(F.count("*").alias("stops"), F.max("stop_name").alias("stop_name"),
          F.round(F.percentile_approx("delay_min", 0.5), 1).alias("median_delay_min"),
          F.round(F.avg(on_time), 3).alias("on_time_share"),
          F.round(F.avg(recorded), 3).alias("recorded_share"))
     .orderBy("stop_order").show(60, truncate=False))

print(f"\n3. {LINE}: median delay per hour (Norway time)")
hour = F.hour(F.from_utc_timestamp(F.coalesce("aimed_departure", "aimed_arrival"), "Europe/Oslo"))
(line.groupBy(hour.alias("hour"))
     .agg(F.count("*").alias("stops"),
          F.round(F.percentile_approx("delay_min", 0.5), 1).alias("median_delay_min"))
     .orderBy("hour").show(24))

print(f"\n4. One trip of {LINE}: Silver")
trip = line.filter("call_status = 'recorded'").orderBy(F.col("service_date").desc()).first()
jid, day = trip["journey_id"], trip["service_date"]
rows = line.filter((F.col("journey_id") == jid) & (F.col("service_date") == day)).orderBy("stop_order")
(rows.select("stop_order", "stop_name",
             F.date_format("aimed_departure", "HH:mm:ss").alias("aimed_utc"),
             F.date_format("actual_or_expected_departure", "HH:mm:ss").alias("actual_utc"),
             "call_status", F.round("delay_min", 1).alias("delay_min"))
     .show(60, truncate=False))

# the Bronze file closest to the newest snapshot this trip was seen in
stamp = rows.agg(F.date_format(F.max("snapshot_time"), "yyyyMMdd'T'HHmmss")).first()[0]
folder = ROOT / "bronze" / "entur_siri" / f"dt={stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}" / f"hour={stamp[9:11]}"


def seconds(hhmmss):
    return int(hhmmss[:2]) * 3600 + int(hhmmss[2:4]) * 60 + int(hhmmss[4:6])


path = min(folder.glob("entur_siri_*.xml*"), key=lambda p: abs(seconds(p.name[20:26]) - seconds(stamp[9:15])))
print(f"\n5. Same trip ({jid}, {day}) in raw Bronze: {path.name}")
opener = gzip.open if path.suffix == ".gz" else open
with opener(path, "rb") as f:
    tree = ET.parse(f)
for journey in tree.iter(S + "EstimatedVehicleJourney"):
    if journey.findtext(f"{S}FramedVehicleJourneyRef/{S}DatedVehicleJourneyRef") != jid:
        continue
    print("PublishedLineName:", journey.findtext(S + "PublishedLineName"))
    for call in journey.iter():
        if call.tag not in (S + "RecordedCall", S + "EstimatedCall"):
            continue
        real = call.findtext(S + "ActualDepartureTime") or call.findtext(S + "ExpectedDepartureTime")
        print(call.tag.replace(S, "")[:9], call.findtext(S + "Order"), call.findtext(S + "StopPointName"),
              "aimed", call.findtext(S + "AimedDepartureTime"), "actual/expected", real)

spark.stop()
