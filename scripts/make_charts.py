"""Charts for the presentation and the report, made from Gold (and Silver for the route chart).

python scripts/make_charts.py --config config/prod.yaml --out charts/
Optional: --day 2026-11-10 (day for the funnel), --line KOL:Line:8_2043 (line for the route).
Needs matplotlib in the venv: ~/venv/bin/pip install matplotlib
(optional for the stop map: contextily for the background, folium for the HTML map)
"""
import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt  # no screen on the VM, matplotlib then draws to files only
import requests
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from entur_pipeline.config import load_config

MIN_CELL_CALLS = 300   # heatmap cells with fewer measured stops are greyed out
MIN_LINE_CALLS = 500   # same limit as gold_report.py
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
GREEN, GREY, RED, BLUE, ORANGE = "#2e8b57", "#b0b0b0", "#c0392b", "#2c6fbb", "#e08e0b"

# Performance on prod, 12 Nov, service day 4 Nov. Typed in from ~/perf/times.txt and the
# event logs (scripts/stages.py), because the event logs only live on the prod VM.
# Silver baseline, seconds per stage (average of the two baseline runs)
SILVER = [("parse XML (Python)", 137), ("rows to DataFrame + rules", 100),
          ("2nd pass: unparseable count", 27), ("dedup shuffle", 6), ("rest", 35)]
# Gold, seconds inside Spark: (200 partitions, 8 partitions)
GOLD = {"AQE on": (27, 15), "AQE off": (24, 13)}
# Silver for 1, 2, 3 service days (reads 3, 4, 5 Bronze days)
SCALE = [(3, 308), (4, 408), (5, 516)]


def share(on_time, measured):
    return F.try_divide(F.sum(on_time), F.sum(measured))  # null when nothing was measured


def heatmap(line_hour, out):
    """On-time share per weekday and hour (Norway time)."""
    # dayofweek: 1 = Sunday ... 7 = Saturday -> 0 = Monday ... 6 = Sunday
    wd = (F.dayofweek("service_date") + 5) % 7
    rows = (line_hour.groupBy(wd.alias("wd"), "hour")
            .agg(F.sum("measured_calls").alias("n"),
                 share("on_time_calls", "measured_calls").alias("s"))
            .collect())
    grid = [[float("nan")] * 24 for _ in range(7)]
    for r in rows:
        if r["n"] and r["n"] >= MIN_CELL_CALLS:
            grid[r["wd"]][r["hour"]] = r["s"] * 100

    fig, ax = plt.subplots(figsize=(12, 3.8))
    cmap = plt.get_cmap("RdYlGn").copy()
    cmap.set_bad("#eeeeee")
    im = ax.imshow(grid, cmap=cmap, vmin=30, vmax=90, aspect="auto")
    ax.set_xticks(range(24))
    ax.set_yticks(range(7), WEEKDAYS)
    ax.set_xlabel("Hour of planned departure (Norway time)")
    first, last, n_days = line_hour.agg(F.min("service_date"), F.max("service_date"),
                                        F.countDistinct("service_date")).first()
    ax.set_title(f"Share of stops on time (-1 to +3 min), by weekday and hour "
                 f"({first:%d %b} - {last:%d %b}, {n_days} days)")
    for d in range(7):
        for h in range(24):
            v = grid[d][h]
            if v == v:  # not nan
                ax.text(h, d, f"{v:.0f}", ha="center", va="center", fontsize=7)
    fig.colorbar(im, ax=ax, label="% on time", fraction=0.025)
    fig.text(0.01, 0.01, f"Grey: fewer than {MIN_CELL_CALLS} measured stops.", fontsize=8)
    save(fig, out / "1_heatmap_weekday_hour.png")


def worst_lines(line_hour, out, n=10):
    """Early / on time / late shares of the least punctual lines, with their public numbers."""
    total = line_hour.agg(share("on_time_calls", "measured_calls").alias("s")).first()["s"]
    rows = (line_hour.groupBy("line")
            .agg(F.max("public_code").alias("code"), F.max("line_name").alias("name"),
                 F.sum("measured_calls").alias("n"), F.sum("early_calls").alias("early"),
                 F.sum("on_time_calls").alias("on_time"), F.sum("late_calls").alias("late"))
            .filter(F.col("n") >= MIN_LINE_CALLS)
            .withColumn("s", F.col("on_time") / F.col("n"))
            .orderBy("s").limit(n).collect())
    rows = list(reversed(rows))  # worst at the top of the chart

    labels = []
    for r in rows:
        code, name = r["code"] or r["line"], r["name"] or ""
        labels.append(code if name in ("", code) else f"{code}  {name[:28]}")
    early = [100 * r["early"] / r["n"] for r in rows]
    on_time = [100 * r["on_time"] / r["n"] for r in rows]
    late = [100 * r["late"] / r["n"] for r in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    y = range(len(rows))
    ax.barh(y, early, color=BLUE, label="early (> 1 min)")
    ax.barh(y, on_time, left=early, color=GREEN, label="on time")
    ax.barh(y, late, left=[e + o for e, o in zip(early, on_time)], color=RED,
            label="late (> 3 min)")
    for i, (e, o) in enumerate(zip(early, on_time)):
        ax.text(e + o / 2, i, f"{o:.0f}%", ha="center", va="center", color="white", fontsize=8)
    ax.axvline(100 * total, color="black", linestyle="--", linewidth=1)
    ax.text(100 * total, len(rows) - 0.4, f" all lines: {100 * total:.0f}% on time", fontsize=8)
    ax.set_yticks(list(y), labels)
    ax.set_xlim(0, 100)
    ax.set_xlabel("% of measured stops")
    ax.set_title(f"The {n} least punctual lines (at least {MIN_LINE_CALLS} measured stops)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3, fontsize=8, frameon=False)
    save(fig, out / "2_worst_lines.png")


def funnel(spark, root, line_hour, day, out):
    """From stop rows read in Silver to stops on time, for one service date."""
    q = spark.read.option("mergeSchema", "true").parquet(str(root / "silver" / "quality"))
    if "date_from" not in q.columns:
        print("funnel: quality table has no dates yet, skipped (run Silver with the new code first)")
        return
    q = q.filter((F.col("date_from") == day) & (F.col("date_to") == day))
    latest = q.agg(F.max("run_time")).first()[0]
    if latest is None:
        print(f"funnel: no quality rows for {day}, skipped (run Silver for that day first)")
        return
    m = {r["metric"]: r["value"] for r in q.filter(F.col("run_time") == latest).collect()}
    rejected = sum(v for k, v in m.items() if k.startswith("rejected_"))
    g = (line_hour.filter(F.col("service_date") == day)
         .agg(F.sum("measured_calls").alias("measured"), F.sum("on_time_calls").alias("on_time"))
         .first())

    steps = [
        ("Stop rows in all snapshots", m["rows_clean"] + rejected),
        ("Rejected by the rules", rejected),
        ("After dedup (Silver)", m["rows_silver"]),
        ("Used for punctuality", g["measured"] or 0),
        ("On time", g["on_time"] or 0),
    ]
    fig, ax = plt.subplots(figsize=(10, 3.8))
    names = [s[0] for s in reversed(steps)]
    values = [s[1] for s in reversed(steps)]
    colors = [GREEN, BLUE, BLUE, RED, GREY]
    ax.barh(names, values, color=colors)
    ax.set_xscale("log")
    left = ax.get_xlim()[0]
    for i, v in enumerate(values):
        ax.text(max(v, left), i, f" {v:,.0f}", va="center", fontsize=9)  # 0 has no bar on log scale
    ax.set_xlabel("rows (log scale)")
    ax.set_title(f"What happens to one day of data ({day})")
    save(fig, out / "3_silver_funnel.png")


def route(spark, root, line, out):
    """Median delay at each stop of one line, from Silver, one panel per direction."""
    s = spark.read.parquet(str(root / "silver" / "stop_calls")).filter(F.col("line") == line)
    # a line has several variants (first stop, last stop, number of stops). Only inside one
    # variant stop_order is always the same stop, so we take medians per variant
    trip = Window.partitionBy("journey_id", "service_date")
    first_dep = F.min(F.coalesce("aimed_departure", "aimed_arrival")).over(trip)
    s = (s.withColumn("first", F.min_by("stop_name", "stop_order").over(trip))
         .withColumn("last", F.max_by("stop_name", "stop_order").over(trip))
         .withColumn("stops", F.count("*").over(trip))
         .withColumn("start_hour", F.hour(F.from_utc_timestamp(first_dep, "Europe/Oslo")))
         .cache())
    variants = (s.groupBy("first", "last", "stops")
                .agg(F.countDistinct("journey_id", "service_date").alias("trips"))
                .orderBy(F.desc("trips")).collect())
    picked = []  # most common variant from each first stop, max two
    for v in variants:
        if v["first"] not in [p["first"] for p in picked] and len(picked) < 2:
            picked.append(v)
    if not picked:
        print(f"route: no data for {line}, skipped")
        return
    code = (spark.read.parquet(str(root / "reference" / "line_names"))
            .filter(F.col("line") == line).select("public_code").first())
    title = code["public_code"] if code and code["public_code"] else line

    delay = F.coalesce("departure_delay_s", "arrival_delay_s") / 60
    periods = [("morning", 0, 11, RED), ("afternoon", 12, 23, BLUE)]
    fig, axes = plt.subplots(1, len(picked), figsize=(7 * len(picked), 4.8), sharey=True,
                             squeeze=False)
    for ax, v in zip(axes[0], picked):
        sv = s.filter((F.col("first") == v["first"]) & (F.col("last") == v["last"])
                      & (F.col("stops") == v["stops"]))
        names = {r["stop_order"]: r["name"] for r in
                 sv.groupBy("stop_order").agg(F.max("stop_name").alias("name")).collect()}
        # shorter variants between the same two stops are drawn dashed, placed by stop name
        # on the long variant's stops (stops they skip are just left out)
        position = {}
        for o in sorted(names):
            position.setdefault(names[o], o)
        others = [w for w in variants if w["first"] == v["first"] and w["last"] == v["last"]
                  and w["stops"] != v["stops"]]
        for w in [v] + others:
            sw = s.filter((F.col("first") == w["first"]) & (F.col("last") == w["last"])
                          & (F.col("stops") == w["stops"]))
            style = "o-" if w is v else "s--"
            extra = "" if w is v else f", {w['stops']}-stop variant"
            for label, h_from, h_to, color in periods:
                part = sw.filter(F.col("start_hour").between(h_from, h_to))
                n_trips = part.select("journey_id", "service_date").distinct().count()
                if n_trips < 5:  # too few trips for a median
                    continue
                rows = (part.filter(~F.col("exclude_from_punctuality"))
                        .groupBy("stop_order")
                        .agg(F.max("stop_name").alias("name"),
                             F.percentile_approx(delay, 0.5).alias("med"))
                        .filter(F.col("med").isNotNull()).orderBy("stop_order").collect())
                pts = [(position[r["name"]], r["med"]) for r in rows if r["name"] in position]
                ax.plot([x for x, _ in pts], [y for _, y in pts], style, color=color,
                        markersize=3, label=f"{label} trips{extra} ({n_trips})")
        ax.axhspan(-1, 3, color=GREEN, alpha=0.12, label="on time band")
        order = sorted(names)
        ticks = order[::max(1, len(order) // 12)]
        ax.set_xticks(ticks, [(names[o] or "")[:18] for o in ticks], rotation=45, ha="right",
                      fontsize=7)
        ax.set_title(f"{v['first']} -> {v['last']} ({v['stops']} stops)", fontsize=10)
        ax.legend(fontsize=8)
    axes[0][0].set_ylabel("median delay (min)")
    fig.suptitle(f"Line {title}: median delay along the route, all days")
    s.unpersist()
    save(fig, out / "4_route_delay.png")


def bronze_health(root, out, first_day="2026-10-27"):
    """Valid Bronze snapshots per day (288 = one every 5 min) and stored MB per day."""
    days = {}
    for path in sorted((root / "bronze" / "entur_siri" / "_metadata").glob("dt=*.jsonl")):
        day = path.stem[3:]
        if day < first_day:
            continue
        valid = other = stored = 0
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("_bronze_status") == "valid":
                valid += 1
            else:
                other += 1
            stored += r.get("stored_bytes") or 0
        days[day] = (valid, other, stored / 1e6)
    if not days:
        print("bronze_health: no metadata found, skipped")
        return
    names = list(days)
    x = range(len(names))
    fig, (a, b) = plt.subplots(2, 1, figsize=(11, 4.8), sharex=True,
                               gridspec_kw={"height_ratios": [2, 1]})
    a.bar(x, [v[0] for v in days.values()], color=GREEN, label="valid")
    a.bar(x, [v[1] for v in days.values()], bottom=[v[0] for v in days.values()], color=RED,
          label="failed")
    a.axhline(288, color="black", linestyle="--", linewidth=1)
    a.axhline(270, color=GREY, linestyle=":", linewidth=1)
    box = {"facecolor": "white", "edgecolor": "none", "pad": 1}
    a.text(len(names) - 0.5, 292, "288 = every 5 min", ha="right", va="bottom", fontsize=8)
    a.text(len(names) - 0.5, 262, "270 = nightly run fails below this", ha="right", va="top", fontsize=8, bbox=box)
    a.set_ylim(0, 320)
    a.set_ylabel("snapshots")
    a.set_title("Bronze: snapshots saved per day (UTC) and storage")
    a.legend(fontsize=8, loc="lower left")
    b.bar(x, [v[2] for v in days.values()], color=BLUE)
    b.set_ylabel("MB stored")
    b.set_xticks(list(x), [d[5:] for d in names], rotation=45, ha="right", fontsize=8)
    save(fig, out / "6_bronze_health.png")


def quality_per_day(spark, root, out):
    """Dedup and flags for every service date, from the last Silver run of each day."""
    q = spark.read.option("mergeSchema", "true").parquet(str(root / "silver" / "quality"))
    if "date_from" not in q.columns:
        print("quality_per_day: quality table has no dates yet, skipped")
        return
    q = q.filter(F.col("date_from").isNotNull() & (F.col("date_from") == F.col("date_to")))
    last = Window.partitionBy("date_from")
    q = (q.withColumn("latest", F.max("run_time").over(last))
         .filter(F.col("run_time") == F.col("latest")))
    per_day = {}
    for r in q.collect():
        per_day.setdefault(r["date_from"], {})[r["metric"]] = r["value"]
    days = sorted(per_day)
    if not days:
        print("quality_per_day: no rows, skipped")
        return
    m = [per_day[d] for d in days]
    rows_in = [v.get("rows_clean", 0) + sum(x for k, x in v.items() if k.startswith("rejected_"))
               for v in m]
    silver = [v.get("rows_silver", 0) for v in m]
    x = range(len(days))
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 4))
    a.bar(x, [s / 1000 for s in silver], color=BLUE)
    for i, (n_in, n_out) in enumerate(zip(rows_in, silver)):
        if n_out:
            a.text(i, n_out / 1000, f"x{n_in / n_out:.0f}", ha="center", va="bottom", fontsize=7)
    a.set_ylabel("thousand stops")
    a.set_title("Silver rows per service day (label: snapshot rows per Silver row)")
    a.set_xticks(list(x), [f"{d:%d %b}" for d in days], rotation=45, ha="right", fontsize=8)

    series = [("flagged_exclude_from_punctuality", "excluded from punctuality", RED),
              ("flagged_is_missing_actual", "no actual time", ORANGE),
              ("measured_estimated_only", "measured, forecast only", BLUE),
              ("flagged_is_delay_outlier", "delay outlier", GREEN),
              ("flagged_is_bad_stop_name", "broken stop name", GREY)]
    for key, label, color in series:
        b.plot(x, [100 * v.get(key, 0) / v["rows_silver"] if v.get("rows_silver") else 0
                   for v in m], "o-", markersize=3, color=color, label=label)
    rejected = [sum(x for k, x in v.items() if k.startswith("rejected_")) for v in m]
    b.set_ylabel("% of Silver rows")
    b.yaxis.set_major_formatter(lambda v, _: f"{v:.1f}%")
    b.set_title(f"Flags per service day (rejected rows in total: {sum(rejected):,.0f})")
    b.set_xticks(list(x), [f"{d:%d %b}" for d in days], rotation=45, ha="right", fontsize=8)
    b.legend(fontsize=7)
    save(fig, out / "7_quality_per_day.png")


def delay_distribution(spark, root, out):
    """Histogram of measured delays (30 s bins), with the on-time band."""
    s = spark.read.parquet(str(root / "silver" / "stop_calls"))
    delay = F.coalesce("departure_delay_s", "arrival_delay_s")
    s = s.filter(~F.col("exclude_from_punctuality") & delay.isNotNull()).select(delay.alias("d"))
    total, early, on_time, over10 = s.agg(
        F.count("*"), F.sum((F.col("d") < -60).cast("int")),
        F.sum(F.col("d").between(-60, 180).cast("int")),
        F.sum((F.col("d") > 600).cast("int"))).first()
    bins = (s.withColumn("b", F.floor(F.greatest(F.least(F.col("d"), F.lit(1199)), F.lit(-300)) / 30))
            .groupBy("b").count().orderBy("b").collect())
    late = total - early - on_time
    fig, ax = plt.subplots(figsize=(11, 4))
    # the first and last bar hold everything below -5 and above +20 min, shown in grey
    edge = [r["b"] in (-10, 39) for r in bins]
    ax.bar([r["b"] * 0.5 for r in bins], [r["count"] / 1000 for r in bins], width=0.5,
           align="edge", color=[GREY if e else BLUE for e in edge])
    for r, e in zip(bins, edge):
        if e:
            label = "-5 min\nor less" if r["b"] < 0 else "20 min\nor more"
            ax.text(r["b"] * 0.5 + 0.25, r["count"] / 1000, label, ha="center", va="bottom",
                    fontsize=7)
    ax.axvspan(-1, 3, color=GREEN, alpha=0.15, zorder=0)
    top = max(r["count"] for r in bins) / 1000
    ax.set_ylim(0, top * 1.25)
    ax.text(1, top * 1.06, f"on time\n{100 * on_time / total:.0f}%", ha="center",
            fontsize=9)
    ax.text(-3.5, ax.get_ylim()[1] * 0.5, f"early\n{100 * early / total:.0f}%", ha="center",
            fontsize=9)
    ax.text(8, ax.get_ylim()[1] * 0.5, f"late (> 3 min) {100 * late / total:.0f}%\n"
            f"more than 10 min late: {100 * over10 / total:.1f}%", fontsize=9)
    ax.set_xlim(-5, 20)
    ax.set_xlabel("delay (min), 30 s bars: the bar at 0 holds 0 to 29 s late")
    ax.set_ylabel("thousand stops")
    ax.set_title(f"Delay of all {total:,} measured stops")
    save(fig, out / "8_delay_distribution.png")


def daily_trend(line_hour, out):
    """On-time share and measured stops per service date, weekends shaded."""
    rows = (line_hour.groupBy("service_date")
            .agg(F.sum("measured_calls").alias("n"),
                 share("on_time_calls", "measured_calls").alias("s"))
            .orderBy("service_date").collect())
    x = range(len(rows))
    fig, ax = plt.subplots(figsize=(11, 4))
    ax2 = ax.twinx()
    ax2.bar(x, [r["n"] / 1000 for r in rows], color=GREY, alpha=0.4)
    ax2.set_ylabel("measured stops (thousand)", color="#777777")
    for i, r in enumerate(rows):
        if r["service_date"].weekday() >= 5:
            ax.axvspan(i - 0.5, i + 0.5, color=BLUE, alpha=0.08)
    ax.plot(x, [100 * r["s"] for r in rows], "o-", color=GREEN)
    ax.set_zorder(ax2.get_zorder() + 1)
    ax.patch.set_visible(False)
    ax.set_ylim(0, 100)
    ax.set_ylabel("% on time")
    ax.set_xticks(list(x), [f"{r['service_date']:%a %d %b}" for r in rows], rotation=45,
                  ha="right", fontsize=8)
    ax.set_title("On-time share per service day (weekends shaded)")
    save(fig, out / "9_daily_trend.png")


# grey map from Kartverket (Norwegian mapping authority), free and without an API key
KARTVERKET = ("https://cache.kartverket.no/v1/wmts/1.0.0/topograatone/default/webmercator/"
              "{z}/{y}/{x}.png")
KARTVERKET_ATTR = "Map: Kartverket (CC BY 4.0)"


def draw_stop_map(pts, box, zoom, title, path):
    """One static map: stops as dots on Kartverket's grey map. zoom = detail of the map tiles."""
    inside = [p for p in pts if box[0] <= p[0] <= box[1] and box[2] <= p[1] <= box[3]]
    fig, ax = plt.subplots(figsize=(9, 9))
    sc = ax.scatter([p[0] for p in inside], [p[1] for p in inside], c=[p[2] for p in inside],
                    s=[6 + p[3] / 300 for p in inside], cmap="RdYlGn", vmin=20, vmax=90,
                    edgecolors="black", linewidths=0.2, alpha=0.9, zorder=3)
    ax.set_xlim(box[0], box[1])
    ax.set_ylim(box[2], box[3])
    ax.set_aspect(1 / math.cos(math.radians((box[2] + box[3]) / 2)))  # not stretched
    try:
        import contextily  # pip install contextily
        contextily.add_basemap(ax, crs="EPSG:4326", source=KARTVERKET, zoom=zoom,
                               attribution=KARTVERKET_ATTR)
    except Exception as exc:
        print(f"stop_map: no background map ({exc})")
    ax.set_xticks([])
    ax.set_yticks([])
    fig.colorbar(sc, ax=ax, label="% on time", fraction=0.035)
    ax.set_title(f"{title}\ndot size = number of measured stops", fontsize=10)
    save(fig, path)


def stop_map(root, cfg, stop, out, min_calls=300):
    """Stops coloured by on-time share, on a map. Coordinates come from Journey Planner (quays)
    and are kept in reference/stop_coords.json, so the API is only asked about new stops.
    Static PNG/PDF needs contextily for the background map, the HTML map needs folium."""
    rows = (stop.groupBy("stop_id")
            .agg(F.max("stop_name").alias("name"), F.sum("measured_calls").alias("n"),
                 share("on_time_calls", "measured_calls").alias("s"))
            .filter(F.col("n") >= min_calls).collect())
    cache_file = root / "reference" / "stop_coords.json"
    coords = json.loads(cache_file.read_text()) if cache_file.exists() else {}
    todo = sorted({r["stop_id"] for r in rows} - set(coords))
    query = "query($ids: [String]) { quays(ids: $ids) { id latitude longitude } }"
    try:
        for i in range(0, len(todo), 100):
            resp = requests.post("https://api.entur.io/journey-planner/v3/graphql", timeout=60,
                                 headers={"ET-Client-Name": cfg["entur"]["client_name"]},
                                 json={"query": query, "variables": {"ids": todo[i:i + 100]}})
            resp.raise_for_status()
            for q in resp.json()["data"]["quays"] or []:
                if q and q.get("latitude"):
                    coords[q["id"]] = [q["latitude"], q["longitude"]]
    except Exception as exc:  # the map is optional, the other charts should still be made
        print(f"stop_map: could not get coordinates ({exc}), using what we have")
    cache_file.write_text(json.dumps(coords))
    pts = [(coords[r["stop_id"]][1], coords[r["stop_id"]][0], 100 * r["s"], r["n"], r["name"])
           for r in rows if r["stop_id"] in coords]
    if not pts:
        print("stop_map: no coordinates, skipped")
        return
    pts.sort(key=lambda p: -p[2])  # worst stops drawn last, on top

    # zoom on where most stops are, a few far away stops are left outside
    lons, lats = sorted(p[0] for p in pts), sorted(p[1] for p in pts)
    k = len(pts) // 50
    box = (lons[k] - 0.02, lons[-k - 1] + 0.02, lats[k] - 0.01, lats[-k - 1] + 0.01)
    # the whole area, and a close-up of Stavanger - Sandnes with a more detailed map
    draw_stop_map(pts, box, 11, f"On-time share per stop ({len(pts)} stops with {min_calls}+ "
                  "measured)", out / "10_stop_map.png")
    draw_stop_map(pts, (5.62, 5.82, 58.83, 59.00), 13,
                  "Stavanger - Sandnes, on-time share per stop", out / "11_stop_map_city.png")

    try:
        import folium  # pip install folium; the HTML map loads its tiles in the browser
    except ImportError:
        print("stop_map: folium not installed, no HTML map")
        return
    cmap = plt.get_cmap("RdYlGn")
    m = folium.Map(location=[(box[2] + box[3]) / 2, (box[0] + box[1]) / 2], zoom_start=11,
                   tiles=KARTVERKET, attr=KARTVERKET_ATTR)
    for lon, lat, pct, n, name in pts:
        r, g, b, _ = cmap(min(max((pct - 20) / 70, 0), 1))
        color = f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"
        folium.CircleMarker([lat, lon], radius=3 + n / 2000, color=color, fill=True,
                            fill_opacity=0.9, weight=1,
                            tooltip=f"{name}: {pct:.0f}% on time ({n:,} measured)").add_to(m)
    m.save(str(out / "10_stop_map.html"))
    print("saved", out / "10_stop_map.html")


def performance(out):
    """Where Silver's time goes, Gold with 200 vs 8 shuffle partitions, and Silver scaling."""
    fig, (a, b, c) = plt.subplots(1, 3, figsize=(13, 3.8), gridspec_kw={"width_ratios": [1.3, 1, 1]})

    # a: where Silver's time goes
    left = 0
    colors = [RED, ORANGE, BLUE, GREEN, GREY]
    for (name, sec), color in zip(SILVER, colors):
        a.barh([0], [sec], height=0.5, left=left, color=color, label=f"{name} ({sec} s)")
        left += sec
    a.set_yticks([])
    a.set_xlabel("seconds")
    a.set_title("Silver, one day: time per stage")
    a.legend(fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=2, frameon=False)

    # b: Gold with 200 vs 8 shuffle partitions
    x = range(len(GOLD))
    a200 = [v[0] for v in GOLD.values()]
    a8 = [v[1] for v in GOLD.values()]
    b.bar([i - 0.2 for i in x], a200, 0.4, color=GREY, label="200 partitions (default)")
    b.bar([i + 0.2 for i in x], a8, 0.4, color=GREEN, label="8 partitions")
    for i, (p, q) in enumerate(zip(a200, a8)):
        b.text(i - 0.2, p, f"{p}", ha="center", va="bottom", fontsize=8)
        b.text(i + 0.2, q, f"{q}", ha="center", va="bottom", fontsize=8)
    b.set_xticks(list(x), list(GOLD))
    b.set_ylabel("seconds")
    b.set_ylim(0, 36)
    b.set_title("Gold, one day: shuffle partitions")
    b.legend(fontsize=7, loc="upper right", ncol=2, frameon=False)

    # c: Silver time grows with the Bronze days read
    days = [d for d, _ in SCALE]
    secs = [s for _, s in SCALE]
    c.plot(days, secs, "o-", color=BLUE)
    for d, s, n in zip(days, secs, [1, 2, 3]):
        c.text(d + 0.08, s - 45, f"{n} service day{'s' if n > 1 else ''}: {s} s", fontsize=8)
    c.set_xticks(days)
    c.set_xlim(2.7, 5.6)
    c.set_xlabel("Bronze days read (service days + 2)")
    c.set_ylabel("seconds")
    c.set_ylim(0, 600)
    c.set_title("Silver: ~100 s per Bronze day read")

    save(fig, out / "5_performance.png")


def save(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    fig.savefig(path.with_suffix(".pdf"))  # vector version for the LaTeX report
    plt.close(fig)
    print("saved", path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--out", default="charts")
    parser.add_argument("--day", default="2026-11-10", help="service date for the funnel")
    parser.add_argument("--line", default="KOL:Line:8_2043", help="line for the route chart")
    args = parser.parse_args()
    cfg = load_config(args.config)
    root = Path(cfg["data_root"])
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    spark = (SparkSession.builder.appName("charts").config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    line_hour = spark.read.parquet(str(root / "gold" / "punctuality_by_line_hour")).cache()

    heatmap(line_hour, out)
    worst_lines(line_hour, out)
    funnel(spark, root, line_hour, args.day, out)
    route(spark, root, args.line, out)
    performance(out)
    bronze_health(root, out)
    quality_per_day(spark, root, out)
    delay_distribution(spark, root, out)
    daily_trend(line_hour, out)
    stop = spark.read.parquet(str(root / "gold" / "punctuality_by_stop"))
    stop_map(root, cfg, stop, out)
    spark.stop()


if __name__ == "__main__":
    main()
