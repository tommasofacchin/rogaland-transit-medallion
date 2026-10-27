# Rogaland Transit Medallion

How punctual are the buses in Rogaland? Entur only shows what is happening *right now*, so we collect the live feed ourselves and build up a history we can actually analyse.

This is our project for DAT535 (Data Engineering) at UiS, by **Tommaso Facchin** and **Oliwier Trawinski**.

## The idea

Entur publishes real-time departures as SIRI XML. Every snapshot is a messy pile of nested trips, and the same trip shows up again and again with slightly updated times. There's no history, so you can't ask "which lines are usually late, and when?".

So the plan is:

1. Poll the Entur API on a schedule and keep every raw response.
2. Clean it up into one row per stop of each trip.
3. Aggregate it into punctuality numbers per line, stop and hour.

We use the medallion setup for this:

| Layer | What it holds |
|---|---|
| **Bronze** | Raw XML responses, untouched, with a bit of metadata (when, which run, which endpoint) |
| **Silver** | Parsed, deduplicated, validated stop calls. This is where most of the work is |
| **Gold** | Small tables with punctuality and average delay, ready for a chart or a query |

Processing is done with PySpark, and GitHub Actions handles testing, deployment and the schedule.

## Status

Bronze is running: a scheduled job on the prod VM fetches a snapshot every 5 minutes into `/data/entur` (see [scripts/run_bronze_cron.sh](scripts/run_bronze_cron.sh)). Silver and Gold come next. Things will change here as we go.

- [docs/plan.md](docs/plan.md): scope, design for each layer, who does what, timeline
- [docs/Guideline.md](docs/Guideline.md): the course guideline we're following

## What's not included

No streaming, no real-time dashboard, no delay prediction. It's a batch pipeline that answers one question well.

## Running it

Not available yet. This section will get the real steps (config, environments, how to trigger a run) once the pipeline exists. The goal is that you can clone the repo and run it without asking us anything.

## Data

Entur real-time API (SIRI). No key needed, but you have to send an `ET-Client-Name` header, and the rate limit is around 5 requests per minute. Entur data is open; we'll double check the exact licence and cite it in the report.

One catch: the API has no history, so anything we don't collect is gone. That's why Bronze starts first.

## Team

- Tommaso Facchin
- Oliwier Trawinski
