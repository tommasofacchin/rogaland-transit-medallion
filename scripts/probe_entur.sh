#!/usr/bin/env bash
# Step D1: look at what the Entur SIRI ET endpoint really returns.
# Usage: scripts/probe_entur.sh [datasetId]     (default KOL)
set -euo pipefail
DATASET="${1:-KOL}"
OUT="probe_${DATASET}_$(date -u +%Y%m%dT%H%M%SZ).xml"
curl -sS -H "ET-Client-Name: uis-dat535-group05" \
  -w "HTTP %{http_code}, %{size_download} bytes, %{time_total} s\n" \
  "https://api.entur.io/realtime/v1/rest/et?datasetId=${DATASET}" -o "$OUT"
echo "Saved: $OUT ($(du -h "$OUT" | cut -f1))"
echo "Journeys:      $(grep -o '<EstimatedVehicleJourney>' "$OUT" | wc -l)"
echo "Recorded calls: $(grep -o '<RecordedCall>' "$OUT" | wc -l)"
echo "Estimated calls: $(grep -o '<EstimatedCall>' "$OUT" | wc -l)"
echo "DataSource values (is it only ${DATASET}?):"
grep -o '<DataSource>[^<]*' "$OUT" | sed 's/<DataSource>//' | sort | uniq -c | sort -rn | head
