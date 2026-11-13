#!/usr/bin/env bash
# Download CMS DE-SynPUF Sample 1 Outpatient Claims (public domain, synthetic
# Medicare claims) and sample 1,000 rows. Raw file is NOT committed.
# Source page:
# https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1
# Usage: bash scripts/download_cms_synpuf.sh
# Output: data/raw_open/cms_outpatient_sample1_1000.csv
set -euo pipefail
OUT_DIR="data/raw_open"
mkdir -p "$OUT_DIR"
URL="https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_outpatient_claims_sample_1.zip"
ZIP="$OUT_DIR/cms_outpatient_sample1.zip"
echo "Downloading $URL ..."
curl -L -o "$ZIP" "$URL"
unzip -o "$ZIP" -d "$OUT_DIR/cms_outpatient_sample1"
CSV=$(find "$OUT_DIR/cms_outpatient_sample1" -name "*.csv" | head -1)
echo "Source CSV: $CSV ($(wc -l < "$CSV") rows)"
head -1001 "$CSV" > "$OUT_DIR/cms_outpatient_sample1_1000.csv"
echo "Sampled 1,000 rows -> $OUT_DIR/cms_outpatient_sample1_1000.csv"
echo "NOTE: mapping CMS columns (CLM_ID, CLM_PMT_AMT, CLM_FROM_DT, ...) to the"
echo "Claim schema is still to do — see docs/open_data.md. Do not commit $ZIP."
