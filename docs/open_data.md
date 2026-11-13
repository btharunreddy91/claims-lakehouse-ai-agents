# Open-source sample data

The repo's own generator (Day 1) is synthetic. This page records the real
open-source data we also use, so anyone can reproduce it and check the licence.

## 1. Insurance claims — Mendeley Data (in the repo)

- **Dataset:** insurance_claims, Aqqad (2023), Mendeley Data, v2
- **DOI:** https://doi.org/10.17632/992mh7dk9y.2
- **File in repo:** `data/raw_open/insurance_claims.csv` (261 KB, 1,000 rows, 40 cols)
- **Licence:** Mendeley Data, CC BY 4.0 — cite the DOI when reusing.
- **What it is:** 1,000 anonymised auto-insurance claims from three US states
  (OH 352, IL 338, IN 310). No real people, no PII.
- **Checked Nov 12, 2026:** fraud_reported Y = 247, N = 753 (the project README
  that mirrors it says 200 — the file itself says 247, trust the file).
  Amounts $100–$114,920. Incident types: multi-vehicle collision 419,
  single-vehicle collision 403, vehicle theft 94, parked car 84.
- **Mapping:** `src/ingestion/load_open_data.py` → `Claim` schema.
  See that file's docstring for the honest gaps: no claim id, no status,
  no filed date — those are derived / defaulted and labelled as such.
  Fraud is kept in `description`, never invented as a status.
- **Note:** the Mendeley direct S3 download link returned 403 on Nov 12, 2026.
  The file in the repo was obtained from the public MIT-licensed mirror
  `kevinchs0808/DSA4263-Project` (`Data/Raw/insurance_claims.csv`, branch
  `prod`), which carries the same file. If you re-download, use the DOI above.

Run it through Bronze:

```bash
python -m src.ingestion.load_open_data
python -m src.ingestion.bronze_ingest --input data/raw_open/insurance_claims_mapped.jsonl --bronze data/bronze_open
```

## 2. CMS DE-SynPUF — Medicare synthetic claims (download script only)

- **Dataset:** CMS 2008–2010 Data Entrepreneurs' Synthetic Public Use File
  (DE-SynPUF), Sample 1 — fully synthetic Medicare claims, public domain
  (US federal government), no registration.
- **Source page:** https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1
- **Why it's not in the repo:** the files are large (e.g. the Prescription Drug
  Events ZIP alone is ~105 MB). Use `scripts/download_cms_synpuf.sh` to fetch
  the Outpatient Claims file and sample 1,000 rows — the script does not commit
  the raw file.
- **Use it for:** a second, health-claims-shaped source with real claim ids,
  payment amounts and dates, to test the same Bronze → Silver path at scale.
  Mapping CMS fields to `Claim` is still to do — see the script's notes.
