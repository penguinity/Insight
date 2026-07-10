# Insight — CMS Part B Provider Compliance Analytics Engine

A production-grade Python ETL and outlier detection system that streams CMS Medicare Part B national bulk data, loads it into a normalized SQLite star schema, and surfaces billing anomalies via SQL analytical views, AI-generated audit narratives, and PDF compliance memos.

---

## Project Structure

```
etl_pipeline.py           # Bulk streaming ETL: discovery → download → CSV parse → SQLite load
diagnostics.py            # Health check: row counts, view validation, join integrity
ai_reporter.py            # AI layer: anomaly context packaging + narrative generation
pdf_report.py             # PDF generation: renders AI memos into compliance audit reports for human analysts

queries/
  ddl_schema.sql                      # Star schema DDL: all tables, foreign keys, and indexes
  populate_dim_benchmarks.sql         # Peer group benchmark computation (run after each ETL load)
  v_billing_elasticity_anomalies.sql  # View: providers with markup ratio > 1.2x peer baseline
  v_em_upcoding_anomalies.sql         # View: providers with > 50% level-5 E/M coding share
  v_provider_peer_benchmark.sql       # View: all-provider benchmark deviation table

data/
  cms_source.zip          # Downloaded CMS bulk archive (gitignored)
  cms_outliers.db         # SQLite warehouse (gitignored)

reports/                  # Generated PDF audit memos (gitignored)
.env                      # Local secrets: API_KEY (gitignored)
```

---

## Quickstart

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

Windows note:
- `etl_pipeline.py` now auto-hardens `SPARK_HOME` to a safe junction path (`C:\\sparklink_cms`) when PySpark is installed under a path with spaces or `&`.
- This resolves the recurring `[JAVA_GATEWAY_EXITED]` startup failure without manual reconfiguration each run.

### 2. Configure environment

Add your API key to `.env`:

```
OPENROUTER_API_KEY=sk-or-...
```

### 3. Run the ETL pipeline

```bash
python etl_pipeline.py
```

Single-state explicit run:

```bash
python etl_pipeline.py --states "FL"
```

Dual-state run in one database:

```bash
python etl_pipeline.py --states "FL, GA"
```

The pipeline will:
- Fetch `data.cms.gov/data.json` to locate the latest bulk archive URL
- Stream the archive to `data/cms_source.zip` in 1 MB blocks
- Use PySpark to clean and standardize CMS columns in batch
- Filter rows to one or two states from `--states` (supports `STATE` or `STATE, STATE`)
- Batch-insert every 10,000 qualifying rows into the SQLite star schema
- Build `dim_benchmarks` directly with PySpark aggregations for the selected state set

Database naming:
- `--states "FL"` writes to `data/cms_outliers_fl.db`
- `--states "FL, GA"` writes to `data/cms_outliers_fl_ga.db`

### 4. Populate the benchmark table

PySpark now rebuilds `dim_benchmarks` inside `etl_pipeline.py`, so no separate SQL benchmark refresh is required for normal ETL runs.

### 5. Run diagnostics

```bash
python diagnostics.py
```

Validates row counts across all tables and views, checks join integrity, and warns if the benchmark table is empty.

### 6. Generate AI audit memos (PDF)

```bash
python pdf_report.py
```

Generates one PDF compliance memo per top-ranked anomaly in `reports/`.

---

## Outlier Detection Methodology

### Star Schema

| Table | Purpose |
|---|---|
| `fact_provider_services` | One row per NPI × HCPCS × place-of-service combination |
| `dim_providers` | Provider name, specialty, and credentials |
| `dim_procedures` | HCPCS code descriptions |
| `dim_geography` | ZIP code and state |
| `dim_benchmarks` | Pre-computed peer group averages per specialty/CPT |

### Statistical Benchmarks

Peer groups are defined as all service lines sharing the same `Rndrg_Prvdr_Type` (specialty) and `Hcpcs_Cd` (procedure code). The benchmark table stores:

- **`Peer_Avg_Markup_Ratio`** — `AVG(submitted_charge / allowed_amount)` across the peer group
- **`Peer_Avg_Allowed_Amt`** — Average Medicare allowed amount per service line
- **`Peer_Avg_Payment_Amt`** — Average Medicare payment amount per service line
- **`Peer_Row_Count`** — Number of service lines in the peer group (minimum 10 required to flag)

### Anomaly Flags

| View | Condition | Risk Points |
|---|---|---|
| `v_billing_elasticity_anomalies` | Provider markup ratio > 1.20× peer group baseline, peer group ≥ 10 | 35 |
| `v_em_upcoding_anomalies` | Level-5 E/M (99215) share > 50% with ≥ 50 total E/M services | 30 |
| `v_provider_peer_benchmark` | Allowed amount ≥ 40% above peer average | 40 |
| `v_provider_peer_benchmark` | Allowed amount 25–39% above peer average | 20 |

### AI Model

The AI narrative layer uses **`google/gemini-flash-1.5`** via OpenRouter (~$0.075/M input tokens) as the default model. To switch models, set `DEFAULT_MODEL` in `ai_reporter.py`. Recommended alternatives:

| Model | Best For |
|---|---|
| `google/gemini-flash-1.5` | Default — fast, cheap, coherent |
| `meta-llama/llama-3.1-70b-instruct` | Stronger reasoning, open weights |
| `anthropic/claude-3-haiku` | Most coherent long-form compliance |

---

## Rerunning the Pipeline

To perform a full wipe-and-reload:

```bash
# 1. Re-run ETL (clears fact table automatically before reload)
python etl_pipeline.py --states "FL, GA"

# 2. Validate
python diagnostics.py

# 3. Generate reports
python pdf_report.py
```

---

## Data Source

**CMS Medicare Physician & Other Practitioners — by Provider and Service**  
Source: [data.cms.gov](https://data.cms.gov)  
Catalog endpoint: `https://data.cms.gov/data.json`  
The pipeline resolves the current bulk archive URL dynamically at runtime — no hardcoded dataset IDs.

---

## Notes for Analysts

- All monetary thresholds (1.20× markup, 40% allowed amount variance) are configurable in the respective SQL view files under `queries/`.
- The benchmark peer group minimum of 10 service lines prevents false flags on rare specialty/CPT combinations.
- AI narratives are clearly labeled as AI-assisted and must be reviewed by a qualified analyst before distribution.
- The `.env` file is gitignored. Never commit API keys to version control.
