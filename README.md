# Insight

CMS Part B Provider Data, Benchmarks, and Analysis is a Python + SQLite analytics project for ingesting national CMS Part B provider/service data, normalizing it into a relational star schema, and surfacing outlier patterns through SQL views, diagnostics, and PDF reporting.

This README reflects the repository in its current state and aligns with shipped work shown in recent commits.

## Current Status

- ETL, schema creation, benchmarking, diagnostics, and PDF reporting are implemented and runnable.
- State-scoped database targeting is implemented for one or two states per ETL run.
- SQL view-based anomaly scoring is implemented.
- AI narrative plumbing exists, but the AI layer is not released as a production feature.

## Development Note (AI Layer)

The AI narrative component is still in development and is not released for production use.

- `ai_reporter.py` is present and callable.
- `pdf_report.py` defaults to deterministic non-AI narratives.
- AI output is only attempted when `--use-ai` is passed and `OPENROUTER_API_KEY` is configured.
- Treat generated narratives as draft analyst assist text, not final compliance determinations.

## Repository Scope

### Core Python scripts

- `etl_pipeline.py`
  - Discovers CMS bulk URL from `https://data.cms.gov/data.json`
  - Downloads archive to `data/cms_source.zip`
  - Extracts CSV if needed
  - Uses PySpark to normalize source columns and filter one/two states
  - Loads star schema tables in SQLite
  - Rebuilds `dim_benchmarks` with PySpark aggregates

- `diagnostics.py`
  - Verifies table and view row counts
  - Checks join integrity (`fact_provider_services` -> `dim_providers`)
  - Validates benchmark coverage
  - Resolves target DB by `--db`, `--state`, or auto-selection rules

- `schema_router.py`
  - Normalizes legacy SQL identifier aliases (`Rndrng_*` -> `Rndrg_*`)
  - Reconciles schema objects and view SQL in existing databases

- `pdf_report.py`
  - Fetches top anomalies
  - Prints terminal summaries
  - Generates branded PDF compliance memos in `reports/`
  - Supports optional AI narratives via `--use-ai`

- `ai_reporter.py`
  - Converts anomaly rows into structured audit context
  - Calls OpenRouter when enabled/configured
  - Returns fallback narrative when API key is absent

### SQL assets (`queries/`)

- `ddl_schema.sql` - canonical SQLite schema (dimensions, fact table, indexes, benchmark table)
- `populate_dim_benchmarks.sql` - SQL benchmark rebuild script (still used by schema reconciliation path)
- `v_billing_elasticity_anomalies.sql` - billing elasticity risk flags
- `v_em_upcoding_anomalies.sql` - E/M level-5 utilization outlier flags
- `v_provider_peer_benchmark.sql` - provider vs peer variance metrics and scoring

### Other repository files

- `requirements.txt` - pinned/runtime dependencies
- `project_relational_stack.dot` - architecture/relational graph source
- `quick test.py` - local exploratory helper script for inspecting raw state column values
- `reference/CMS API.txt` - reference notes

## Commit-Based Milestones

Recent commits indicate this delivery sequence:

- `feat: initial ingestion ETL engine and star schema framework for Insight`
- `feat(data): implement provider service star schema diagnostics`
- `feat: implement state-parameterized ETL pipeline`
- `feat: added PySpark dual state filtering and benchmarking`
- `fixed critial virtual env & local dependencies issues, added requirements.txt, added relational stack diagram`
- `feat: release initial version of pdf report generation`

Supporting maintenance commits include `.gitignore` adjustments and removal of incomplete scripts.

## Data Model

### Star schema tables

- `dim_providers`
- `dim_procedures`
- `dim_geography`
- `fact_provider_services`
- `dim_benchmarks`

The fact table links to the three dimensions with foreign keys and stores CMS service/amount metrics used by anomaly views.

### View logic summary

- `v_billing_elasticity_anomalies`
  - Flags rows when provider markup ratio exceeds peer baseline by >20% and peer group size >= 10
  - Assigns 35 risk points

- `v_em_upcoding_anomalies`
  - Flags NPIs where HCPCS `99215` exceeds 50% share of E/M volume with at least 50 E/M services
  - Assigns 30 risk points

- `v_provider_peer_benchmark`
  - Computes provider deltas and percent deltas vs specialty/CPT peers
  - Scores 40 points for >=40% allowed amount variance (peer group >=20)
  - Scores 20 points for >=25% allowed amount variance (peer group >=20)

## Environment and Dependencies

Install project dependencies:

```bash
pip install -r requirements.txt
```

Current requirements:

- `fpdf2>=2.8,<3`
- `pyspark==4.1.2`
- `python-dotenv>=1.0,<2`
- `requests>=2.32,<3`

Optional `.env` for AI development/testing:

```env
OPENROUTER_API_KEY=your_key_here
```

## Quickstart

### 1. Run ETL

Default states in code are currently `FL, GA`.

```bash
python etl_pipeline.py
```

Single-state example:

```bash
python etl_pipeline.py --states "FL"
```

Dual-state example:

```bash
python etl_pipeline.py --states "FL, GA"
```

Force new archive download:

```bash
python etl_pipeline.py --states "FL, GA" --force-download
```

### 2. Run diagnostics

Auto-target DB (with ambiguity protection):

```bash
python diagnostics.py
```

Target by state:

```bash
python diagnostics.py --state FL
```

Target by explicit path:

```bash
python diagnostics.py --db data/cms_outliers_fl_ga.db
```

### 3. Generate reports

Deterministic narratives (default):

```bash
python pdf_report.py --limit 10
```

Enable AI narrative attempt (development only):

```bash
python pdf_report.py --limit 10 --use-ai
```

Optional explicit DB target:

```bash
python pdf_report.py --db data/cms_outliers_fl_ga.db --limit 10
```

## Database Naming and Targeting

ETL writes state-scoped databases:

- One state: `data/cms_outliers_<state>.db` (example: `data/cms_outliers_fl.db`)
- Two states: `data/cms_outliers_<state1>_<state2>.db` (example: `data/cms_outliers_fl_ga.db`)

ETL also stores state filter metadata and prevents accidental reuse of a database with mismatched state scope.

## Windows PySpark Note

On Windows, if PySpark is installed in a path containing spaces or `&`, Spark startup can fail with `JAVA_GATEWAY_EXITED`.

`etl_pipeline.py` mitigates this by creating and using a safe junction path (`C:\sparklink_cms`) for `SPARK_HOME` when required.

## Outputs

- SQLite database files under `data/`
- Extracted CSV cache: `data/cms_source_extracted.csv` (when archive extraction is needed)
- PDF compliance memos under `reports/`

## Known Limitations

- AI layer is development-only and not production-released.
- `etl_pipeline.py --no-spark` is intentionally unsupported in the current revision and raises an error.
- `quick test.py` is a local utility script, not a production pipeline component.

## Data Source

- CMS dataset: Medicare Physician & Other Practitioners - by Provider and Service
- Metadata catalog endpoint: https://data.cms.gov/data.json
- The ETL dynamically discovers current distribution URLs from the catalog.

## License

See `LICENSE`.
