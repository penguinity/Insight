# CMS Part B Provider Compliance Analytics ETL pipeline.

"""
This pipeline dynamically discovers and downloads the full CMS national bulk
archive from the federal metadata catalog, extracts the enclosed CSV when
needed, standardizes source columns with PySpark, filters rows to one or two
configured states, and loads normalized rows into a SQLite star schema. The
pipeline also rebuilds peer benchmarks with PySpark so multi-state analytics
remain consistent with the selected ingestion scope.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sqlite3
import zipfile
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from schema_router import read_sql_file

CATALOG_URL = "https://data.cms.gov/data.json"
TARGET_DATASET_TITLE = (
    "Medicare Physician & Other Practitioners - by Provider and Service"
)
STATE_FILTER = "FL, GA"  # Supports one or two states: "FL" or "FL, GA"
BATCH_SIZE = 10_000
DOWNLOAD_CHUNK_BYTES = 1_048_576
REQUEST_TIMEOUT_SECONDS = 120
ZIP_PATH = Path("data/cms_source.zip")
EXTRACTED_CSV_PATH = Path("data/cms_source_extracted.csv")
DDL_PATH = Path("queries/ddl_schema.sql")
USER_AGENT = "CMS-PartB-Provider-Compliance-ETL/2.0"


def parse_states(raw_states: str) -> tuple[str, ...]:
    """Parse one or two comma-delimited state codes into normalized uppercase values."""

    cleaned = [part.strip().upper() for part in raw_states.split(",") if part.strip()]
    if not cleaned:
        raise ValueError("Provide at least one state, e.g. 'FL' or 'FL, GA'")

    unique_states: list[str] = []
    for state in cleaned:
        if len(state) != 2 or not state.isalpha():
            raise ValueError(f"Invalid state code: {state}")
        if state not in unique_states:
            unique_states.append(state)

    if len(unique_states) > 2:
        raise ValueError(
            "This ETL revision supports one or two states per run. "
            "Example: --states 'FL, GA'"
        )
    return tuple(unique_states)


def derive_db_path(states: Iterable[str]) -> Path:
    """Build a deterministic state-scoped database path for one or two states."""

    normalized = [state.lower() for state in states]
    return Path(f"data/cms_outliers_{'_'.join(normalized)}.db")


DEFAULT_STATES = parse_states(STATE_FILTER)
DB_PATH = derive_db_path(DEFAULT_STATES)


def _require_pyspark() -> tuple[Any, Any]:
    """Import and return SparkSession plus pyspark.sql.functions lazily."""

    try:
        spark_sql = importlib.import_module("pyspark.sql")
        spark_functions = importlib.import_module("pyspark.sql.functions")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PySpark is required for this ETL mode. Install with: pip install pyspark"
        ) from exc
    return spark_sql.SparkSession, spark_functions


def _resolve_column_name(df: Any, candidates: tuple[str, ...]) -> str | None:
    """Resolve a source column without depending on exact header casing."""

    normalized_columns = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        resolved = normalized_columns.get(candidate.lower())
        if resolved is not None:
            return resolved
    return None

PROVIDER_INSERT_SQL = """
    INSERT OR IGNORE INTO dim_providers (
        Rndrg_Npi,
        Rndrg_Prvdr_Last_Org_Name,
        Rndrg_Prvdr_First_Name,
        Rndrg_Prvdr_Crdntl,
        Rndrg_Prvdr_Type
    ) VALUES (?, ?, ?, ?, ?)
"""

PROCEDURE_INSERT_SQL = """
    INSERT OR IGNORE INTO dim_procedures (
        Hcpcs_Cd,
        Hcpcs_Desc
    ) VALUES (?, ?)
"""

GEOGRAPHY_INSERT_SQL = """
    INSERT OR IGNORE INTO dim_geography (
        Rndrg_Prvdr_Zip5,
        Rndrg_Prvdr_State_Abrvtn
    ) VALUES (?, ?)
"""

FACT_INSERT_SQL = """
    INSERT INTO fact_provider_services (
        Rndrg_Npi,
        Hcpcs_Cd,
        Rndrg_Prvdr_Zip5,
        Place_Of_Srvc,
        Tot_Benes,
        Tot_Srvcs,
        Avg_Srvc_Smtd_Chrg,
        Avg_Medcr_Alwd_Amt,
        Avg_Medcr_Pymt_Amt
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

BENCHMARK_INSERT_SQL = """
    INSERT INTO dim_benchmarks (
        Rndrg_Prvdr_Type,
        Hcpcs_Cd,
        Peer_Avg_Submitted_Charge,
        Peer_Avg_Allowed_Amt,
        Peer_Avg_Payment_Amt,
        Peer_Avg_Markup_Ratio,
        Peer_Row_Count
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def configure_logging() -> None:

    """Process-wide structured logging for ETL progress and failures."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def fetch_json(url: str) -> Any:

    """Fetch JSON from a remote endpoint with a predictable user agent."""

    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)
    except (HTTPError, URLError, json.JSONDecodeError) as exc:
        logging.exception("Failed to fetch JSON from %s", url)
        raise RuntimeError(f"Unable to fetch JSON from {url}") from exc


def discover_download_url() -> str:
    """Resolve the bulk archive download URL from the federal metadata catalog.

    Scans data.json for the target dataset title, then inspects each distribution
    node for a ZIP or CSV format entry. Prefers ZIP over CSV when both are present
    so we always fetch the smallest wire-transfer form of the full dataset.
    """

    catalog = fetch_json(CATALOG_URL)
    datasets = catalog.get("dataset", [])

    zip_url: str | None = None
    csv_url: str | None = None

    for dataset in datasets:
        if dataset.get("title") != TARGET_DATASET_TITLE:
            continue

        for distribution in dataset.get("distribution", []):
            fmt = (distribution.get("format") or "").upper()
            dl_url = distribution.get("downloadURL") or ""

            if fmt == "ZIP" and dl_url and zip_url is None:
                zip_url = dl_url
            elif fmt == "CSV" and dl_url and csv_url is None:
                csv_url = dl_url

        # Stop scanning after we hit the matching dataset node.
        break

    chosen = zip_url or csv_url
    if not chosen:
        raise RuntimeError(
            f"Could not find a ZIP or CSV distribution for: {TARGET_DATASET_TITLE}"
        )
    return chosen


def download_archive(download_url: str, dest_path: Path) -> None:
    """Stream the CMS bulk archive to disk in 1 MB chunks.

    Streaming block-by-block avoids loading the full compressed file into RAM,
    which is critical given that the national CMS dataset can exceed 500 MB.
    """

    logger = logging.getLogger("cms_etl")
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    request = Request(download_url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response, \
                open(dest_path, "wb") as dest_file:
            total_bytes = 0
            while True:
                chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                dest_file.write(chunk)
                total_bytes += len(chunk)
                logger.info(
                    "Downloaded %.1f MB so far...",
                    total_bytes / 1_048_576,
                )
    except (HTTPError, URLError) as exc:
        logging.exception("Failed to download archive from %s", download_url)
        raise RuntimeError(f"Archive download failed: {download_url}") from exc

    logger.info(
        "Archive download complete: %s (%.1f MB)",
        dest_path,
        dest_path.stat().st_size / 1_048_576,
    )


def resolve_csv_source(file_path: Path, extracted_csv_path: Path) -> Path:
    """Return a local CSV path, extracting from ZIP once when needed."""

    if not zipfile.is_zipfile(file_path):
        return file_path

    extracted_csv_path.parent.mkdir(parents=True, exist_ok=True)
    if extracted_csv_path.exists() and extracted_csv_path.stat().st_mtime >= file_path.stat().st_mtime:
        return extracted_csv_path

    with zipfile.ZipFile(file_path, "r") as archive:
        csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_members:
            raise RuntimeError(f"No CSV file found inside archive: {file_path}")

        member = csv_members[0]
        with archive.open(member) as source, extracted_csv_path.open("wb") as target:
            while True:
                chunk = source.read(DOWNLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                target.write(chunk)

    return extracted_csv_path


def open_database(db_path: Path) -> sqlite3.Connection:
    """Open the SQLite warehouse and enable referential integrity checks."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON;")
    # Boost write throughput for bulk loads by deferring fsync until transaction commit.
    connection.execute("PRAGMA synchronous = NORMAL;")
    connection.execute("PRAGMA journal_mode = WAL;")
    return connection


def initialize_schema(connection: sqlite3.Connection, ddl_path: Path) -> None:
    """Initialize the database schema from the canonical SQL DDL file."""

    connection.executescript(read_sql_file(ddl_path))


def _guard_states(connection: sqlite3.Connection, states: tuple[str, ...], db_path: Path) -> None:
    """Verify database metadata matches the configured one- or two-state target."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_metadata (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    connection.commit()

    row = connection.execute(
        "SELECT value FROM etl_metadata WHERE key = 'state_filters'"
    ).fetchone()

    encoded = ",".join(states)

    if row is None:
        connection.execute(
            "INSERT INTO etl_metadata (key, value) VALUES ('state_filters', ?)",
            (encoded,),
        )
        connection.execute("DELETE FROM etl_metadata WHERE key = 'state_filter'")
        connection.commit()
    elif row[0] != encoded:
        raise RuntimeError(
            f"Database '{db_path}' was built for state set '{row[0]}' but "
            f"states are currently '{encoded}'. "
            "Point to a new DB path or re-run with the original state set."
        )


def _first_existing_col(df: Any, candidates: tuple[str, ...], spark_functions: Any) -> Any:
    resolved_name = _resolve_column_name(df, candidates)
    existing = [spark_functions.col(resolved_name)] if resolved_name is not None else []
    if not existing:
        return spark_functions.lit(None)
    return spark_functions.coalesce(*existing)


def _clean_text_col(col: Any, spark_functions: Any) -> Any:
    return spark_functions.when(
        spark_functions.trim(col) == "",
        spark_functions.lit(None),
    ).otherwise(spark_functions.trim(col))


def _clean_numeric_col(col: Any, spark_functions: Any, cast_type: str = "double") -> Any:
    normalized = spark_functions.regexp_replace(spark_functions.trim(col), ",", "")
    return spark_functions.when(
        (spark_functions.trim(col) == "") | col.isNull(),
        spark_functions.lit(None),
    ).otherwise(normalized.cast(cast_type))


def build_standardized_frame(spark: Any, csv_path: Path, states: tuple[str, ...]) -> Any:
    """Use PySpark to standardize CMS columns and filter to one or two target states."""

    _, spark_functions = _require_pyspark()
    raw_df = spark.read.option("header", True).option("inferSchema", False).csv(str(csv_path))

    standardized = raw_df.select(
        _clean_text_col(
            _first_existing_col(raw_df, ("rndrng_npi", "rndrg_npi"), spark_functions),
            spark_functions,
        ).alias("rndrg_npi"),
        _clean_text_col(
            _first_existing_col(
                raw_df,
                ("rndrng_prvdr_last_org_name", "rndrg_prvdr_last_org_name"),
                spark_functions,
            ),
            spark_functions,
        ).alias("rndrg_prvdr_last_org_name"),
        _clean_text_col(
            _first_existing_col(
                raw_df,
                ("rndrng_prvdr_first_name", "rndrg_prvdr_first_name"),
                spark_functions,
            ),
            spark_functions,
        ).alias("rndrg_prvdr_first_name"),
        _clean_text_col(
            _first_existing_col(
                raw_df,
                ("rndrng_prvdr_crdntls", "rndrg_prvdr_crdntl", "rndrg_prvdr_crdntls"),
                spark_functions,
            ),
            spark_functions,
        ).alias("rndrg_prvdr_crdntl"),
        _clean_text_col(
            _first_existing_col(raw_df, ("rndrng_prvdr_type", "rndrg_prvdr_type"), spark_functions),
            spark_functions,
        ).alias("rndrg_prvdr_type"),
        _clean_text_col(
            _first_existing_col(raw_df, ("hcpcs_cd",), spark_functions),
            spark_functions,
        ).alias("hcpcs_cd"),
        _clean_text_col(
            _first_existing_col(raw_df, ("hcpcs_desc",), spark_functions),
            spark_functions,
        ).alias("hcpcs_desc"),
        _clean_text_col(
            _first_existing_col(raw_df, ("rndrng_prvdr_zip5", "rndrg_prvdr_zip5"), spark_functions),
            spark_functions,
        ).alias("rndrg_prvdr_zip5"),
        spark_functions.upper(
            _clean_text_col(
                _first_existing_col(
                    raw_df,
                    ("rndrng_prvdr_state_abrvtn", "rndrg_prvdr_state_abrvtn"),
                    spark_functions,
                ),
                spark_functions,
            )
        ).alias("rndrg_prvdr_state_abrvtn"),
        _clean_text_col(
            _first_existing_col(raw_df, ("place_of_srvc",), spark_functions),
            spark_functions,
        ).alias("place_of_srvc"),
        _clean_numeric_col(
            _first_existing_col(raw_df, ("tot_benes",), spark_functions),
            spark_functions,
            "int",
        ).alias("tot_benes"),
        _clean_numeric_col(
            _first_existing_col(raw_df, ("tot_srvcs",), spark_functions),
            spark_functions,
            "double",
        ).alias("tot_srvcs"),
        _clean_numeric_col(
            _first_existing_col(raw_df, ("avg_sbmtd_chrg", "avg_srvc_smtd_chrg"), spark_functions),
            spark_functions,
            "double",
        ).alias("avg_srvc_smtd_chrg"),
        _clean_numeric_col(
            _first_existing_col(raw_df, ("avg_mdcr_alowd_amt", "avg_medcr_alwd_amt"), spark_functions),
            spark_functions,
            "double",
        ).alias("avg_medcr_alwd_amt"),
        _clean_numeric_col(
            _first_existing_col(raw_df, ("avg_mdcr_pymt_amt", "avg_medcr_pymt_amt"), spark_functions),
            spark_functions,
            "double",
        ).alias("avg_medcr_pymt_amt"),
    )

    filtered = standardized.filter(spark_functions.col("rndrg_prvdr_state_abrvtn").isin(list(states)))

    return filtered.filter(
        spark_functions.col("rndrg_npi").isNotNull()
        & spark_functions.col("hcpcs_cd").isNotNull()
        & spark_functions.col("rndrg_prvdr_zip5").isNotNull()
    )


def _batched_rows(df: Any, columns: tuple[str, ...], batch_size: int = BATCH_SIZE):
    batch: list[tuple[Any, ...]] = []
    for row in df.select(*columns).toLocalIterator():
        batch.append(tuple(row[col] for col in columns))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_standardized_frame_to_sqlite(connection: sqlite3.Connection, standardized_df: Any) -> tuple[int, int]:
    """Load provider/procedure/geography/fact tables from a standardized Spark DataFrame."""

    logger = logging.getLogger("cms_etl")

    provider_df = standardized_df.select(
        "rndrg_npi",
        "rndrg_prvdr_last_org_name",
        "rndrg_prvdr_first_name",
        "rndrg_prvdr_crdntl",
        "rndrg_prvdr_type",
    ).dropDuplicates(["rndrg_npi"])

    procedure_df = standardized_df.select("hcpcs_cd", "hcpcs_desc").dropDuplicates(["hcpcs_cd"])
    geography_df = standardized_df.select(
        "rndrg_prvdr_zip5",
        "rndrg_prvdr_state_abrvtn",
    ).dropDuplicates(["rndrg_prvdr_zip5"])

    fact_df = standardized_df.select(
        "rndrg_npi",
        "hcpcs_cd",
        "rndrg_prvdr_zip5",
        "place_of_srvc",
        "tot_benes",
        "tot_srvcs",
        "avg_srvc_smtd_chrg",
        "avg_medcr_alwd_amt",
        "avg_medcr_pymt_amt",
    )

    provider_count = 0
    for batch in _batched_rows(
        provider_df,
        (
            "rndrg_npi",
            "rndrg_prvdr_last_org_name",
            "rndrg_prvdr_first_name",
            "rndrg_prvdr_crdntl",
            "rndrg_prvdr_type",
        ),
    ):
        connection.executemany(PROVIDER_INSERT_SQL, batch)
        provider_count += len(batch)

    procedure_count = 0
    for batch in _batched_rows(procedure_df, ("hcpcs_cd", "hcpcs_desc")):
        connection.executemany(PROCEDURE_INSERT_SQL, batch)
        procedure_count += len(batch)

    geography_count = 0
    for batch in _batched_rows(
        geography_df,
        ("rndrg_prvdr_zip5", "rndrg_prvdr_state_abrvtn"),
    ):
        connection.executemany(GEOGRAPHY_INSERT_SQL, batch)
        geography_count += len(batch)

    fact_count = 0
    for batch in _batched_rows(
        fact_df,
        (
            "rndrg_npi",
            "hcpcs_cd",
            "rndrg_prvdr_zip5",
            "place_of_srvc",
            "tot_benes",
            "tot_srvcs",
            "avg_srvc_smtd_chrg",
            "avg_medcr_alwd_amt",
            "avg_medcr_pymt_amt",
        ),
    ):
        connection.executemany(FACT_INSERT_SQL, batch)
        fact_count += len(batch)

    logger.info(
        "Loaded dims/facts | providers: %s | procedures: %s | geographies: %s | facts: %s",
        provider_count,
        procedure_count,
        geography_count,
        fact_count,
    )

    return fact_count, provider_count


def populate_benchmarks_with_spark(connection: sqlite3.Connection, standardized_df: Any) -> int:
    """Compute benchmark aggregates in PySpark and write to dim_benchmarks."""

    _, spark_functions = _require_pyspark()
    benchmark_df = standardized_df.filter(
        spark_functions.col("avg_srvc_smtd_chrg").isNotNull()
        & spark_functions.col("avg_medcr_alwd_amt").isNotNull()
        & (spark_functions.col("avg_medcr_alwd_amt") > spark_functions.lit(0))
        & spark_functions.col("rndrg_prvdr_type").isNotNull()
    ).withColumn(
        "markup_ratio",
        spark_functions.col("avg_srvc_smtd_chrg") / spark_functions.col("avg_medcr_alwd_amt"),
    ).groupBy("rndrg_prvdr_type", "hcpcs_cd").agg(
        spark_functions.round(spark_functions.avg("avg_srvc_smtd_chrg"), 4).alias("peer_avg_submitted_charge"),
        spark_functions.round(spark_functions.avg("avg_medcr_alwd_amt"), 4).alias("peer_avg_allowed_amt"),
        spark_functions.round(spark_functions.avg("avg_medcr_pymt_amt"), 4).alias("peer_avg_payment_amt"),
        spark_functions.round(spark_functions.avg("markup_ratio"), 6).alias("peer_avg_markup_ratio"),
        spark_functions.count(spark_functions.lit(1)).alias("peer_row_count"),
    )

    total = 0
    for batch in _batched_rows(
        benchmark_df,
        (
            "rndrg_prvdr_type",
            "hcpcs_cd",
            "peer_avg_submitted_charge",
            "peer_avg_allowed_amt",
            "peer_avg_payment_amt",
            "peer_avg_markup_ratio",
            "peer_row_count",
        ),
    ):
        connection.executemany(BENCHMARK_INSERT_SQL, batch)
        total += len(batch)

    return total


def run_etl(states: tuple[str, ...], use_spark: bool = True, force_download: bool = False) -> None:
    """Run ETL with one- or two-state ingestion and Spark-backed cleaning/benchmarks."""

    logger = logging.getLogger("cms_etl")

    # ------------------------------------------------------------------ #
    # Layer 1 – Dynamic bulk archive discovery                            #
    # ------------------------------------------------------------------ #
    download_url = discover_download_url()
    logger.info("Discovered bulk archive URL: %s", download_url)

    # ------------------------------------------------------------------ #
    # Layer 2 – Stream the archive to disk in 1 MB blocks                #
    # ------------------------------------------------------------------ #
    if force_download or not ZIP_PATH.exists():
        download_archive(download_url, ZIP_PATH)
    else:
        logger.info("Reusing existing archive at %s", ZIP_PATH.resolve())

    if use_spark:
        SparkSession, _ = _require_pyspark()
    else:
        raise RuntimeError(
            "This ETL revision requires PySpark to standardize columns and build benchmarks."
        )

    # ------------------------------------------------------------------ #
    # Layer 3 – Open database, initialize schema, and wipe fact table    #
    # ------------------------------------------------------------------ #
    db_path = derive_db_path(states)
    logger.info(
        "ETL target — states: %s | database: %s",
        ", ".join(states),
        db_path.resolve(),
    )

    connection = open_database(db_path)
    try:
        initialize_schema(connection, DDL_PATH)
        _guard_states(connection, states, db_path)

        # Wipe previous fact rows so reruns stay idempotent. Dimension tables
        # retain their INSERT OR IGNORE protection and are not truncated.
        connection.execute("DELETE FROM fact_provider_services;")
        connection.execute("DELETE FROM dim_benchmarks;")
        logger.info(
            "Cleared prior fact and benchmark data for state set %s.",
            ", ".join(states),
        )

        csv_source = resolve_csv_source(ZIP_PATH, EXTRACTED_CSV_PATH)

        spark = SparkSession.builder.appName("cms-dual-state-etl").getOrCreate()
        try:
            standardized_df = build_standardized_frame(spark, csv_source, states).cache()
            extracted_count = standardized_df.count()
            logger.info(
                "Spark extraction complete | states: %s | extracted rows: %s",
                ", ".join(states),
                extracted_count,
            )

            with connection:
                inserted_fact_count, _ = load_standardized_frame_to_sqlite(
                    connection,
                    standardized_df,
                )
                benchmark_count = populate_benchmarks_with_spark(connection, standardized_df)

            logger.info(
                "ETL complete | extracted: %s | facts inserted: %s | benchmark groups: %s",
                extracted_count,
                inserted_fact_count,
                benchmark_count,
            )
        finally:
            spark.stop()
    except Exception:
        logger.exception("CMS ETL pipeline failed")
        raise
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    """CLI options for one- or two-state ETL plus Spark execution controls."""

    parser = argparse.ArgumentParser(
        description="Run CMS Part B ETL with optional dual-state targeting and PySpark transforms."
    )
    parser.add_argument(
        "--states",
        default=STATE_FILTER,
        help="One or two state codes, comma-delimited. Example: 'FL' or 'FL, GA'.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Always download the latest archive, even if data/cms_source.zip exists.",
    )
    parser.add_argument(
        "--no-spark",
        action="store_true",
        help="Reserved for future compatibility. Current ETL path requires PySpark.",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for running the ETL pipeline as a script."""

    configure_logging()
    args = parse_args()

    states = parse_states(args.states)
    if args.no_spark:
        raise RuntimeError(
            "This ETL revision requires PySpark to standardize columns and build benchmarks."
        )

    run_etl(
        states=states,
        use_spark=True,
        force_download=args.force_download,
    )


if __name__ == "__main__":
    main()
