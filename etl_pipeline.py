# CMS Part B Provider Compliance Analytics ETL pipeline.

"""
This pipeline dynamically discovers and downloads the full CMS national bulk
archive from the federal metadata catalog, streams the enclosed CSV through
Python's zipfile and csv.DictReader without loading it into memory, filters
rows to North Carolina in-flight, and batch-loads every 10,000 qualifying
records into a normalized SQLite star schema inside a single master transaction.
"""

from __future__ import annotations

import codecs
import csv
import io
import json
import logging
import sqlite3
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Generator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from schema_router import get_source_value, read_sql_file

CATALOG_URL = "https://data.cms.gov/data.json"
TARGET_DATASET_TITLE = (
    "Medicare Physician & Other Practitioners - by Provider and Service"
)
STATE_FILTER = "NC"
BATCH_SIZE = 10_000          # Number of qualifying NC rows buffered before each executemany flush
DOWNLOAD_CHUNK_BYTES = 1_048_576  # 1 MB streaming blocks for the archive download
REQUEST_TIMEOUT_SECONDS = 120
DB_PATH = Path("data/cms_outliers.db")
ZIP_PATH = Path("data/cms_source.zip")
DDL_PATH = Path("queries/ddl_schema.sql")
USER_AGENT = "CMS-PartB-Provider-Compliance-ETL/1.0"

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


def iter_csv_rows(file_path: Path) -> Generator[dict[str, object], None, None]:
    """
    Yield one CSV row dict at a time, adaptively handling both compressed ZIP
    archives and raw Comma-Separated Values (CSV) text files dynamically.
    """

    # Pathway A: The file is genuinely a compressed ZIP archive
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, "r") as archive:
            csv_members = [
                name for name in archive.namelist()
                if name.lower().endswith(".csv")
            ]
            if not csv_members:
                raise RuntimeError(f"No CSV file found inside archive: {file_path}")

            csv_member = csv_members[0]
            with archive.open(csv_member) as raw_stream:
                text_stream = codecs.getreader("utf-8-sig")(raw_stream)
                reader = csv.DictReader(text_stream)
                for row in reader:
                    yield {str(k).lower(): v for k, v in row.items()}

    # Pathway B: The federal server handed us the uncompressed CSV directly
    else:
        # Open the massive text file dynamically using a zero-memory generator
        with file_path.open("r", encoding="utf-8-sig") as text_stream:
            reader = csv.DictReader(text_stream)
            for row in reader:
                yield {str(k).lower(): v for k, v in row.items()}


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



def clean_text(value: Any) -> str | None:
    """Normalize CMS string fields and preserve NULLs for missing values."""

    if value is None:
        return None

    text_value = str(value).strip()
    return text_value if text_value else None


def clean_int(value: Any) -> int | None:
    """Convert CMS integer-like values to Python ints before SQLite binding."""

    text_value = clean_text(value)
    if text_value is None:
        return None

    try:
        return int(Decimal(text_value.replace(",", "")))
    except (InvalidOperation, ValueError, TypeError):
        return None


def clean_decimal(value: Any) -> float | None:
    """Convert CMS numeric strings to floats for SQLite NUMERIC affinity."""

    text_value = clean_text(value)
    if text_value is None:
        return None

    try:
        return float(Decimal(text_value.replace(",", "")))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_row(record: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    """Split one CMS source row into matching lookup dimension and metric fact tuple rows."""

    provider_row = (
        clean_text(get_source_value(record, "rndrng_npi")),
        clean_text(get_source_value(record, "rndrng_prvdr_last_org_name")),
        clean_text(get_source_value(record, "rndrng_prvdr_first_name")),
        clean_text(get_source_value(record, "rndrng_prvdr_crdntls")),
        clean_text(get_source_value(record, "rndrng_prvdr_type")),
    )
    procedure_row = (
        clean_text(record.get("hcpcs_cd")),
        clean_text(record.get("hcpcs_desc")),
    )
    geography_row = (
        clean_text(get_source_value(record, "rndrng_prvdr_zip5")),
        clean_text(get_source_value(record, "rndrng_prvdr_state_abrvtn")),
    )
    fact_row = (
        clean_text(get_source_value(record, "rndrng_npi")),
        clean_text(record.get("hcpcs_cd")),
        clean_text(get_source_value(record, "rndrng_prvdr_zip5")),
        clean_text(record.get("place_of_srvc")),
        clean_int(record.get("tot_benes")),
        clean_decimal(record.get("tot_srvcs")),
        clean_decimal(get_source_value(record, "avg_sbmtd_chrg")),
        clean_decimal(get_source_value(record, "avg_mdcr_alowd_amt")),
        clean_decimal(get_source_value(record, "avg_mdcr_pymt_amt")),
    )

    return provider_row, procedure_row, geography_row, fact_row


def flush_buffers(
    connection: sqlite3.Connection,
    provider_rows: list[tuple[Any, ...]],
    procedure_rows: list[tuple[Any, ...]],
    geography_rows: list[tuple[Any, ...]],
    fact_rows: list[tuple[Any, ...]],
) -> int:
    """Flush the four in-memory row buffers to SQLite via atomic executemany calls.

    Dimensions are inserted first (INSERT OR IGNORE) so foreign-key constraints on
    the fact table are always satisfied before the fact rows land.
    Returns the number of fact rows flushed.
    """

    if not fact_rows:
        return 0

    connection.executemany(PROVIDER_INSERT_SQL, provider_rows)
    connection.executemany(PROCEDURE_INSERT_SQL, procedure_rows)
    connection.executemany(GEOGRAPHY_INSERT_SQL, geography_rows)
    connection.executemany(FACT_INSERT_SQL, fact_rows)

    return len(fact_rows)


def run_etl() -> None:
    """Orchestrate bulk discovery, streaming download, CSV parsing, and SQLite loading."""

    logger = logging.getLogger("cms_etl")

    # ------------------------------------------------------------------ #
    # Layer 1 – Dynamic bulk archive discovery                            #
    # ------------------------------------------------------------------ #
    download_url = discover_download_url()
    logger.info("Discovered bulk archive URL: %s", download_url)

    # ------------------------------------------------------------------ #
    # Layer 2 – Stream the archive to disk in 1 MB blocks                #
    # ------------------------------------------------------------------ #
    # download_archive(download_url, ZIP_PATH)

    # ------------------------------------------------------------------ #
    # Layer 3 – Open database, initialize schema, and wipe fact table    #
    # ------------------------------------------------------------------ #
    connection = open_database(DB_PATH)
    try:
        initialize_schema(connection, DDL_PATH)

        # Wipe previous fact rows so reruns stay idempotent. Dimension tables
        # retain their INSERT OR IGNORE protection and are not truncated.
        connection.execute("DELETE FROM fact_provider_services;")
        logger.info("Cleared previous fact records to prevent data duplication anomalies.")

        # ------------------------------------------------------------------ #
        # Layer 4 + 5 – Stream CSV rows, filter, buffer, and batch-insert    #
        # inside a single master transaction for maximum write performance.   #
        # ------------------------------------------------------------------ #
        
        provider_buf: list[tuple[Any, ...]] = []
        procedure_buf: list[tuple[Any, ...]] = []
        geography_buf: list[tuple[Any, ...]] = []
        fact_buf: list[tuple[Any, ...]] = []

        total_scanned = 0
        total_extracted = 0
        total_inserted = 0

        with connection:
            for record in iter_csv_rows(ZIP_PATH):
                total_scanned += 1

                # In-flight state filter — skip every non-NC row immediately
                # to keep the memory footprint near zero for the national file.
                if clean_text(
                    get_source_value(record, "rndrng_prvdr_state_abrvtn")
                ) != STATE_FILTER:
                    continue

                total_extracted += 1

                provider_row, procedure_row, geography_row, fact_row = normalize_row(record)

                # Guard against rows where any business key is absent.
                if not provider_row[0] or not procedure_row[0] or not geography_row[0]:
                    logger.warning(
                        "Skipping record with missing business key fields: %s", record
                    )
                    continue

                provider_buf.append(provider_row)
                procedure_buf.append(procedure_row)
                geography_buf.append(geography_row)
                fact_buf.append(fact_row)

                # Flush every BATCH_SIZE qualifying NC rows to keep buffer RAM bounded.
                if len(fact_buf) >= BATCH_SIZE:
                    flushed = flush_buffers(
                        connection, provider_buf, procedure_buf, geography_buf, fact_buf
                    )
                    total_inserted += flushed
                    logger.info(
                        "Flushed batch: %s rows inserted | "
                        "extracted so far: %s | scanned so far: %s",
                        flushed,
                        total_extracted,
                        total_scanned,
                    )
                    provider_buf.clear()
                    procedure_buf.clear()
                    geography_buf.clear()
                    fact_buf.clear()

            # Flush any remaining rows that did not fill a complete batch.
            if fact_buf:
                flushed = flush_buffers(
                    connection, provider_buf, procedure_buf, geography_buf, fact_buf
                )
                total_inserted += flushed
                logger.info(
                    "Flushed final partial batch: %s rows inserted.", flushed
                )

        logger.info(
            "ETL complete | scanned: %s | NC extracted: %s | inserted: %s",
            total_scanned,
            total_extracted,
            total_inserted,
        )
    except Exception:
        logger.exception("CMS ETL pipeline failed")
        raise
    finally:
        connection.close()


def main() -> None:

    """Entry point for running the ETL pipeline as a script."""

    configure_logging()
    run_etl()


if __name__ == "__main__":
    main()
