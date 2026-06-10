from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Iterable

DDL_PATH = Path("queries/ddl_schema.sql")
BENCHMARK_SQL_PATH = Path("queries/populate_dim_benchmarks.sql")
VIEW_SQL_PATHS = (
    Path("queries/v_em_upcoding_anomalies.sql"),
    Path("queries/v_provider_peer_benchmark.sql"),
    Path("queries/v_billing_elasticity_anomalies.sql"),
)

# Canonical SQLite identifiers use the shorter `Rndrg_*` prefix. Older views
# and ad hoc SQL can still drift to `Rndrng_*`, so we route them here.
SQL_IDENTIFIER_ALIASES = {
    "Rndrng_Npi": "Rndrg_Npi",
    "Rndrng_Prvdr_Last_Org_Name": "Rndrg_Prvdr_Last_Org_Name",
    "Rndrng_Prvdr_First_Name": "Rndrg_Prvdr_First_Name",
    "Rndrng_Prvdr_Crdntl": "Rndrg_Prvdr_Crdntl",
    "Rndrng_Prvdr_Crdntls": "Rndrg_Prvdr_Crdntl",
    "Rndrng_Prvdr_Type": "Rndrg_Prvdr_Type",
    "Rndrng_Prvdr_Zip5": "Rndrg_Prvdr_Zip5",
    "Rndrng_Prvdr_State_Abrvtn": "Rndrg_Prvdr_State_Abrvtn",
}

# The raw CMS file can expose either the original `rndrng_*` headers or
# shorter variants, so ETL access should accept both.
SOURCE_FIELD_ALIASES = {
    "rndrng_npi": ("rndrng_npi", "rndrg_npi"),
    "rndrng_prvdr_last_org_name": (
        "rndrng_prvdr_last_org_name",
        "rndrg_prvdr_last_org_name",
    ),
    "rndrng_prvdr_first_name": (
        "rndrng_prvdr_first_name",
        "rndrg_prvdr_first_name",
    ),
    "rndrng_prvdr_crdntls": (
        "rndrng_prvdr_crdntls",
        "rndrg_prvdr_crdntl",
        "rndrg_prvdr_crdntls",
    ),
    "rndrng_prvdr_type": ("rndrng_prvdr_type", "rndrg_prvdr_type"),
    "rndrng_prvdr_zip5": ("rndrng_prvdr_zip5", "rndrg_prvdr_zip5"),
    "rndrng_prvdr_state_abrvtn": (
        "rndrng_prvdr_state_abrvtn",
        "rndrg_prvdr_state_abrvtn",
    ),
    "avg_sbmtd_chrg": ("avg_sbmtd_chrg", "avg_srvc_smtd_chrg"),
    "avg_mdcr_alowd_amt": ("avg_mdcr_alowd_amt", "avg_medcr_alwd_amt"),
    "avg_mdcr_pymt_amt": ("avg_mdcr_pymt_amt", "avg_medcr_pymt_amt"),
}


def rewrite_sql_identifiers(sql_text: str) -> str:
    """Convert rogue SQL identifiers to the canonical SQLite column names."""

    rewritten = sql_text
    for rogue_name, canonical_name in sorted(
        SQL_IDENTIFIER_ALIASES.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        rewritten = re.sub(
            rf"\b{re.escape(rogue_name)}\b",
            canonical_name,
            rewritten,
        )
    return rewritten


def read_sql_file(sql_path: Path) -> str:
    """Load a SQL file and normalize any stale identifiers before execution."""

    return rewrite_sql_identifiers(sql_path.read_text(encoding="utf-8"))


def get_source_value(record: dict[str, object], key: str) -> object:
    """Read a CMS source field through the alias router."""

    for candidate in SOURCE_FIELD_ALIASES.get(key, (key,)):
        if candidate in record:
            return record[candidate]
    return None


def find_rogue_database_objects(
    connection: sqlite3.Connection,
    aliases: Iterable[str] | None = None,
) -> dict[str, list[str]]:
    """Report views/tables whose stored SQL still contains rogue identifiers."""

    rogue_names = tuple(aliases or SQL_IDENTIFIER_ALIASES.keys())
    objects = connection.execute(
        """
        SELECT name, COALESCE(sql, '')
        FROM sqlite_master
        WHERE sql IS NOT NULL
        """
    ).fetchall()

    findings: dict[str, list[str]] = {}
    for name, sql_text in objects:
        matched = [
            rogue_name
            for rogue_name in rogue_names
            if re.search(rf"\b{re.escape(rogue_name)}\b", sql_text)
        ]
        if matched:
            findings[name] = matched
    return findings


def reconcile_database_objects(
    connection: sqlite3.Connection,
    ddl_path: Path = DDL_PATH,
    benchmark_sql_path: Path = BENCHMARK_SQL_PATH,
    view_sql_paths: Iterable[Path] = VIEW_SQL_PATHS,
) -> None:
    """Create missing schema objects and rebuild derived analytics objects."""

    connection.executescript(read_sql_file(ddl_path))
    connection.executescript(read_sql_file(benchmark_sql_path))
    for sql_path in view_sql_paths:
        connection.executescript(read_sql_file(sql_path))
