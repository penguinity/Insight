"""
Insight — CMS Part B Provider Compliance Analytics Engine
Diagnostic script: validates row counts, view health, and join integrity
across the entire SQLite warehouse.

Run from the project root:
    python diagnostics.py
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path

from etl_pipeline import DB_PATH
from schema_router import find_rogue_database_objects, reconcile_database_objects


def build_db_path_for_state(state: str) -> Path:
    """Return the conventional state-scoped database path for diagnostics."""

    normalized = state.strip().upper()
    if len(normalized) != 2 or not normalized.isalpha():
        raise ValueError("State must be a two-letter abbreviation like AL or NC")
    return Path(f"data/cms_outliers_{normalized.lower()}.db")


def discover_state_databases(data_dir: Path = Path("data")) -> dict[str, Path]:
    """Discover created state databases keyed by state-set suffix."""

    discovered: dict[str, Path] = {}
    for db_file in data_dir.glob("cms_outliers_*.db"):
        state_key = db_file.stem.removeprefix("cms_outliers_").upper()
        if state_key and all(part.isalpha() and len(part) == 2 for part in state_key.split("_")):
            discovered[state_key] = db_file
    return dict(sorted(discovered.items()))


def parse_args() -> argparse.Namespace:
    """Parse CLI flags that select which SQLite database to diagnose."""

    parser = argparse.ArgumentParser(
        description="Run integrity diagnostics against a CMS SQLite database."
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Explicit path to a SQLite file (overrides default state target).",
    )
    parser.add_argument(
        "--state",
        type=str,
        help="Two-letter state abbreviation (e.g., AL, NC).",
    )
    return parser.parse_args()


def resolve_target_db_path(args: argparse.Namespace) -> tuple[Path, str]:
    """Resolve target database path from CLI args with clear precedence rules."""

    if args.db and args.state:
        raise ValueError("Use either --db or --state, not both in the same run")

    if args.db:
        return args.db, "custom"

    if args.state:
        normalized_state = args.state.strip().upper()
        return build_db_path_for_state(normalized_state), normalized_state

    state_dbs = discover_state_databases()
    if len(state_dbs) == 1:
        only_state, only_path = next(iter(state_dbs.items()))
        return only_path, only_state

    if len(state_dbs) > 1:
        available = ", ".join(state_dbs.keys())
        raise ValueError(
            "Multiple state databases detected. "
            "Specify --state <XX> or --db <path>. "
            f"Available states: {available}"
        )

    return DB_PATH, "legacy-default"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def run_diagnostics(db_path: Path = DB_PATH) -> None:
    logger = logging.getLogger("cms_diagnostics")

    logger.info("Diagnostics target database: %s", db_path.resolve())

    if not db_path.exists():
        available = discover_state_databases()
        if available:
            logger.error(
                "Database file not found: %s | Available state databases: %s",
                db_path,
                ", ".join(available.keys()),
            )
        else:
            logger.error("Database file not found: %s", db_path)
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rogue_objects = find_rogue_database_objects(conn)
    with conn:
        reconcile_database_objects(conn)
    if rogue_objects:
        logger.info("ROUTED ROGUE SQL IDENTIFIERS")
        logger.info("=" * 55)
        for object_name, aliases in sorted(rogue_objects.items()):
            logger.info(
                "  %-35s %s",
                object_name,
                ", ".join(sorted(aliases)),
            )
    else:
        logger.info("Schema objects already aligned with canonical identifiers.")

    # ------------------------------------------------------------------ #
    # 1. Table row counts                                                  #
    # ------------------------------------------------------------------ #
    logger.info("=" * 55)
    logger.info("TABLE ROW COUNTS")
    logger.info("=" * 55)
    tables = [
        "fact_provider_services",
        "dim_providers",
        "dim_procedures",
        "dim_geography",
        "dim_benchmarks",
    ]
    for table in tables:
        try:
            count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            status = "OK" if count > 0 else "EMPTY — check ETL or benchmark load"
            logger.info("  %-35s %8d rows  [%s]", table, count, status)
        except Exception as exc:
            logger.warning("  %-35s ERROR: %s", table, exc)

    # ------------------------------------------------------------------ #
    # 2. Analytical view row counts                                        #
    # ------------------------------------------------------------------ #
    logger.info("=" * 55)
    logger.info("VIEW ROW COUNTS")
    logger.info("=" * 55)
    views = [
        "v_billing_elasticity_anomalies",
        "v_em_upcoding_anomalies",
        "v_provider_peer_benchmark",
    ]
    for view in views:
        try:
            count = cur.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
            status = "OK" if count > 0 else "ZERO — investigate joins/filters"
            logger.info("  %-40s %6d rows  [%s]", view, count, status)
        except Exception as exc:
            logger.warning("  %-40s ERROR: %s", view, exc)

    # ------------------------------------------------------------------ #
    # 3. Sample fact row — quick schema shape check                        #
    # ------------------------------------------------------------------ #
    logger.info("=" * 55)
    logger.info("SAMPLE FACT ROW")
    logger.info("=" * 55)
    try:
        row = cur.execute(
            "SELECT * FROM fact_provider_services LIMIT 1"
        ).fetchone()
        if row:
            for key in row.keys():
                logger.info("  %-35s %s", key, row[key])
        else:
            logger.warning("  fact_provider_services is empty — ETL has not run.")
    except Exception as exc:
        logger.error("  ERROR reading fact table: %s", exc)

    # ------------------------------------------------------------------ #
    # 4. Top 10 provider specialties                                       #
    # ------------------------------------------------------------------ #
    logger.info("=" * 55)
    logger.info("TOP 10 PROVIDER SPECIALTIES (dim_providers)")
    logger.info("=" * 55)
    try:
        rows = cur.execute("""
            SELECT Rndrg_Prvdr_Type, COUNT(*) AS cnt
            FROM dim_providers
            WHERE Rndrg_Prvdr_Type IS NOT NULL
            GROUP BY Rndrg_Prvdr_Type
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()
        for row in rows:
            logger.info("  %-45s %d", row[0], row[1])
    except Exception as exc:
        logger.error("  ERROR: %s", exc)

    # ------------------------------------------------------------------ #
    # 5. JOIN integrity: fact_provider_services → dim_providers            #
    # Orphaned NPI rows mean the dim was not populated for those records.  #
    # ------------------------------------------------------------------ #
    logger.info("=" * 55)
    logger.info("JOIN INTEGRITY: fact -> dim_providers")
    logger.info("=" * 55)
    try:
        total = cur.execute(
            "SELECT COUNT(*) FROM fact_provider_services"
        ).fetchone()[0]
        orphaned = cur.execute("""
            SELECT COUNT(*) FROM fact_provider_services f
            LEFT JOIN dim_providers p ON f.Rndrg_Npi = p.Rndrg_Npi
            WHERE p.Rndrg_Npi IS NULL
        """).fetchone()[0]
        pct = (orphaned / total * 100) if total > 0 else 0
        logger.info(
            "  Total fact rows: %d | Orphaned (no dim match): %d (%.1f%%)",
            total, orphaned, pct,
        )
        if orphaned > 0:
            logger.warning(
                "  Orphaned rows will be silently dropped from analytical views."
            )
    except Exception as exc:
        logger.error("  ERROR: %s", exc)

    # ------------------------------------------------------------------ #
    # 6. Benchmark coverage check                                          #
    # ------------------------------------------------------------------ #
    logger.info("=" * 55)
    logger.info("BENCHMARK COVERAGE (dim_benchmarks)")
    logger.info("=" * 55)
    try:
        bm_count = cur.execute(
            "SELECT COUNT(*) FROM dim_benchmarks"
        ).fetchone()[0]
        if bm_count == 0:
            logger.warning(
                "  dim_benchmarks is EMPTY. "
                "Run: sqlite3 %s < queries/populate_dim_benchmarks.sql",
                db_path,
            )
        else:
            logger.info(
                "  %d specialty/CPT benchmark combinations loaded.", bm_count
            )
    except Exception as exc:
        logger.warning("  dim_benchmarks missing or inaccessible: %s", exc)

    conn.close()
    logger.info("=" * 55)
    logger.info("DIAGNOSTICS COMPLETE")
    logger.info("=" * 55)


if __name__ == "__main__":
    configure_logging()
    cli_args = parse_args()
    try:
        target_db_path, target_label = resolve_target_db_path(cli_args)
    except ValueError as exc:
        logging.getLogger("cms_diagnostics").error("%s", exc)
        raise SystemExit(2) from exc

    logging.getLogger("cms_diagnostics").info(
        "Running diagnostics for target: %s", target_label
    )
    run_diagnostics(target_db_path)
