"""
Insight — CMS Part B Provider Compliance Analytics Engine
Diagnostic script: validates row counts, view health, and join integrity
across the entire SQLite warehouse.

Run from the project root:
    python diagnostics.py
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

DB_PATH = Path("data/cms_outliers.db")

from schema_router import find_rogue_database_objects, reconcile_database_objects

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def run_diagnostics(db_path: Path = DB_PATH) -> None:
    logger = logging.getLogger("cms_diagnostics")

    if not db_path.exists():
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
                "Run: sqlite3 data/cms_outliers.db < queries/populate_dim_benchmarks.sql"
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
    run_diagnostics()
