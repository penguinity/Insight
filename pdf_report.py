"""
Insight — CMS Part B Provider Compliance Analytics Engine
PDF Report Generator: renders AI-generated audit narratives into professional
single-page compliance memos using fpdf2.

Requires:
    pip install fpdf2

Run standalone to generate PDFs for the top 5 anomalies:
    python pdf_report.py
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, cast

try:
    from fpdf import FPDF as _FPDF
    from fpdf import XPos as _XPos
    from fpdf import YPos as _YPos
    FPDF_AVAILABLE = True
except ModuleNotFoundError:
    class _PosStub:
        LMARGIN = "LMARGIN"
        NEXT = "NEXT"

    _FPDF = object
    _XPos = _PosStub
    _YPos = _PosStub
    FPDF_AVAILABLE = False

FPDF = cast(Any, _FPDF)
XPos = cast(Any, _XPos)
YPos = cast(Any, _YPos)

from ai_reporter import build_audit_context, fetch_top_anomalies, generate_audit_narrative

REPORTS_DIR = Path("reports")
LEGACY_DB_PATH = Path("data/cms_outliers.db")

# Brand palette
COLOR_NAVY = (0, 51, 102)
COLOR_LIGHT_BLUE = (230, 240, 255)
COLOR_GREY = (128, 128, 128)
COLOR_BLACK = (30, 30, 30)


def _pdf_safe_text(value: Any) -> str:
    """Normalize text for built-in PDF fonts that only support latin-1."""

    text = str(value)
    replacements = {
        "—": "-",
        "–": "-",
        "’": "'",
        "“": '"',
        "”": '"',
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text.encode("latin-1", "replace").decode("latin-1")


def _deterministic_narrative(audit_context: dict[str, Any]) -> str:
    """Return a deterministic narrative when AI is unavailable or disabled."""

    prov = audit_context["provider"]
    metrics = audit_context["billing_metrics"]
    risk = audit_context["risk_assessment"]
    return (
        f"Provider {prov.get('npi')} ({prov.get('specialty')}) shows elevated billing behavior "
        f"for HCPCS {audit_context['procedure'].get('hcpcs_code')}. "
        f"The provider markup ratio is {metrics.get('provider_markup_ratio', 0):.2f}x versus "
        f"a peer baseline of {metrics.get('peer_group_markup_baseline', 0):.2f}x across "
        f"{metrics.get('peer_group_size_service_lines', 0)} peer service lines. "
        f"Current risk tier is {risk.get('risk_tier')} with "
        f"{risk.get('elasticity_risk_points', 0)} points. "
        "Recommended next step: validate coding documentation and compare billed amounts "
        "to CMS fee schedule and local policy guidance."
    )

if FPDF_AVAILABLE:
    class ComplianceMemo(FPDF):
        """FPDF subclass with branded header and footer for compliance memos."""

        def header(self) -> None:
            self.set_font("Helvetica", "B", 11)
            self.set_fill_color(*COLOR_NAVY)
            self.set_text_color(255, 255, 255)
            self.cell(
                0, 12,
                "INSIGHT  |  CMS Part B Provider Compliance Analytics Engine",
                new_x=XPos.LMARGIN, new_y=YPos.NEXT,
                fill=True, align="C",
            )
            self.set_text_color(0, 0, 0)
            self.ln(4)

        def footer(self) -> None:
            self.set_y(-14)
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(*COLOR_GREY)
            self.cell(
                0, 8,
                f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  "
                "CONFIDENTIAL - For Internal Compliance Use Only  |  "
                f"Page {self.page_no()}",
                align="C",
            )


def _section_heading(pdf: Any, title: str) -> None:
    pdf.set_fill_color(*COLOR_LIGHT_BLUE)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*COLOR_NAVY)
    pdf.cell(
        0, 7, _pdf_safe_text(f"  {title}"),
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        fill=True,
    )
    pdf.set_text_color(0, 0, 0)
    pdf.ln(1)


def _kv_row(pdf: Any, label: str, value: Any) -> None:
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(72, 6, _pdf_safe_text(f"  {label}:"))
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(
        0, 6,
        _pdf_safe_text(str(value) if value is not None else "N/A"),
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )


def render_audit_memo(
    audit_context: dict[str, Any],
    narrative: str,
    output_path: Path,
) -> None:
    """Render one provider anomaly record into a single-page PDF compliance memo."""
    if not FPDF_AVAILABLE:
        raise RuntimeError(
            "PDF output requires fpdf2. Install with: pip install fpdf2"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    pdf = ComplianceMemo()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=18)

    prov = audit_context["provider"]
    metrics = audit_context["billing_metrics"]
    risk = audit_context["risk_assessment"]

    # ---- Title block -------------------------------------------------- #
    pdf.set_font("Helvetica", "B", 15)
    pdf.set_text_color(*COLOR_NAVY)
    pdf.cell(
        0, 9, _pdf_safe_text("COMPLIANCE AUDIT MEMO"),
        new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C",
    )
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*COLOR_GREY)
    pdf.cell(
        0, 5,
        _pdf_safe_text(
            f"Reference Date: {datetime.now().strftime('%B %d, %Y')}  |  "
            "Source: CMS Medicare Part B Claims Data"
        ),
        new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C",
    )
    pdf.set_text_color(0, 0, 0)
    pdf.ln(5)

    # ---- Provider Identification --------------------------------------- #
    _section_heading(pdf, "PROVIDER IDENTIFICATION")
    _kv_row(pdf, "National Provider Identifier (NPI)", prov.get("npi"))
    full_name = f"{prov.get('first_name', '')} {prov.get('last_name_or_org', '')}".strip()
    _kv_row(pdf, "Provider Name", full_name or "N/A")
    _kv_row(pdf, "Specialty / Provider Type", prov.get("specialty"))
    _kv_row(pdf, "Procedure Code (HCPCS)", audit_context["procedure"].get("hcpcs_code"))
    pdf.ln(3)

    # ---- Billing Metrics ---------------------------------------------- #
    _section_heading(pdf, "BILLING METRICS vs. SPECIALTY PEER BENCHMARK")
    _kv_row(pdf, "Avg Submitted Charge",
            f"${metrics.get('avg_submitted_charge_usd', 0):,.2f}")
    _kv_row(pdf, "Avg Medicare Allowed Amount",
            f"${metrics.get('avg_medicare_allowed_amt_usd', 0):,.2f}")
    _kv_row(pdf, "Provider Markup Ratio",
            f"{metrics.get('provider_markup_ratio', 0):.2f}x")
    _kv_row(pdf, "Peer Group Baseline Ratio",
            f"{metrics.get('peer_group_markup_baseline', 0):.2f}x")
    _kv_row(pdf, "Peer Group Size",
            f"{metrics.get('peer_group_size_service_lines', 0):,} service lines")
    pdf.ln(3)

    # ---- Risk Assessment ---------------------------------------------- #
    _section_heading(pdf, "AUTOMATED RISK ASSESSMENT")
    risk_pts = risk.get("elasticity_risk_points", 0)
    risk_label = risk.get("risk_tier", "LOW")
    _kv_row(pdf, "Elasticity Risk Points", f"{risk_pts} / 35")
    _kv_row(pdf, "Risk Tier", risk_label)
    pdf.ln(3)

    # ---- AI Narrative ------------------------------------------------- #
    _section_heading(pdf, "ANALYST NARRATIVE  (AI-ASSISTED — REVIEW BEFORE DISTRIBUTION)")
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*COLOR_BLACK)
    pdf.multi_cell(
        0, 5.5, _pdf_safe_text(narrative),
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )
    pdf.ln(3)

    # ---- Recommended Actions ------------------------------------------ #
    _section_heading(pdf, "RECOMMENDED COMPLIANCE ACTIONS")
    actions = [
        "1.  Request supporting documentation for submitted charges against this HCPCS code.",
        "2.  Cross-reference against the current CMS fee schedule and local coverage determinations.",
        "3.  If markup ratio exceeds peer baseline by >40%, escalate to the compliance officer immediately.",
        "4.  Flag this NPI for inclusion in the next scheduled medical record audit cycle.",
    ]
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*COLOR_BLACK)
    for action in actions:
        pdf.multi_cell(
            0, 5.5, _pdf_safe_text(f"  {action}"),
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )

    pdf.output(str(output_path))
    logging.getLogger("cms_pdf").info("Report written: %s", output_path)


def _discover_state_databases(data_dir: Path = Path("data")) -> dict[str, Path]:
    discovered: dict[str, Path] = {}
    for db_file in data_dir.glob("cms_outliers_*.db"):
        state_key = db_file.stem.removeprefix("cms_outliers_").upper()
        if state_key and all(part.isalpha() and len(part) == 2 for part in state_key.split("_")):
            discovered[state_key] = db_file
    return discovered


def _resolve_target_db_path(db_override: Path | None = None) -> Path:
    if db_override is not None:
        return db_override

    state_dbs = _discover_state_databases()
    if state_dbs:
        return max(state_dbs.values(), key=lambda path: path.stat().st_mtime)

    return LEGACY_DB_PATH


def _state_tag_from_db_path(db_path: Path) -> str:
    stem = db_path.stem
    if stem.startswith("cms_outliers_"):
        suffix = stem.removeprefix("cms_outliers_").upper()
        return suffix.replace("_", "-")
    return "STATE-UNK"


def _timestamp_suffix(now: datetime | None = None) -> str:
    point_in_time = now or datetime.now()
    hour_12 = point_in_time.strftime("%I").lstrip("0") or "12"
    return f"{point_in_time.strftime('%Y-%m-%d')}-{hour_12}{point_in_time.strftime('%M%p')}"


def _print_terminal_summary(index: int, total: int, audit_context: dict[str, Any]) -> None:
    prov = audit_context["provider"]
    metrics = audit_context["billing_metrics"]
    risk = audit_context["risk_assessment"]
    proc = audit_context["procedure"]

    print("=" * 78)
    print(f"ANOMALY {index}/{total} | {risk.get('risk_tier', 'LOW')} RISK")
    print("=" * 78)
    provider_name = f"{prov.get('first_name', '')} {prov.get('last_name_or_org', '')}".strip()
    print(
        f"Provider: {provider_name} | NPI: {prov.get('npi')} | "
        f"Specialty: {prov.get('specialty')}"
    )
    print(f"HCPCS: {proc.get('hcpcs_code')}")
    print(
        "Charges vs Peer: "
        f"Provider {metrics.get('provider_markup_ratio', 0):.2f}x | "
        f"Peer {metrics.get('peer_group_markup_baseline', 0):.2f}x | "
        f"Peer Rows {metrics.get('peer_group_size_service_lines', 0)}"
    )
    print(
        "Amounts: "
        f"Submitted ${metrics.get('avg_submitted_charge_usd', 0):,.2f} | "
        f"Allowed ${metrics.get('avg_medicare_allowed_amt_usd', 0):,.2f}"
    )
    print(
        "Action: Prioritize documentation pull and fee-schedule validation; "
        "escalate if provider ratio remains >1.20x peer baseline."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate anomaly reports as terminal summaries and PDFs."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="How many top anomalies to report.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Explicit SQLite database path. Defaults to latest state DB.",
    )
    parser.add_argument(
        "--use-ai",
        action="store_true",
        help="Enable AI narrative generation via OpenRouter when API key is configured.",
    )
    return parser.parse_args()


def generate_report_batch(
    limit: int = 10,
    db_path: Path | None = None,
    use_ai: bool = False,
) -> None:
    """Generate terminal summaries plus one timestamped PDF per top anomaly row."""
    logger = logging.getLogger("cms_pdf")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    target_db = _resolve_target_db_path(db_path)
    state_tag = _state_tag_from_db_path(target_db)
    logger.info("Report target database: %s", target_db.resolve())
    logger.info("Report state scope tag: %s", state_tag)

    anomalies = fetch_top_anomalies(limit=limit, db_path=target_db)

    if not anomalies:
        logger.warning("No anomalies found — ensure dim_benchmarks is populated.")
        return

    if not FPDF_AVAILABLE:
        logger.warning(
            "fpdf2 is not installed. Terminal findings will be shown, but PDF files will be skipped."
        )

    run_stamp = _timestamp_suffix()
    has_api_key = bool(os.getenv("OPENROUTER_API_KEY"))
    ai_enabled = use_ai and has_api_key
    if use_ai and not has_api_key:
        logger.warning("--use-ai supplied but OPENROUTER_API_KEY is not set; using deterministic narrative.")

    for i, row in enumerate(anomalies, 1):
        context = build_audit_context(row)
        _print_terminal_summary(i, len(anomalies), context)

        narrative = _deterministic_narrative(context)
        if ai_enabled:
            try:
                narrative = generate_audit_narrative(context)
            except Exception as exc:
                logger.warning(
                    "AI narrative unavailable for anomaly %d; using deterministic fallback. Error: %s",
                    i,
                    exc,
                )

        file_name = f"{state_tag}-{run_stamp}-{i:02d}.pdf"
        out_path = REPORTS_DIR / file_name
        if FPDF_AVAILABLE:
            render_audit_memo(context, narrative, out_path)
            logger.info("Generated report %d/%d: %s", i, len(anomalies), out_path.name)

    logger.info("Batch complete — %d reports written to %s/", len(anomalies), REPORTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    cli_args = parse_args()
    generate_report_batch(
        limit=cli_args.limit,
        db_path=cli_args.db,
        use_ai=cli_args.use_ai,
    )
