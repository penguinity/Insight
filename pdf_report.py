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

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from fpdf import FPDF, XPos, YPos

from ai_reporter import build_audit_context, fetch_top_anomalies, generate_audit_narrative

REPORTS_DIR = Path("reports")

# Brand palette
COLOR_NAVY = (0, 51, 102)
COLOR_LIGHT_BLUE = (230, 240, 255)
COLOR_GREY = (128, 128, 128)
COLOR_BLACK = (30, 30, 30)


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
            "CONFIDENTIAL — For Internal Compliance Use Only  |  "
            f"Page {self.page_no()}",
            align="C",
        )


def _section_heading(pdf: ComplianceMemo, title: str) -> None:
    pdf.set_fill_color(*COLOR_LIGHT_BLUE)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*COLOR_NAVY)
    pdf.cell(
        0, 7, f"  {title}",
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        fill=True,
    )
    pdf.set_text_color(0, 0, 0)
    pdf.ln(1)


def _kv_row(pdf: ComplianceMemo, label: str, value: Any) -> None:
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(72, 6, f"  {label}:")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(
        0, 6,
        str(value) if value is not None else "N/A",
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )


def render_audit_memo(
    audit_context: dict[str, Any],
    narrative: str,
    output_path: Path,
) -> None:
    """Render one provider anomaly record into a single-page PDF compliance memo."""
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
        0, 9, "COMPLIANCE AUDIT MEMO",
        new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C",
    )
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*COLOR_GREY)
    pdf.cell(
        0, 5,
        f"Reference Date: {datetime.now().strftime('%B %d, %Y')}  |  "
        "Source: CMS Medicare Part B Claims Data",
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
        0, 5.5, narrative,
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
            0, 5.5, f"  {action}",
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )

    pdf.output(str(output_path))
    logging.getLogger("cms_pdf").info("Report written: %s", output_path)


def generate_report_batch(limit: int = 10) -> None:
    """Generate one PDF memo per top anomaly row, ordered by markup ratio."""
    logger = logging.getLogger("cms_pdf")
    anomalies = fetch_top_anomalies(limit=limit)

    if not anomalies:
        logger.warning("No anomalies found — ensure dim_benchmarks is populated.")
        return

    for i, row in enumerate(anomalies, 1):
        npi = row.get("Rndrg_Npi", f"unknown_{i}")
        hcpcs = row.get("Hcpcs_Cd", "X")
        context = build_audit_context(row)
        narrative = generate_audit_narrative(context)
        out_path = REPORTS_DIR / f"audit_memo_{npi}_{hcpcs}.pdf"
        render_audit_memo(context, narrative, out_path)
        logger.info("Generated report %d/%d: %s", i, len(anomalies), out_path.name)

    logger.info("Batch complete — %d reports written to %s/", len(anomalies), REPORTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    generate_report_batch(limit=5)
