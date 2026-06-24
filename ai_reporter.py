"""
Insight — CMS Part B Provider Compliance Analytics Engine
AI Translation Layer: packages anomaly rows into structured audit context
and calls OpenRouter to generate human-readable compliance narratives.

Requires:
    pip install requests python-dotenv
    OPENROUTER_API_KEY set in .env

Run standalone to preview one audit narrative:
    python ai_reporter.py
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any

try:
    import requests
except ModuleNotFoundError:
    requests = None

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

from schema_router import reconcile_database_objects

if load_dotenv is not None:
    load_dotenv()

DB_PATH = Path("data/cms_outliers.db")
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# google/gemini-flash-1.5 offers the best cost-to-reasoning tradeoff on OpenRouter
# for structured healthcare data summarization: ~$0.075/M input tokens.
# Alternative high-reasoning options:
#   "meta-llama/llama-3.1-70b-instruct"  — strong reasoning, open weights
#   "anthropic/claude-3-haiku"            — fast, cheap, very coherent prose
DEFAULT_MODEL = "google/gemini-flash-1.5"

# Required by OpenRouter to identify your application.
SITE_URL = "https://github.com/penguinity/insight"
SITE_NAME = "Insight CMS Compliance Engine"


def build_audit_context(row: dict[str, Any]) -> dict[str, Any]:
    """Serialize one anomaly view row into a structured JSON audit context.

    This format is intentionally verbose so the AI model has full numeric
    context without needing to interpret raw column names.
    """
    return {
        "audit_type": "Medicare Part B — Billing Elasticity Anomaly",
        "provider": {
            "npi": row.get("Rndrg_Npi"),
            "last_name_or_org": row.get("Rndrg_Prvdr_Last_Org_Name"),
            "first_name": row.get("Rndrg_Prvdr_First_Name"),
            "specialty": row.get("Rndrg_Prvdr_Type"),
        },
        "procedure": {
            "hcpcs_code": row.get("Hcpcs_Cd"),
        },
        "billing_metrics": {
            "avg_submitted_charge_usd": row.get("Avg_Srvc_Smtd_Chrg"),
            "avg_medicare_allowed_amt_usd": row.get("Avg_Medcr_Alwd_Amt"),
            "provider_markup_ratio": row.get("provider_markup_ratio"),
            "peer_group_markup_baseline": row.get("peer_group_markup_baseline"),
            "peer_group_size_service_lines": row.get("peer_group_row_count"),
        },
        "risk_assessment": {
            "elasticity_risk_points": row.get("elasticity_risk_points"),
            "risk_tier": (
                "HIGH" if (row.get("elasticity_risk_points") or 0) >= 35
                else "MODERATE" if (row.get("elasticity_risk_points") or 0) >= 20
                else "LOW"
            ),
        },
    }


def generate_audit_narrative(
    audit_context: dict[str, Any],
    api_key: str | None = None,
    model: str = DEFAULT_MODEL,
) -> str:
    """Send audit context to OpenRouter and return a narrative compliance memo string.

    Falls back to a formatted JSON summary when no API key is configured
    so the rest of the pipeline (PDF generation etc.) still functions.
    """
    api_key = api_key or os.getenv("OPENROUTER_API_KEY")

    if not api_key:
        logging.warning(
            "OPENROUTER_API_KEY not set — returning structured JSON summary. "
            "Add the key to your .env file to enable AI narratives."
        )
        return _format_fallback_narrative(audit_context)

    if requests is None:
        raise RuntimeError(
            "AI narrative generation requires the 'requests' package. "
            "Install it with: pip install requests"
        )

    prompt = (
        "You are a senior Medicare compliance analyst reviewing CMS Part B claims data. "
        "Below is a structured audit finding from an automated outlier detection system.\n\n"
        "Write a professional, concise compliance audit memo of exactly 3 paragraphs (max 200 words total):\n"
        "  Paragraph 1: Summarize the anomaly in plain language.\n"
        "  Paragraph 2: Explain why the markup ratio deviation is clinically and financially significant.\n"
        "  Paragraph 3: Recommend one specific, actionable compliance step.\n\n"
        "Write in formal third-person. Do not include headers or bullet points.\n\n"
        f"Audit Data:\n{json.dumps(audit_context, indent=2)}"
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": SITE_URL,
        "X-Title": SITE_NAME,
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 450,
        "temperature": 0.25,
    }

    try:
        response = requests.post(
            OPENROUTER_API_URL,
            headers=headers,
            json=payload,
            timeout=45,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()
    except requests.RequestException as exc:
        logging.exception("OpenRouter API call failed")
        raise RuntimeError("AI narrative generation failed") from exc


def _format_fallback_narrative(audit_context: dict[str, Any]) -> str:
    """Return a plain-text summary when the AI API is unavailable."""
    prov = audit_context["provider"]
    metrics = audit_context["billing_metrics"]
    risk = audit_context["risk_assessment"]
    return (
        f"Provider {prov.get('npi')} ({prov.get('specialty')}) submitted charges for "
        f"HCPCS {audit_context['procedure'].get('hcpcs_code')} at a markup ratio of "
        f"{metrics.get('provider_markup_ratio')}x against a peer group baseline of "
        f"{metrics.get('peer_group_markup_baseline')}x "
        f"({metrics.get('peer_group_size_service_lines')} peer service lines). "
        f"Risk tier: {risk.get('risk_tier')} ({risk.get('elasticity_risk_points')} points). "
        f"Average submitted charge: ${metrics.get('avg_submitted_charge_usd', 0):,.2f}. "
        f"Average Medicare allowed amount: ${metrics.get('avg_medicare_allowed_amt_usd', 0):,.2f}. "
        "Recommend documentation review and comparison against CMS fee schedule."
    )


def fetch_top_anomalies(
    limit: int = 10,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    """Fetch the highest-markup anomaly rows from the billing elasticity view."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    with conn:
        reconcile_database_objects(conn)
    rows = conn.execute(
        "SELECT * FROM v_billing_elasticity_anomalies "
        "ORDER BY provider_markup_ratio DESC "
        f"LIMIT {limit}"
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    anomalies = fetch_top_anomalies(limit=1)
    if not anomalies:
        print("No anomalies found — ensure dim_benchmarks is populated.")
    else:
        context = build_audit_context(anomalies[0])
        print(json.dumps(context, indent=2))
        narrative = generate_audit_narrative(context)
        print("\n--- AUDIT NARRATIVE ---\n")
        print(narrative)
