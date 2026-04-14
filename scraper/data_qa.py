"""
Data QA Engine — Pre-pipeline source reconciliation

Runs between Stage 1 (scrape) and Stage 2 (standardise) in run.py.

For each field/year in the reconciled dataset, gathers values from all
available sources and applies the authority hierarchy:

    EDGAR  >  Yahoo Finance  >  Macrotrends

If a higher-authority source disagrees with the current reconciled value
by more than _THRESHOLD (5%), the reconciled value is overridden and the
correction is logged.

EDGAR corrections are only possible for the EDGAR latest-year annual figure.
Macrotrends fills historical years, so corrections there primarily come from
cross-checking against Yahoo where both cover the same year.

Output:
    The original data dict is returned with two fields updated/added:
      data["reconciled"]   — corrected reconciled dataset
      data["data_qa_log"]  — list of correction records

Each correction record:
    {
      "field":             str,          e.g. "revenue"
      "year":              str,          e.g. "2022"
      "original_value":    float,
      "original_source":   str,          e.g. "macrotrends"
      "corrected_value":   float,
      "corrected_source":  str,          e.g. "yahoo"
      "pct_diff":          float,        percentage difference
      "reason":            str,
    }
"""

from __future__ import annotations

import copy
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Minimum percentage disagreement before a correction is applied
_THRESHOLD = 5.0   # %

# Source authority order (index 0 = highest authority)
_AUTHORITY_ORDER = ["edgar", "yahoo", "macrotrends"]

# Fields eligible for cross-source comparison.
# Only fields that are reported by at least two sources in compatible units.
_COMPARABLE_FIELDS = [
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "operating_cash_flow",
    "capex",
    "total_assets",
    "total_debt",
    "cash_and_equivalents",
    "ebitda",
]


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_data_qa(data: dict) -> dict:
    """
    Apply source authority rules to correct the reconciled dataset.

    Parameters
    ----------
    data : dict returned by run_data_engine — must contain "reconciled" and "raw".

    Returns
    -------
    Updated data dict with corrected "reconciled" and new "data_qa_log" key.
    The input dict is not mutated.
    """
    reconciled = data.get("reconciled", {})
    raw        = data.get("raw", {})

    if not reconciled or not raw:
        out = dict(data)
        out["data_qa_log"] = []
        return out

    # ── Extract per-source data ───────────────────────────────────────────────
    yahoo_by_yr = (raw.get("yahoo") or {}).get("financials_by_year", {})

    edgar_info  = raw.get("edgar") or {}
    edgar_fin   = edgar_info.get("financials", {}) if edgar_info.get("available") else {}
    edgar_yr    = str(edgar_info.get("latest_year") or "")

    mt_info    = raw.get("macrotrends") or {}
    mt_by_yr   = mt_info.get("financials_by_year", {}) if mt_info.get("available") else {}

    # ── Walk every year × field in reconciled ────────────────────────────────
    updated = copy.deepcopy(reconciled)
    log: list[dict] = []

    for year, yr_data in updated.items():
        sources = yr_data.get("_sources", {})

        for field in _COMPARABLE_FIELDS:
            current_val = yr_data.get(field)
            if current_val is None:
                continue

            current_src = sources.get(field, "yahoo")

            # Gather all available source values for this field/year
            source_vals: dict[str, float] = {}

            yf_val = _safe(yahoo_by_yr.get(year, {}).get(field))
            if yf_val is not None:
                source_vals["yahoo"] = yf_val

            if year == edgar_yr:
                ed_val = _safe(edgar_fin.get(field))
                if ed_val is not None:
                    source_vals["edgar"] = ed_val

            mt_val = _safe(mt_by_yr.get(year, {}).get(field))
            if mt_val is not None:
                source_vals["macrotrends"] = mt_val

            if len(source_vals) < 2:
                continue   # can't compare without at least two sources

            # Determine the highest-authority source available
            best_src = None
            best_val = None
            for src in _AUTHORITY_ORDER:
                if src in source_vals:
                    best_src = src
                    best_val = source_vals[src]
                    break

            if best_src is None or best_val is None:
                continue

            # Current value already from the best available source — nothing to do
            if current_src == best_src:
                continue

            # Skip zero / near-zero denominators
            if abs(current_val) < 1:
                continue

            pct_diff = abs(best_val - current_val) / abs(current_val) * 100

            if pct_diff > _THRESHOLD:
                log.append({
                    "field":           field,
                    "year":            year,
                    "original_value":  current_val,
                    "original_source": current_src,
                    "corrected_value": best_val,
                    "corrected_source": best_src,
                    "pct_diff":        round(pct_diff, 1),
                    "reason": (
                        f"{best_src.title()} (higher authority) reports "
                        f"{_fmt(best_val)} vs {current_src}'s {_fmt(current_val)} "
                        f"({pct_diff:.1f}% diff) — {best_src} value applied."
                    ),
                })
                updated[year][field]              = best_val
                updated[year]["_sources"][field]  = best_src

    out                = dict(data)
    out["reconciled"]  = updated
    out["data_qa_log"] = log
    return out


def print_data_qa_report(data: dict) -> None:
    """Print a summary of data QA corrections to the terminal."""
    log = data.get("data_qa_log", [])

    print()
    print("=" * 60)
    print("  Data QA  —  Pre-pipeline Source Reconciliation")
    print("=" * 60)

    if not log:
        print("  No corrections required — all sources agree within "
              f"{_THRESHOLD:.0f}% tolerance.\n")
        return

    print(f"  {len(log)} correction(s) applied  "
          f"(threshold: >{_THRESHOLD:.0f}% disagreement)\n")

    for c in log:
        arrow = f"{_fmt(c['original_value'])}  →  {_fmt(c['corrected_value'])}"
        print(f"  [{c['corrected_source'].upper()} override]  "
              f"{c['field']}  ({c['year']})   {arrow}   "
              f"({c['pct_diff']:.1f}% diff from {c['original_source']})")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f   # NaN → None
    except (TypeError, ValueError):
        return None


def _fmt(v: float | None) -> str:
    if v is None:
        return "—"
    try:
        v = float(v)
        if abs(v) >= 1e12:
            return f"{v/1e12:.2f}T"
        if abs(v) >= 1e9:
            return f"{v/1e9:.2f}B"
        if abs(v) >= 1e6:
            return f"{v/1e6:.0f}M"
        return f"{v:,.2f}"
    except (TypeError, ValueError):
        return str(v)
