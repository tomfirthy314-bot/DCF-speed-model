"""
Sprint 1A — Standardiser

Purpose:
    Convert the raw reconciled scraper output into a single canonical schema.
    Every field gets a consistent name, a source tag, and a mapping confidence
    level. Unmapped fields are logged for review. Graduation is gated.

Inputs:
    scraper output dict from data_engine.run_data_engine()

Outputs:
    {
        "status":              "pass" | "pass_with_caution" | "fail",
        "blockers":            [...],
        "warnings":            [...],
        "ticker":              str,
        "company_name":        str,
        "stats":               {...},         # pass-through from scraper
        "canonical_by_year":   {year: {...}}, # all canonical fields + _sources + _mapping_confidence
        "field_mapping_table": [...],         # audit trail of every mapping decision
        "unmapped_fields":     [...],         # raw fields with no canonical mapping
        "years_available":     [...],
        "metadata":            {...},
    }

Canonical field names (all lowercase snake_case):
    Income:    revenue, gross_profit, ebit, ebitda, da, pre_tax_income,
               tax_provision, net_income, interest_expense
    Cash flow: operating_cash_flow, capex, free_cash_flow, change_in_working_cap
    Balance:   total_assets, current_assets, accounts_receivable, inventory,
               current_liabilities, accounts_payable, deferred_revenue,
               debt, long_term_debt, lease_liabilities,
               cash, total_equity, retained_earnings
    Other:     shares_outstanding, eps_diluted, net_profit_margin_pct

Graduation logic:
    pass               — all hard blockers present, no warnings
    pass_with_caution  — all hard blockers present, some warnings
    fail               — one or more hard blockers missing
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Field map: raw_field -> (canonical_name, confidence)
#
# confidence:
#   "high"   — direct, unambiguous mapping (same concept, different label)
#   "medium" — proxy mapping (close but not identical concept)
#   "low"    — rough approximation only
# ---------------------------------------------------------------------------

FIELD_MAP: dict[str, tuple[str, str]] = {
    # Income statement
    "revenue":               ("revenue",               "high"),
    "gross_profit":          ("gross_profit",          "high"),
    # "ebit" intentionally not mapped — derived exclusively by canonical_fundamentals
    # "operating_income" kept as its own field so derivation methods can use it as a primitive
    "operating_income":      ("operating_income",       "high"),
    "ebitda":                ("ebitda",                "high"),
    "pre_tax_income":        ("pre_tax_income",         "high"),
    "tax_provision":         ("tax_provision",          "high"),
    "net_income":            ("net_income",             "high"),
    "interest_expense":      ("interest_expense",       "high"),

    # Cash flow
    "operating_cash_flow":   ("operating_cash_flow",   "high"),
    "capex":                 ("capex",                 "high"),
    "free_cash_flow":        ("free_cash_flow",        "high"),
    "depreciation_amort":    ("da",                    "high"),
    "change_in_working_cap": ("change_in_working_cap", "high"),

    # Balance sheet — assets
    "total_assets":          ("total_assets",          "high"),
    "current_assets":        ("current_assets",        "high"),
    "accounts_receivable":   ("accounts_receivable",   "high"),
    "inventory":             ("inventory",             "high"),
    "cash_and_equivalents":  ("cash",                  "high"),

    # Balance sheet — liabilities
    "current_liabilities":   ("current_liabilities",   "high"),
    "accounts_payable":      ("accounts_payable",      "high"),
    "deferred_revenue":      ("deferred_revenue",      "high"),
    "total_debt":            ("debt",                  "high"),
    "long_term_debt":        ("long_term_debt",        "high"),
    "lease_liabilities":     ("lease_liabilities",     "high"),

    # Balance sheet — equity
    "total_equity":          ("total_equity",          "high"),
    "retained_earnings":     ("retained_earnings",     "high"),

    # Other / derived
    "shares_outstanding":    ("shares_outstanding",    "high"),
    "eps_diluted":           ("eps_diluted",           "high"),
    "net_profit_margin_pct": ("net_profit_margin_pct", "high"),
}

# Internal metadata keys in the reconciled dict — skip these
_INTERNAL_FIELDS = {"_sources"}

# Hard blockers: if absent from ALL years, graduation fails
# Note: "ebit" and "free_cash_flow" are NOT listed here — they are derived
# by canonical_fundamentals (Stage 2.5) which runs after this stage.
# Their presence is checked downstream in the validator after derivation.
_HARD_BLOCKER_FIELDS = [
    "revenue",
    "capex",
    "cash",
    "debt",
    "shares_outstanding",
]

# Soft requirements: absent triggers a warning, not a blocker
_SOFT_REQUIRED_FIELDS = [
    "gross_profit",
    "ebitda",
    "da",
    "net_income",
    "operating_cash_flow",
    "free_cash_flow",
    "accounts_receivable",
    "accounts_payable",
    "total_assets",
    "total_equity",
]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_standardiser(scraper_output: dict) -> dict:
    """
    Standardise the full scraper output into a canonical dataset.
    Returns a structured dict with status, canonical data, and audit trail.
    """
    ticker       = scraper_output.get("ticker", "")
    company_name = scraper_output.get("company_name", "")
    stats        = scraper_output.get("stats", {})
    reconciled   = scraper_output.get("reconciled", {})

    canonical_by_year: dict[str, dict] = {}
    field_mapping_table: list[dict]    = []
    unmapped_raw_fields: set[str]      = set()

    # Shares outstanding from stats — used as fallback if not in year data
    stats_shares = stats.get("shares_outstanding")

    # -------------------------------------------------------------------------
    # Map each year
    # -------------------------------------------------------------------------
    for year in sorted(reconciled.keys(), reverse=True):
        raw_year = reconciled[year]
        sources  = raw_year.get("_sources", {})

        canon_year: dict = {
            "_sources":             {},
            "_mapping_confidence":  {},
        }

        for raw_field, raw_val in raw_year.items():
            if raw_field in _INTERNAL_FIELDS:
                continue

            if raw_field not in FIELD_MAP:
                unmapped_raw_fields.add(raw_field)
                continue

            canon_name, confidence = FIELD_MAP[raw_field]
            source = sources.get(raw_field, "unknown")

            # EBIT conflict resolution:
            # If we already have a high-confidence "ebit" (from the "ebit" raw field),
            # do not overwrite it with a medium-confidence "operating_income" proxy.
            if canon_name == "ebit" and "ebit" in canon_year:
                existing_conf = canon_year["_mapping_confidence"].get("ebit", "")
                if existing_conf == "high" and confidence == "medium":
                    field_mapping_table.append({
                        "raw_field":       raw_field,
                        "canonical_field": canon_name,
                        "year":            year,
                        "source":          source,
                        "confidence":      confidence,
                        "value":           raw_val,
                        "note":            "skipped — superseded by ebit (high confidence)",
                    })
                    continue

            if raw_val is not None:
                canon_year[canon_name]                       = raw_val
                canon_year["_sources"][canon_name]           = source
                canon_year["_mapping_confidence"][canon_name] = confidence

            field_mapping_table.append({
                "raw_field":       raw_field,
                "canonical_field": canon_name,
                "year":            year,
                "source":          source,
                "confidence":      confidence,
                "value":           raw_val,
            })

        # Backfill shares_outstanding from stats if not present in this year
        if "shares_outstanding" not in canon_year and stats_shares is not None:
            canon_year["shares_outstanding"]                          = stats_shares
            canon_year["_sources"]["shares_outstanding"]              = "yahoo_stats"
            canon_year["_mapping_confidence"]["shares_outstanding"]   = "medium"
            field_mapping_table.append({
                "raw_field":       "shares_outstanding (point-in-time stats)",
                "canonical_field": "shares_outstanding",
                "year":            year,
                "source":          "yahoo_stats",
                "confidence":      "medium",
                "value":           stats_shares,
                "note":            "backfilled from current stats — not period-specific",
            })

        canonical_by_year[year] = canon_year

    # -------------------------------------------------------------------------
    # Graduation checks
    # -------------------------------------------------------------------------
    blockers: list[str] = []
    warnings: list[str] = []

    # Hard blockers — must appear in at least one year
    for field in _HARD_BLOCKER_FIELDS:
        present = any(field in yr for yr in canonical_by_year.values())
        if not present:
            blockers.append(
                f"BLOCKER: '{field}' is missing from all years — "
                "required for DCF, cannot proceed to validation"
            )

    # Soft requirements — warn if absent
    for field in _SOFT_REQUIRED_FIELDS:
        present = any(field in yr for yr in canonical_by_year.values())
        if not present:
            warnings.append(
                f"WARNING: '{field}' not found in any year — "
                "lower confidence for downstream normalisation"
            )

    # EBIT proxy warning
    ebit_proxy_years = [
        y for y, d in canonical_by_year.items()
        if d.get("_mapping_confidence", {}).get("ebit") == "medium"
    ]
    if ebit_proxy_years:
        warnings.append(
            f"WARNING: EBIT sourced from 'operating_income' proxy in "
            f"{sorted(ebit_proxy_years)} — confirm this is not adjusted EBIT or EBITDA"
        )

    # Year coverage
    n_years = len(canonical_by_year)
    if n_years < 3:
        warnings.append(
            f"WARNING: Only {n_years} year(s) of history — "
            "at least 3 years recommended for trend-based normalisation"
        )

    # Check for fields with conflicting values not resolved (unmapped raw fields)
    if unmapped_raw_fields:
        warnings.append(
            f"WARNING: {len(unmapped_raw_fields)} raw field(s) had no canonical mapping "
            f"and were logged for review: {sorted(unmapped_raw_fields)}"
        )

    # Determine overall status
    if blockers:
        status = "fail"
    elif warnings:
        status = "pass_with_caution"
    else:
        status = "pass"

    return {
        "status":              status,
        "blockers":            blockers,
        "warnings":            warnings,
        "ticker":              ticker,
        "company_name":        company_name,
        "stats":               stats,
        "canonical_by_year":   canonical_by_year,
        "field_mapping_table": field_mapping_table,
        "unmapped_fields":     sorted(unmapped_raw_fields),
        "years_available":     sorted(canonical_by_year.keys(), reverse=True),
        "metadata": {
            "standardised_at": datetime.utcnow().isoformat() + "Z",
            "source_years":    sorted(reconciled.keys(), reverse=True),
            "n_years":         n_years,
        },
    }


# ---------------------------------------------------------------------------
# Terminal output helper
# ---------------------------------------------------------------------------

def print_standardiser_report(result: dict) -> None:
    """Print a formatted standardiser report to stdout."""
    status  = result["status"]
    ticker  = result["ticker"]
    company = result["company_name"]
    years   = result["years_available"]

    status_icon = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}
    print(f"\n{'='*60}")
    print(f"  Standardiser — {company} ({ticker})")
    print(f"  Status : [{status_icon.get(status, status)}]")
    print(f"  Years  : {years}")
    print(f"{'='*60}")

    if result["blockers"]:
        print("\n  Blockers (must resolve before validation):")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["warnings"]:
        print("\n  Warnings:")
        for w in result["warnings"]:
            print(f"    {w}")

    # Field mapping summary — show canonical fields present in latest year
    latest_year = years[0] if years else None
    if latest_year:
        latest = result["canonical_by_year"][latest_year]
        data_fields = {k: v for k, v in latest.items() if not k.startswith("_")}
        print(f"\n  Canonical fields present in {latest_year}: {len(data_fields)}")
        for field, val in sorted(data_fields.items()):
            src  = latest["_sources"].get(field, "?")
            conf = latest["_mapping_confidence"].get(field, "?")
            print(f"    {field:<30} {_fmt(val):<14} [{src} / {conf}]")

    if result["unmapped_fields"]:
        print(f"\n  Unmapped raw fields (logged, not included):")
        for f in result["unmapped_fields"]:
            print(f"    {f}")

    print(f"\n  Mapping table entries : {len(result['field_mapping_table'])}")
    print(f"  Standardised at       : {result['metadata']['standardised_at']}")
    print(f"{'='*60}\n")


def _fmt(val) -> str:
    if val is None:
        return "—"
    try:
        val = float(val)
    except (TypeError, ValueError):
        return str(val)
    if abs(val) >= 1e12:
        return f"{val/1e12:.2f}T"
    if abs(val) >= 1e9:
        return f"{val/1e9:.2f}B"
    if abs(val) >= 1e6:
        return f"{val/1e6:.0f}M"
    if abs(val) < 100:
        return f"{val:.4f}"
    return f"{val:,.0f}"
