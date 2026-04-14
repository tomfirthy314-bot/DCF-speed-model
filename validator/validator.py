"""
Sprint 1B — Validator

Purpose:
    Test whether the standardised canonical data is coherent enough to support
    modelling. Runs a battery of checks across presence, sign, magnitude, and
    period integrity. Produces a validation report with a data quality score.

Inputs:
    standardised output dict from standardiser.run_standardiser()

Outputs:
    {
        "status":        "pass" | "pass_with_caution" | "fail",
        "quality_score": float (0–100),
        "passed":        [...],   # checks that passed
        "failed":        [...],   # checks that failed
        "warnings":      [...],   # non-critical anomalies
        "blockers":      [...],   # critical issues that must be resolved
        "manual_review": [...],   # items flagged for analyst attention
        "ticker":        str,
        "company_name":  str,
        "metadata":      {...},
    }

Check categories:
    Presence   — required fields exist in at least one year
    Sign       — values have the expected sign (e.g. capex ≤ 0, debt ≥ 0)
    Magnitude  — values are within plausible ranges (e.g. margins, growth rates)
    Coherence  — values are internally consistent (e.g. FCF ≈ CFO + capex)
    Period     — years are valid, ordered, and non-duplicate

Quality score thresholds:
    ≥ 75  →  pass
    ≥ 50  →  pass_with_caution
    < 50  →  fail   (also fail if any blocker exists)

Sign convention (Yahoo Finance / standard financial statement convention):
    capex           ≤ 0   (cash outflow, reported as negative)
    operating_cf    can be positive or negative
    free_cash_flow  can be positive or negative
    da              > 0   (non-cash charge added back)
    debt            ≥ 0
    cash            ≥ 0
    shares          > 0
    revenue         > 0   (negative revenue is a hard flag)
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Check definitions
# Each check has: name, description, weight, critical flag
#
# Weights determine contribution to the quality score.
#   critical=True  → failure creates a blocker and caps score at fail level
#   critical=False → failure creates a warning or failed check but not a blocker
# ---------------------------------------------------------------------------

_CURRENT_YEAR = datetime.utcnow().year

# Implausibility thresholds
_MAX_PLAUSIBLE_REVENUE_GROWTH = 2.0   # 200% YoY — above this, flag as suspect
_MIN_PLAUSIBLE_REVENUE_GROWTH = -0.8  # -80% YoY
_MAX_EBIT_MARGIN = 0.80               # 80% EBIT margin — above this, flag
_MIN_EBIT_MARGIN = -1.00              # -100% EBIT margin — below this, flag
_MAX_CAPEX_TO_REVENUE = 0.60          # capex > 60% of revenue is unusual
_FCF_TOLERANCE = 0.20                 # FCF vs (CFO + capex) — allow 20% divergence


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_validator(standardised: dict) -> dict:
    """
    Run all validation checks against the standardised dataset.
    Returns a structured validation report.
    """
    ticker       = standardised.get("ticker", "")
    company_name = standardised.get("company_name", "")
    years_data   = standardised.get("canonical_by_year", {})
    years        = sorted(years_data.keys(), reverse=True)  # most recent first

    passed:        list[dict] = []
    failed:        list[dict] = []
    warnings:      list[str]  = []
    blockers:      list[str]  = []
    manual_review: list[str]  = []

    total_weight  = 0
    passed_weight = 0

    def _check(name: str, description: str, weight: int, critical: bool,
                result: bool, failure_msg: str, warning_msg: str = ""):
        """Record the outcome of a single check."""
        nonlocal total_weight, passed_weight
        total_weight += weight
        if result:
            passed_weight += weight
            passed.append({"check": name, "description": description})
        else:
            if critical:
                blockers.append(f"BLOCKER [{name}]: {failure_msg}")
                failed.append({"check": name, "description": description,
                               "critical": True, "detail": failure_msg})
            else:
                msg = warning_msg or failure_msg
                warnings.append(f"WARNING [{name}]: {msg}")
                failed.append({"check": name, "description": description,
                               "critical": False, "detail": msg})

    # =========================================================================
    # PRESENCE CHECKS — required fields must exist in at least one year
    # =========================================================================

    def _present(field):
        return any(field in years_data[y] for y in years)

    _check("presence_revenue",   "Revenue present",           15, True,
           _present("revenue"),
           "Revenue is missing from all years — cannot build a DCF")

    # EBIT is derived by canonical_fundamentals (Stage 2.5) which runs before
    # validation, so it should be present. Non-critical because derivation can
    # fail gracefully — the assumption engine will handle a missing EBIT.
    _check("presence_ebit",      "EBIT present",              12, False,
           _present("ebit"),
           "EBIT is missing from all years — canonical_fundamentals derivation may have failed")

    _check("presence_capex",     "Capex present",             10, True,
           _present("capex"),
           "Capex missing from all years — required for free cash flow calculation")

    _check("presence_cash",      "Cash present",              10, True,
           _present("cash"),
           "Cash missing from all years — required for equity bridge")

    _check("presence_debt",      "Debt present",              10, True,
           _present("debt"),
           "Debt missing from all years — required for equity bridge")

    _check("presence_shares",    "Shares outstanding present", 10, True,
           _present("shares_outstanding"),
           "Shares outstanding missing — cannot calculate per-share value")

    _check("presence_cfo",       "Operating cash flow present", 6, False,
           _present("operating_cash_flow"),
           "Operating cash flow missing — FCF calculation will rely on EBIT only")

    _check("presence_da",        "D&A present",                6, False,
           _present("da"),
           "D&A missing — EBITDA and FCFF calculations will be impaired")

    _check("presence_net_income","Net income present",         4, False,
           _present("net_income"),
           "Net income missing — limited earnings-based cross-checks")

    _check("presence_ar",        "Accounts receivable present", 3, False,
           _present("accounts_receivable"),
           "Accounts receivable missing — working capital analysis limited")

    _check("presence_ap",        "Accounts payable present",   3, False,
           _present("accounts_payable"),
           "Accounts payable missing — working capital analysis limited")

    _check("presence_equity",    "Total equity present",       3, False,
           _present("total_equity"),
           "Total equity missing — leverage and return ratios limited")

    # =========================================================================
    # PERIOD CHECKS — years are valid integers, ordered, non-duplicate
    # =========================================================================

    int_years = []
    invalid_years = []
    for y in years:
        try:
            iy = int(y)
            if 2000 <= iy <= _CURRENT_YEAR + 1:
                int_years.append(iy)
            else:
                invalid_years.append(y)
        except ValueError:
            invalid_years.append(y)

    _check("period_valid_years", "All period labels are valid years",  4, False,
           len(invalid_years) == 0,
           f"Period labels outside expected range or non-numeric: {invalid_years}")

    has_duplicates = len(int_years) != len(set(int_years))
    _check("period_no_duplicates", "No duplicate fiscal years",        6, False,
           not has_duplicates,
           "Duplicate fiscal year labels detected — period structure is ambiguous")

    _check("period_coverage", "At least 3 years of history",           4, False,
           len(int_years) >= 3,
           f"Only {len(int_years)} year(s) available — 3+ recommended for trend analysis")

    if len(int_years) >= 2:
        sorted_ints = sorted(int_years, reverse=True)
        gaps = [sorted_ints[i] - sorted_ints[i+1] for i in range(len(sorted_ints)-1)]
        has_gaps = any(g > 1 for g in gaps)
        _check("period_no_gaps", "No gaps in annual period sequence",  3, False,
               not has_gaps,
               f"Gap(s) detected in fiscal year sequence: {sorted_ints} — "
               "missing years may distort trend calculations")

    # =========================================================================
    # SIGN CHECKS — values have the expected sign
    # =========================================================================

    def _all_values(field):
        """Return all non-None values for a field across all years."""
        return [years_data[y][field] for y in years
                if field in years_data[y] and years_data[y][field] is not None]

    if _present("revenue"):
        rev_vals = _all_values("revenue")
        negative_rev = [v for v in rev_vals if v < 0]
        _check("sign_revenue", "Revenue is non-negative",               5, False,
               len(negative_rev) == 0,
               f"Revenue reported as negative in {len(negative_rev)} year(s) — "
               "verify sign convention and units")

    if _present("debt"):
        debt_vals = _all_values("debt")
        negative_debt = [v for v in debt_vals if v < 0]
        _check("sign_debt", "Debt is non-negative",                     4, False,
               len(negative_debt) == 0,
               f"Debt reported as negative in {len(negative_debt)} year(s) — likely a data error")

    if _present("cash"):
        cash_vals = _all_values("cash")
        negative_cash = [v for v in cash_vals if v < 0]
        _check("sign_cash", "Cash is non-negative",                     4, False,
               len(negative_cash) == 0,
               f"Cash reported as negative in {len(negative_cash)} year(s) — verify data")

    if _present("shares_outstanding"):
        share_vals = _all_values("shares_outstanding")
        bad_shares = [v for v in share_vals if v <= 0]
        _check("sign_shares", "Shares outstanding is positive",         5, False,
               len(bad_shares) == 0,
               f"Shares outstanding ≤ 0 in {len(bad_shares)} year(s) — data error")

    if _present("da"):
        da_vals = _all_values("da")
        negative_da = [v for v in da_vals if v < 0]
        _check("sign_da", "D&A is positive (non-cash charge)",          3, False,
               len(negative_da) == 0,
               f"D&A reported as negative in {len(negative_da)} year(s) — "
               "check whether sign convention matches capex")

    if _present("capex"):
        capex_vals = _all_values("capex")
        positive_capex = [v for v in capex_vals if v > 0]
        _check("sign_capex", "Capex is non-positive (cash outflow ≤ 0)", 4, False,
               len(positive_capex) == 0,
               f"Capex reported as positive in {len(positive_capex)} year(s) — "
               "Yahoo Finance convention is negative; if positive, FCF calculation will double-count")

    # =========================================================================
    # MAGNITUDE CHECKS — values within plausible ranges
    # =========================================================================

    if _present("revenue") and _present("ebit"):
        ebit_margin_flags = []
        for y in years:
            rev = years_data[y].get("revenue")
            ebt = years_data[y].get("ebit")
            if rev and ebt and abs(rev) > 0:
                margin = ebt / rev
                if margin > _MAX_EBIT_MARGIN or margin < _MIN_EBIT_MARGIN:
                    ebit_margin_flags.append((y, round(margin * 100, 1)))
        _check("magnitude_ebit_margin", "EBIT margin within plausible range",  4, False,
               len(ebit_margin_flags) == 0,
               f"EBIT margin outside ±100% in years: {ebit_margin_flags} — "
               "check for unit mismatch or data error")

    if _present("revenue") and len(int_years) >= 2:
        growth_flags = []
        sorted_years = sorted(years, reverse=True)
        for i in range(len(sorted_years) - 1):
            y_curr = sorted_years[i]
            y_prev = sorted_years[i + 1]
            rev_curr = years_data[y_curr].get("revenue")
            rev_prev = years_data[y_prev].get("revenue")
            if rev_curr and rev_prev and abs(rev_prev) > 0:
                growth = (rev_curr - rev_prev) / abs(rev_prev)
                if growth > _MAX_PLAUSIBLE_REVENUE_GROWTH or growth < _MIN_PLAUSIBLE_REVENUE_GROWTH:
                    growth_flags.append((f"{y_prev}→{y_curr}", f"{growth*100:.0f}%"))
        _check("magnitude_revenue_growth", "Revenue growth within plausible range", 4, False,
               len(growth_flags) == 0,
               f"Implausible revenue growth in periods: {growth_flags} — "
               "possible unit error, acquisition, or restructuring")
        if growth_flags:
            manual_review.append(
                f"Revenue growth spikes detected in {[g[0] for g in growth_flags]} — "
                "check for acquisitions, disposals, or reporting currency changes"
            )

    if _present("capex") and _present("revenue"):
        capex_flags = []
        for y in years:
            rev  = years_data[y].get("revenue")
            cpx  = years_data[y].get("capex")
            if rev and cpx and abs(rev) > 0:
                intensity = abs(cpx) / abs(rev)
                if intensity > _MAX_CAPEX_TO_REVENUE:
                    capex_flags.append((y, f"{intensity*100:.0f}%"))
        _check("magnitude_capex_intensity", "Capex intensity within plausible range", 3, False,
               len(capex_flags) == 0,
               f"Capex > {_MAX_CAPEX_TO_REVENUE*100:.0f}% of revenue in {capex_flags} — "
               "verify — may indicate a major investment cycle or data error")

    # =========================================================================
    # COHERENCE CHECKS — internal consistency across fields
    # =========================================================================

    # FCF ≈ CFO + capex (where capex is negative)
    if _present("free_cash_flow") and _present("operating_cash_flow") and _present("capex"):
        fcf_divergence_years = []
        for y in years:
            fcf = years_data[y].get("free_cash_flow")
            cfo = years_data[y].get("operating_cash_flow")
            cpx = years_data[y].get("capex")
            if fcf is not None and cfo is not None and cpx is not None:
                implied_fcf = cfo + cpx
                if abs(implied_fcf) > 0:
                    divergence = abs(fcf - implied_fcf) / abs(implied_fcf)
                    if divergence > _FCF_TOLERANCE:
                        fcf_divergence_years.append(
                            (y, f"reported={_fmt(fcf)}, implied={_fmt(implied_fcf)}")
                        )
        _check("coherence_fcf", "Free cash flow ≈ CFO + capex",         5, False,
               len(fcf_divergence_years) == 0,
               f"FCF diverges >20% from CFO+capex in: {fcf_divergence_years} — "
               "possible lease adjustments, working capital definition differences, or data error")

    # EBITDA ≈ EBIT + D&A (loose check)
    if _present("ebitda") and _present("ebit") and _present("da"):
        ebitda_flags = []
        for y in years:
            ebitda = years_data[y].get("ebitda")
            ebit   = years_data[y].get("ebit")
            da     = years_data[y].get("da")
            if ebitda and ebit and da and abs(ebit + da) > 0:
                divergence = abs(ebitda - (ebit + da)) / abs(ebit + da)
                if divergence > 0.15:
                    ebitda_flags.append((y, f"{divergence*100:.0f}%"))
        _check("coherence_ebitda", "EBITDA ≈ EBIT + D&A",               4, False,
               len(ebitda_flags) == 0,
               f"EBITDA diverges >15% from EBIT+D&A in {ebitda_flags} — "
               "possible SBC, amortisation, or other adjustments included")

    # Gross profit ≤ revenue (always)
    if _present("gross_profit") and _present("revenue"):
        gp_flags = []
        for y in years:
            gp  = years_data[y].get("gross_profit")
            rev = years_data[y].get("revenue")
            if gp is not None and rev is not None and rev > 0 and gp > rev:
                gp_flags.append(y)
        _check("coherence_gross_profit", "Gross profit ≤ revenue",       4, False,
               len(gp_flags) == 0,
               f"Gross profit exceeds revenue in {gp_flags} — likely a data error")

    # Net debt sanity: cash should not exceed total assets
    if _present("cash") and _present("total_assets"):
        cash_asset_flags = []
        for y in years:
            cash   = years_data[y].get("cash")
            assets = years_data[y].get("total_assets")
            if cash and assets and assets > 0 and cash > assets:
                cash_asset_flags.append(y)
        _check("coherence_cash_vs_assets", "Cash does not exceed total assets", 3, False,
               len(cash_asset_flags) == 0,
               f"Cash exceeds total assets in {cash_asset_flags} — unit mismatch or data error")

    # =========================================================================
    # MANUAL REVIEW FLAGS (not scored — informational)
    # =========================================================================

    # (EBIT proxy warning removed — operating_income is no longer aliased to ebit)

    # Shares backfilled from point-in-time stats
    shares_backfilled = [
        y for y in years
        if years_data[y].get("_sources", {}).get("shares_outstanding") == "yahoo_stats"
    ]
    if shares_backfilled:
        manual_review.append(
            f"Shares outstanding backfilled from current stats in {sorted(shares_backfilled)} — "
            "historical share counts may differ (e.g. buybacks, issuances)"
        )

    # Flag if only one source contributed data
    sources_used = set()
    for y in years:
        sources_used.update(years_data[y].get("_sources", {}).values())
    if len(sources_used) <= 1:
        manual_review.append(
            f"Only one data source used ({sources_used}) — "
            "cross-source validation was not possible; treat data with caution"
        )

    # =========================================================================
    # Score and status
    # =========================================================================

    quality_score = round((passed_weight / total_weight) * 100, 1) if total_weight > 0 else 0.0

    if blockers:
        status = "fail"
    elif quality_score >= 75:
        status = "pass"
    else:
        status = "pass_with_caution"

    return {
        "status":        status,
        "quality_score": quality_score,
        "passed":        passed,
        "failed":        failed,
        "warnings":      warnings,
        "blockers":      blockers,
        "manual_review": manual_review,
        "ticker":        ticker,
        "company_name":  company_name,
        "metadata": {
            "validated_at":  datetime.utcnow().isoformat() + "Z",
            "years_checked": years,
            "n_checks":      len(passed) + len(failed),
            "n_passed":      len(passed),
            "n_failed":      len(failed),
        },
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_validator_report(result: dict) -> None:
    status = result["status"]
    score  = result["quality_score"]
    meta   = result["metadata"]

    status_label = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}
    print(f"\n{'='*60}")
    print(f"  Validator — {result['company_name']} ({result['ticker']})")
    print(f"  Status        : [{status_label.get(status, status)}]")
    print(f"  Quality score : {score}/100")
    print(f"  Checks        : {meta['n_passed']} passed / {meta['n_failed']} failed"
          f" of {meta['n_checks']} total")
    print(f"{'='*60}")

    if result["blockers"]:
        print("\n  Blockers (must resolve before normalisation):")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["failed"]:
        non_critical = [f for f in result["failed"] if not f.get("critical")]
        if non_critical:
            print("\n  Failed checks (non-critical):")
            for f in non_critical:
                print(f"    [{f['check']}] {f['detail']}")

    if result["manual_review"]:
        print("\n  Manual review flags:")
        for m in result["manual_review"]:
            print(f"    {m}")

    if result["passed"]:
        print(f"\n  Passed checks ({len(result['passed'])}):")
        for p in result["passed"]:
            print(f"    ✓ {p['description']}")

    print(f"\n  Validated at : {meta['validated_at']}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Formatting helper
# ---------------------------------------------------------------------------

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
