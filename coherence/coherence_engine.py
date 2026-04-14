"""
Sprint 4A — Coherence Engine

Purpose:
    Reality-check the full pipeline output by cross-validating model results
    against market observables, checking assumption plausibility, and flagging
    any values that warrant analyst review before conclusions are drawn.

    Non-blocking: never stops the pipeline. Produces a structured report of
    FLAG / WARN / PASS checks surfaced in terminal output and Excel.

Check categories:
    market_anchor   — model outputs vs observable market data
    assumption      — individual assumptions vs plausibility bounds
    concentration   — terminal value and model structure risks
    arithmetic      — internal cross-checks and consistency
    data_quality    — source coverage and confidence scoring

Status levels:
    flag  — serious concern that directly undermines model credibility;
            analyst must review before presenting or acting on the output
    warn  — plausible but outside normal bounds; warrants verification
    pass  — within expected range; no action required
"""


# ---------------------------------------------------------------------------
# Classification-based EV/Revenue expected ranges
# ---------------------------------------------------------------------------

_EV_REV_RANGES = {
    "asset_light":  (2.0, 20.0),
    "industrial":   (0.8,  8.0),
    "consumer":     (0.8,  6.0),
    "resources":    (0.8,  6.0),
    "utilities":    (4.0, 15.0),
    "hybrid":       (0.8, 12.0),
    "unknown":      (0.5, 25.0),
}

# Assumption confidence: tags that count as "grounded in data"
_GROUNDED_TAGS = {"sourced", "rule_based", "calibrated"}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_coherence_engine(valued: dict) -> dict:
    """
    Run all coherence checks and return a structured report.
    Never raises — all errors are caught and surfaced as a skipped check.
    """
    try:
        return _run(valued)
    except Exception as e:
        return {
            "status":     "error",
            "checks":     [],
            "flags":      [],
            "warns":      [],
            "passes":     [],
            "assumption_confidence_score": None,
            "error":      str(e),
        }


def _run(valued: dict) -> dict:
    stats      = valued.get("stats", {})
    assumptions = valued.get("assumptions", {})
    valuation  = valued.get("valuation", {})
    forecast   = valued.get("forecast", {})
    norm       = valued.get("normalised", {})
    wc         = valued.get("wacc_components", {})
    bridge_in  = valued.get("equity_bridge_inputs", {})
    classif    = valued.get("classification", "unknown")

    base_val   = valuation.get("base", {})
    base_proj  = forecast.get("base", {})

    price      = stats.get("current_price")
    market_cap = stats.get("market_cap")
    shares     = bridge_in.get("shares_outstanding") or stats.get("shares_outstanding")

    vps        = base_val.get("value_per_share")
    ev         = base_val.get("enterprise_value")
    pv_tv      = base_val.get("pv_terminal_value")
    tv_pct     = base_val.get("terminal_value_pct_ev")
    pv_fcst    = base_val.get("pv_forecast_period")

    wacc       = (assumptions.get("wacc")        or {}).get("value")
    tg         = (assumptions.get("terminal_growth") or {}).get("value")
    tax        = (assumptions.get("tax_rate")    or {}).get("value")
    nwc_pct    = (assumptions.get("nwc_pct_revenue") or {}).get("value")
    capex_pct  = (assumptions.get("capex_pct_revenue") or {}).get("value")

    revenue    = norm.get("revenue")

    checks = []

    # -----------------------------------------------------------------------
    # 1. VPS vs current market price
    # -----------------------------------------------------------------------
    if vps is not None and price and price > 0:
        implied_return = (vps - price) / price
        if abs(implied_return) > 2.0:
            status = "flag"
        elif abs(implied_return) > 0.5:
            status = "warn"
        else:
            status = "pass"

        direction = "undervalued" if implied_return > 0 else "overvalued"
        detail = (
            f"Model VPS {_fp(vps)} vs current price {_fp(price)} = "
            f"{implied_return:+.1%} ({direction})."
        )
        if status == "flag":
            detail += (
                f" A divergence of this magnitude almost always indicates a model error "
                f"rather than a genuine mispricing. Common causes: wrong shares outstanding, "
                f"unit mismatch (pence vs pounds), or a severely distorted base assumption "
                f"(e.g. tax rate, NWC). Verify each input before drawing any conclusion."
            )
        elif status == "warn":
            detail += (
                f" This is a large implied return. Verify that key assumptions — "
                f"particularly tax rate, NWC, and terminal growth — are grounded in data "
                f"rather than defaults before presenting this output."
            )

        checks.append(_chk("vps_vs_price", "market_anchor", status,
                           f"{implied_return:+.1%}", "±50% warn · ±200% flag",
                           "VPS vs Market Price", detail))

    # -----------------------------------------------------------------------
    # 2. Shares cross-check: shares × price vs scraped market cap
    # -----------------------------------------------------------------------
    if shares and price and market_cap and market_cap > 0:
        derived_mktcap = shares * price
        ratio = derived_mktcap / market_cap
        if ratio < 0.5 or ratio > 2.0:
            status = "flag"
        elif ratio < 0.85 or ratio > 1.18:
            status = "warn"
        else:
            status = "pass"

        detail = (
            f"Shares ({_fb(shares)}) × price ({_fp(price)}) = {_fb(derived_mktcap)} "
            f"vs scraped market cap {_fb(market_cap)} (ratio {ratio:.2f}x)."
        )
        if status == "flag":
            detail += (
                " Large discrepancy suggests shares or price are on different unit scales "
                "(e.g. shares in thousands vs actual count, or price in pence vs pounds). "
                "This directly corrupts the equity bridge and value per share."
            )
        elif status == "warn":
            detail += (
                " Minor discrepancy — check for share buybacks or recent issuance "
                "between the balance sheet date and today."
            )

        checks.append(_chk("shares_cross_check", "arithmetic", status,
                           f"{ratio:.2f}x", "0.85–1.18 pass",
                           "Shares Cross-Check", detail))

    # -----------------------------------------------------------------------
    # 3. Terminal value concentration
    # -----------------------------------------------------------------------
    if tv_pct is not None:
        if tv_pct > 0.80:
            status = "flag"
        elif tv_pct > 0.60:
            status = "warn"
        else:
            status = "pass"

        detail = f"Terminal value is {tv_pct:.1%} of enterprise value."
        if status == "flag":
            spread = (wacc - tg) if wacc and tg else None
            sensitivity_hint = ""
            if spread and spread > 0:
                # Approximate: 1% change in g ≈ (1/spread) × VPS / EV × 100%
                sensitivity_hint = (
                    f" At WACC-g spread of {spread:.1%}, a ±0.5% change in terminal "
                    f"growth rate moves VPS by approximately ±{0.005/spread:.0%}. "
                )
            detail += (
                " The model result is almost entirely driven by the perpetuity assumption. "
                + sensitivity_hint +
                "The sensitivity table is essential reading — do not present a point estimate "
                "without showing the full WACC × growth range."
            )
        elif status == "warn":
            detail += (
                " A significant portion of value lies in the terminal period. "
                "Verify terminal growth assumption reflects a sustainable long-run rate, "
                "not the near-term growth trajectory."
            )

        checks.append(_chk("tv_concentration", "concentration", status,
                           f"{tv_pct:.1%}", ">60% warn · >80% flag",
                           "Terminal Value Concentration", detail))

    # -----------------------------------------------------------------------
    # 4. Effective tax rate
    # -----------------------------------------------------------------------
    if tax is not None:
        if tax < 0.10:
            status = "flag"
        elif tax < 0.15:
            status = "warn"
        else:
            status = "pass"

        detail = f"Effective tax rate: {tax:.1%}."
        if status != "pass":
            detail += (
                f" Below the practical floor of 15%. Most likely caused by deferred tax "
                f"credits or pre-tax losses in the historical period pulling down the median. "
                f"Effect: NOPAT is overstated in every forecast year, inflating FCFF and EV. "
                f"Override recommended — statutory rates: UK ~25%, US ~21%, Germany ~30%."
            )

        checks.append(_chk("tax_rate", "assumption", status,
                           f"{tax:.1%}", "≥15% warn · ≥10% flag",
                           "Effective Tax Rate", detail))

    # -----------------------------------------------------------------------
    # 5. NWC % revenue
    # -----------------------------------------------------------------------
    if nwc_pct is not None:
        abs_nwc = abs(nwc_pct)
        if abs_nwc > 0.40:
            status = "flag"
        elif abs_nwc > 0.25:
            status = "warn"
        else:
            status = "pass"

        detail = f"NWC/Revenue: {nwc_pct:.1%}."
        if status == "flag":
            detail += (
                " Extremely high. At this level, NWC investment consumes a large fraction "
                "of NOPAT each year, severely compressing FCFF. This is almost certainly "
                "a data issue — accounts receivable may be inflated by long-term contract "
                "balances (e.g. work-in-progress, unbilled revenue) that are not true "
                "working capital. Verify AR, inventory, and AP directly from the balance sheet."
            )
        elif status == "warn":
            detail += (
                " Above the 25% threshold. Common in long-cycle industrials and aerospace "
                "(large contract receivables), but worth verifying the AR/AP/inventory "
                "components against the reported balance sheet to rule out a data error."
            )

        checks.append(_chk("nwc_pct", "assumption", status,
                           f"{nwc_pct:.1%}", ">25% warn · >40% flag",
                           "NWC % Revenue", detail))

    # -----------------------------------------------------------------------
    # 6. WACC reasonableness
    # -----------------------------------------------------------------------
    if wacc is not None:
        if wacc < 0.04 or wacc > 0.20:
            status = "flag"
        elif wacc < 0.05 or wacc > 0.15:
            status = "warn"
        else:
            status = "pass"

        detail = f"WACC: {wacc:.1%}."
        if wacc < 0.04:
            detail += (" Below 4% — implausibly low for an equity-backed company. "
                       "Check risk-free rate and beta inputs.")
        elif wacc > 0.20:
            detail += (" Above 20% — unusually high. This compresses all DCF values "
                       "dramatically and may reflect an error in cost-of-debt or beta.")
        elif wacc < 0.05:
            detail += " Below 5% — review risk-free rate and equity risk premium inputs."
        elif wacc > 0.15:
            detail += " Above 15% — verify beta and cost-of-debt are reasonable for this company."

        checks.append(_chk("wacc_bounds", "assumption", status,
                           f"{wacc:.1%}", "5–15% pass",
                           "WACC Reasonableness", detail))

    # -----------------------------------------------------------------------
    # 7. WACC − terminal growth spread
    # -----------------------------------------------------------------------
    if wacc is not None and tg is not None:
        spread = wacc - tg
        if spread < 0.01:
            status = "flag"
        elif spread < 0.03:
            status = "warn"
        else:
            status = "pass"

        detail = f"WACC ({wacc:.1%}) − terminal growth ({tg:.1%}) = {spread:.1%} spread."
        if status == "flag":
            detail += (
                " Spread below 1% makes the Gordon Growth Model extremely unstable — "
                "tiny changes in either input produce enormous swings in terminal value. "
                "A spread of ≤0% produces a mathematically undefined (infinite) result."
            )
        elif status == "warn":
            detail += (
                " Narrow spread increases model sensitivity to terminal assumptions. "
                "Consider stress-testing with a lower terminal growth rate."
            )

        checks.append(_chk("wacc_tg_spread", "concentration", status,
                           f"{spread:.1%}", ">3% pass · 1–3% warn · <1% flag",
                           "WACC–Growth Spread", detail))

    # -----------------------------------------------------------------------
    # 8. EV / Revenue multiple (vs classification-based range)
    # -----------------------------------------------------------------------
    if ev is not None and revenue and revenue > 0:
        ev_rev = ev / revenue
        lo, hi = _EV_REV_RANGES.get(classif, _EV_REV_RANGES["unknown"])
        if ev_rev < lo * 0.5 or ev_rev > hi * 2.0:
            status = "flag"
        elif ev_rev < lo or ev_rev > hi:
            status = "warn"
        else:
            status = "pass"

        detail = (
            f"EV/Revenue: {ev_rev:.1f}x (expected range for {classif}: {lo:.1f}–{hi:.1f}x)."
        )
        if status == "flag":
            detail += (
                " Significantly outside the typical range for this classification. "
                "Either the classification is wrong, or a key input (revenue base, EV) "
                "is distorted. Cross-check against public comparables."
            )
        elif status == "warn":
            detail += (
                " At the edge of the expected range. May reflect a premium/discount "
                "valuation situation — verify growth and margin assumptions are consistent "
                "with the implied multiple."
            )

        checks.append(_chk("ev_revenue_multiple", "market_anchor", status,
                           f"{ev_rev:.1f}x", f"{lo:.1f}–{hi:.1f}x for {classif}",
                           "EV / Revenue Multiple", detail))

    # -----------------------------------------------------------------------
    # 9. Forecast NOPAT margin vs historical peak (back-loading check)
    # -----------------------------------------------------------------------
    if base_proj:
        years = sorted(base_proj.keys())
        final_yr = years[-1] if years else None
        if final_yr:
            final_nopat_margin = base_proj[final_yr].get("nopat_margin")
            # Compute historical NOPAT margin from norm if available
            hist_ebit_margin = norm.get("ebit_margin")
            hist_tax = norm.get("tax_rate", 0.25)
            hist_nopat_margin = hist_ebit_margin * (1 - hist_tax) if hist_ebit_margin else None

            if final_nopat_margin is not None and hist_nopat_margin is not None and hist_nopat_margin > 0:
                expansion = (final_nopat_margin - hist_nopat_margin) / hist_nopat_margin
                if expansion > 1.0:
                    status = "flag"
                elif expansion > 0.5:
                    status = "warn"
                else:
                    status = "pass"

                detail = (
                    f"Terminal year NOPAT margin: {final_nopat_margin:.1%} vs "
                    f"historical normalised: {hist_nopat_margin:.1%} "
                    f"({expansion:+.0%} expansion)."
                )
                if status == "flag":
                    detail += (
                        " More than doubling the NOPAT margin over 5 years is rarely achievable "
                        "and implies the terminal FCF is built on an unsustainably high margin. "
                        "This compounds into the terminal value, inflating EV materially."
                    )
                elif status == "warn":
                    detail += (
                        " Significant margin expansion baked into the forecast. "
                        "Verify the margin trajectory is supported by a clear operating "
                        "leverage or mix-shift story."
                    )

                checks.append(_chk("nopat_margin_expansion", "assumption", status,
                                   f"{expansion:+.0%}", "<50% expansion warn · <100% flag",
                                   "Terminal NOPAT Margin Expansion", detail))

    # -----------------------------------------------------------------------
    # 10. Assumption confidence score
    # -----------------------------------------------------------------------
    assumption_fields = [
        "revenue_growth", "ebit_margin", "tax_rate", "da_pct_revenue",
        "capex_pct_revenue", "nwc_pct_revenue", "wacc", "terminal_growth",
    ]
    total = 0
    grounded = 0
    ungrounded_fields = []
    for field in assumption_fields:
        entry = assumptions.get(field)
        if entry and isinstance(entry, dict):
            total += 1
            tag = entry.get("tag", "")
            if tag in _GROUNDED_TAGS:
                grounded += 1
            else:
                ungrounded_fields.append(field)

    conf_score = round(grounded / total * 100) if total > 0 else None

    if conf_score is not None:
        if conf_score < 50:
            status = "flag"
        elif conf_score < 75:
            status = "warn"
        else:
            status = "pass"

        detail = (
            f"{grounded}/{total} assumptions are grounded in historical data "
            f"(score: {conf_score}/100)."
        )
        if ungrounded_fields:
            detail += (
                f" Defaulted assumptions: {', '.join(ungrounded_fields)}. "
                "These rely on rule-based defaults rather than company-specific data — "
                "treat associated outputs with additional caution."
            )

        checks.append(_chk("assumption_confidence", "data_quality", status,
                           f"{conf_score}/100", "≥75 pass · 50–74 warn · <50 flag",
                           "Assumption Confidence Score", detail))

    # -----------------------------------------------------------------------
    # Collate results
    # -----------------------------------------------------------------------
    flags   = [c for c in checks if c["status"] == "flag"]
    warns   = [c for c in checks if c["status"] == "warn"]
    passes  = [c for c in checks if c["status"] == "pass"]

    if flags:
        overall = "review_required"
    elif warns:
        overall = "caution"
    else:
        overall = "pass"

    return {
        "status":                    overall,
        "checks":                    checks,
        "flags":                     flags,
        "warns":                     warns,
        "passes":                    passes,
        "assumption_confidence_score": conf_score,
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_coherence_report(result: dict, valued: dict):
    company = valued.get("company_name", "")
    ticker  = valued.get("ticker", "")
    checks  = result.get("checks", [])
    flags   = result.get("flags", [])
    warns   = result.get("warns", [])
    passes  = result.get("passes", [])
    status  = result.get("status", "")

    status_label = {
        "review_required": "[REVIEW REQUIRED]",
        "caution":         "[CAUTION]",
        "pass":            "[PASS]",
        "error":           "[ERROR]",
    }.get(status, status.upper())

    print()
    print("=" * 60)
    print(f"  Coherence Engine — {company} ({ticker})")
    parts = []
    if flags:  parts.append(f"{len(flags)} flag{'s' if len(flags)>1 else ''}")
    if warns:  parts.append(f"{len(warns)} warning{'s' if len(warns)>1 else ''}")
    if passes: parts.append(f"{len(passes)} passed")
    print(f"  Status   : {status_label}  |  {' · '.join(parts)}")
    print("=" * 60)

    if result.get("error"):
        print(f"\n  ERROR: {result['error']}\n")
        return

    # Print flags first, then warns, then passes (compact)
    flagged_or_warned = [c for c in checks if c["status"] in ("flag", "warn")]
    passed = [c for c in checks if c["status"] == "pass"]

    for chk in flagged_or_warned:
        icon  = "  [FLAG] " if chk["status"] == "flag" else "  [WARN] "
        print()
        print(f"{icon} {chk['title']}  —  {chk['value']}  (benchmark: {chk['benchmark']})")
        # Word-wrap detail at ~70 chars
        words = chk["detail"].split()
        line = "         "
        for word in words:
            if len(line) + len(word) + 1 > 78:
                print(line)
                line = "         " + word
            else:
                line += (" " if line.strip() else "") + word
        if line.strip():
            print(line)

    if passed:
        print()
        pass_summaries = [f"{c['title']} ({c['value']})" for c in passed]
        print(f"  [PASS]  " + "  ·  ".join(pass_summaries))

    print()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chk(check_id, category, status, value, benchmark, title, detail):
    return {
        "check":     check_id,
        "category":  category,
        "status":    status,
        "value":     value,
        "benchmark": benchmark,
        "title":     title,
        "detail":    detail,
    }


def _fp(val) -> str:
    """Format as price/per-share value."""
    if val is None:
        return "—"
    return f"{val:,.2f}"


def _fb(val) -> str:
    """Format as large financial value with B/M suffix."""
    if val is None:
        return "—"
    v = abs(float(val))
    s = f"{v/1e9:.2f}B" if v >= 1e9 else f"{v/1e6:.1f}M" if v >= 1e6 else f"{v:,.0f}"
    return f"({s})" if float(val) < 0 else s
