"""
Sprint 4B — Explainer

Purpose:
    Synthesise the full pipeline output into plain-English narrative.
    No new calculations — pure translation of what the model found.

    Answers the question a first-time reader would ask:
    "What is this model telling me, and should I trust it?"

Output sections:
    one_liner           — single sentence: company, VPS, implied return, confidence
    executive_summary   — 4 bullets covering outcome, confidence, key risk, next step
    assumption_narrative— per-assumption: what it is, where it came from, caveats
    value_drivers       — what is actually driving the number (TV%, growth, margin)
    review_agenda       — prioritised list of things to fix before presenting
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Tag → plain English source description
# ---------------------------------------------------------------------------

_TAG_DESC = {
    "sourced":             "sourced from live market data",
    "rule_based":          "derived from historical company data",
    "calibrated":          "calibrated from historical patterns",
    "rule_based_default":  "system default — no company data available",
    "default":             "system default — no company data available",
}

_CLASSIF_DESC = {
    "asset_light":  "asset-light business (high margins, low capex)",
    "industrial":   "industrial company (moderate capex intensity)",
    "consumer":     "consumer business (brand-driven, inventory-dependent)",
    "resources":    "resources / commodities company (cyclical, capex-heavy)",
    "utilities":    "regulated utility (stable cashflows, high leverage)",
    "hybrid":       "hybrid / diversified business model",
    "unknown":      "unclassified business",
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_explainer(valued: dict) -> dict:
    """Generate plain-English explanation of the full pipeline output."""
    try:
        return _run(valued)
    except Exception as e:
        return {
            "status":               "error",
            "error":                str(e),
            "one_liner":            "Explainer failed — see error.",
            "executive_summary":    [],
            "assumption_narrative": [],
            "value_drivers":        [],
            "review_agenda":        [],
            "overall_confidence":   "low",
        }


def _run(valued: dict) -> dict:
    stats      = valued.get("stats", {})
    norm       = valued.get("normalised", {})
    assumptions = valued.get("assumptions", {})
    wc         = valued.get("wacc_components", {})
    valuation  = valued.get("valuation", {})
    forecast   = valued.get("forecast", {})
    coherence  = valued.get("coherence", {})
    classif    = valued.get("classification", "unknown")
    base_year  = valued.get("base_year", "")
    quality    = valued.get("quality_score", 0)

    company    = valued.get("company_name", "")
    ticker     = valued.get("ticker", "")
    ccy        = stats.get("currency", "")
    sector     = stats.get("sector", "")
    country    = stats.get("country", "")
    price      = stats.get("current_price")

    base_val   = valuation.get("base", {})
    vps        = base_val.get("value_per_share")
    ev         = base_val.get("enterprise_value")
    tv_pct     = base_val.get("terminal_value_pct_ev")
    impl_ret   = base_val.get("implied_return")

    wacc       = (assumptions.get("wacc")             or {}).get("value")
    tg         = (assumptions.get("terminal_growth")  or {}).get("value")
    tax        = (assumptions.get("tax_rate")         or {}).get("value")
    nwc_pct    = (assumptions.get("nwc_pct_revenue")  or {}).get("value")
    capex_pct  = (assumptions.get("capex_pct_revenue") or {}).get("value")

    flags      = coherence.get("flags", [])
    warns      = coherence.get("warns", [])
    conf_score = coherence.get("assumption_confidence_score")

    # -----------------------------------------------------------------------
    # Overall confidence
    # -----------------------------------------------------------------------
    if flags:
        overall_confidence = "low"
    elif warns or quality < 75:
        overall_confidence = "medium"
    else:
        overall_confidence = "high"

    conf_label = overall_confidence.upper()

    # -----------------------------------------------------------------------
    # One-liner
    # -----------------------------------------------------------------------
    if vps is not None and price:
        direction = "below" if vps < price else "above"
        pct = abs(impl_ret) if impl_ret is not None else abs((vps - price) / price)
        one_liner = (
            f"{company} ({ticker}) — base case DCF values the equity at "
            f"{ccy} {vps:.2f}/share ({pct:.0%} {direction} current market price of "
            f"{ccy} {price:.2f}). Overall model confidence: {conf_label}."
        )
    else:
        one_liner = f"{company} ({ticker}) — valuation could not produce a per-share value."

    # -----------------------------------------------------------------------
    # Executive summary (4 bullets)
    # -----------------------------------------------------------------------
    exec_summary = []

    # Bullet 1: Company context
    classif_desc = _CLASSIF_DESC.get(classif, classif)
    exec_summary.append(
        f"{company} is a {country}-based {sector} company, classified as an "
        f"{classif_desc}. Financial data covers {base_year} as the base year, "
        f"with a data quality score of {quality}/100."
    )

    # Bullet 2: Valuation outcome
    if vps is not None and price:
        bear_vps = (valuation.get("bear") or {}).get("value_per_share")
        bull_vps = (valuation.get("bull") or {}).get("value_per_share")
        scenario_range = (
            f" Scenario range: bear {ccy} {bear_vps:.2f} — bull {ccy} {bull_vps:.2f}."
            if bear_vps and bull_vps else ""
        )
        direction = "below" if vps < price else "above"
        pct = abs(impl_ret) if impl_ret is not None else abs((vps - price) / price)
        exec_summary.append(
            f"Base case intrinsic value is {ccy} {vps:.2f}/share — {pct:.0%} {direction} "
            f"the current market price of {ccy} {price:.2f}, "
            f"implying the stock is {'overvalued' if vps < price else 'undervalued'} "
            f"on these assumptions.{scenario_range}"
        )
    else:
        exec_summary.append("A per-share value could not be determined — see blockers.")

    # Bullet 3: Confidence and key risk
    if flags:
        flag_titles = " and ".join(f["title"] for f in flags[:2])
        exec_summary.append(
            f"Model confidence is {conf_label}. {len(flags)} item(s) require analyst "
            f"review before this output can be presented: {flag_titles}. "
            "These issues directly affect the reliability of the FCF and valuation figures."
        )
    elif warns:
        warn_titles = " and ".join(w["title"] for w in warns[:2])
        exec_summary.append(
            f"Model confidence is {conf_label}. The pipeline raised {len(warns)} "
            f"warning(s) — {warn_titles} — that warrant verification but do not "
            "invalidate the output on their own."
        )
    else:
        exec_summary.append(
            f"Model confidence is {conf_label}. All coherence checks passed. "
            "Assumptions are grounded in historical data and within expected ranges."
        )

    # Bullet 4: TV concentration / key structural note
    if tv_pct and tv_pct > 0.60:
        exec_summary.append(
            f"{tv_pct:.0%} of enterprise value is in the terminal period. "
            "This model is sensitive to terminal growth and WACC assumptions — "
            "no single-point estimate should be presented without the sensitivity table."
        )
    elif tv_pct:
        exec_summary.append(
            f"Terminal value represents {tv_pct:.0%} of EV — reasonable balance "
            "between near-term cashflows and perpetuity value."
        )

    # -----------------------------------------------------------------------
    # Assumption narrative
    # -----------------------------------------------------------------------
    assumption_narrative = []

    def _narrate(field, label, value_fmt, narrative_fn):
        entry = assumptions.get(field) or {}
        val   = entry.get("value")
        tag   = entry.get("tag", "rule_based")
        if val is None:
            return
        source_desc = _TAG_DESC.get(tag, tag)
        assumption_narrative.append({
            "assumption": label,
            "value":      value_fmt(val),
            "source":     source_desc,
            "narrative":  narrative_fn(val, entry),
        })

    # WACC — build from components
    beta = wc.get("beta")
    rf   = wc.get("risk_free_rate")
    erp  = wc.get("equity_risk_premium")
    ke   = wc.get("cost_of_equity")
    kd   = wc.get("cost_of_debt_aftertax")
    we   = wc.get("weight_equity")
    wd   = wc.get("weight_debt")
    if wacc:
        wacc_detail = []
        if rf and erp and beta and ke:
            wacc_detail.append(
                f"Cost of equity: {rf:.1%} risk-free rate + ({beta:.2f} beta × "
                f"{erp:.1%} ERP) = {ke:.1%}."
            )
        if kd and wd and we:
            wacc_detail.append(
                f"Capital structure: {we:.0%} equity / {wd:.0%} debt. "
                f"Post-tax cost of debt: {kd:.1%}."
            )
        assumption_narrative.append({
            "assumption": "Discount Rate (WACC)",
            "value":      f"{wacc:.1%}",
            "source":     "derived from CAPM + balance sheet weights",
            "narrative":  " ".join(wacc_detail) if wacc_detail else f"WACC of {wacc:.1%}.",
        })

    # Terminal growth
    _narrate("terminal_growth", "Terminal Growth Rate", lambda v: f"{v:.1%}",
        lambda v, e: (
            f"Perpetuity growth rate of {v:.1%}. "
            + ("At or below long-run nominal GDP growth — reasonable for a mature company."
               if v <= 0.03 else
               "Above 3% — verify this reflects a sustainable long-run rate, "
               "not near-term cyclical growth.")
            + (f" WACC-growth spread: {wacc - v:.1%}." if wacc else "")
        ))

    # Revenue growth
    rev_entry = assumptions.get("revenue_growth") or {}
    rev_tag   = rev_entry.get("tag", "")
    if rev_entry.get("year_1"):
        yr1 = rev_entry["year_1"]
        yr5 = rev_entry.get("year_5", yr1)
        assumption_narrative.append({
            "assumption": "Revenue Growth",
            "value":      f"Yr1: {yr1:.1%} → Yr5: {yr5:.1%}",
            "source":     _TAG_DESC.get(rev_tag, rev_tag),
            "narrative": (
                f"Year 1 growth of {yr1:.1%} fading to {yr5:.1%} by Year 5. "
                "Growth path is a weighted blend of 1-, 3-, and 5-year historical CAGRs, "
                "linearly fading toward the terminal rate."
            ),
        })

    # EBIT margin
    margin_entry = assumptions.get("ebit_margin") or {}
    if margin_entry.get("year_1"):
        m1 = margin_entry["year_1"]
        m5 = margin_entry.get("year_5", m1)
        norm_margin = norm.get("ebit_margin")
        assumption_narrative.append({
            "assumption": "EBIT Margin",
            "value":      f"Yr1: {m1:.1%} → Yr5: {m5:.1%}",
            "source":     _TAG_DESC.get(margin_entry.get("tag", ""), "rule_based"),
            "narrative": (
                f"Base margin of {norm_margin:.1%} (normalised historical median), "
                f"expanding to {m5:.1%} by Year 5. "
                "Expansion reflects partial mean-reversion toward historical peak margin."
                if norm_margin else
                f"EBIT margin of {m1:.1%} in Year 1, expanding to {m5:.1%} by Year 5."
            ),
        })

    # Tax rate
    _narrate("tax_rate", "Effective Tax Rate", lambda v: f"{v:.1%}",
        lambda v, e: (
            f"Effective rate of {v:.1%}, derived as median of historical tax provisions. "
            + ("Below the practical floor of 15% — likely distorted by deferred tax credits. "
               "Override with statutory rate (~25%) is recommended."
               if v < 0.15 else
               "Within normal range for this jurisdiction.")
        ))

    # Capex
    _narrate("capex_pct_revenue", "Capex % Revenue", lambda v: f"{v:.1%}",
        lambda v, e: (
            f"Capital expenditure held at {v:.1%} of revenue throughout the forecast, "
            "derived as the median of the historical capex intensity. "
            + ("Low capex intensity — consistent with asset-light classification."
               if v < 0.05 else
               "Moderate capex intensity — typical for an industrial business."
               if v < 0.12 else
               "High capex intensity — verify this is maintenance + growth, not one-off.")
        ))

    # NWC
    _narrate("nwc_pct_revenue", "NWC % Revenue", lambda v: f"{v:.1%}",
        lambda v, e: (
            f"Net working capital held at {v:.1%} of revenue. "
            + ("High ratio — each incremental revenue pound requires significant working "
               "capital investment, compressing free cashflow. Verify AR/AP/inventory data."
               if abs(v) > 0.25 else
               "Within normal range.")
        ))

    # -----------------------------------------------------------------------
    # Value drivers
    # -----------------------------------------------------------------------
    value_drivers = []

    # TV concentration
    if tv_pct is not None:
        pv_fcst = base_val.get("pv_forecast_period")
        if pv_fcst and ev:
            fcst_pct = pv_fcst / ev
            value_drivers.append(
                f"Terminal value accounts for {tv_pct:.0%} of enterprise value, "
                f"with the 5-year forecast period contributing the remaining {fcst_pct:.0%}. "
                + ("The model is heavily terminal-dependent — the sensitivity table should be "
                   "read as the primary output, not the point estimate."
                   if tv_pct > 0.75 else
                   "This is a reasonable split for a company with moderate near-term cashflows.")
            )

    # Growth profile
    rev_y1 = (assumptions.get("revenue_growth") or {}).get("year_1")
    rev_y5 = (assumptions.get("revenue_growth") or {}).get("year_5")
    if rev_y1 and rev_y5:
        value_drivers.append(
            f"Revenue grows from {rev_y1:.1%} in Year 1, fading to {rev_y5:.1%} by "
            f"Year 5, then to the {tg:.1%} terminal rate. "
            "The fade reflects the assumption that above-average growth normalises "
            "toward the long-run rate as the company matures."
            if tg else
            f"Revenue growth fades from {rev_y1:.1%} to {rev_y5:.1%} over the forecast."
        )

    # FCF conversion context
    base_proj = forecast.get("base", {})
    if base_proj:
        years = sorted(base_proj.keys())
        if years:
            y1_data = base_proj[years[0]]
            yn_data = base_proj[years[-1]]
            y1_fcff = y1_data.get("fcff")
            yn_fcff = yn_data.get("fcff")
            y1_rev  = y1_data.get("revenue")
            if y1_fcff and yn_fcff and y1_rev and y1_fcff > 0:
                fcff_margin_y1 = y1_fcff / y1_rev if y1_rev else None
                value_drivers.append(
                    f"Free cashflow to firm grows from "
                    f"{_fb(y1_fcff)} in Year 1 to {_fb(yn_fcff)} in Year 5"
                    + (f" ({fcff_margin_y1:.0%} FCFF margin in Year 1)" if fcff_margin_y1 else "")
                    + ". NWC investment is the main drag on conversion from NOPAT to FCFF."
                    if nwc_pct and abs(nwc_pct) > 0.20 else
                    "."
                )

    # WACC sensitivity note
    if wacc and tg:
        spread = wacc - tg
        value_drivers.append(
            f"The WACC-growth spread is {spread:.1%} ({wacc:.1%} − {tg:.1%}). "
            + ("This is a healthy spread — the model is not overly sensitive to "
               "small changes in either rate."
               if spread > 0.04 else
               "This is a narrow spread — small changes in terminal growth or WACC "
               "produce large swings in terminal value. Treat the point estimate with caution.")
        )

    # -----------------------------------------------------------------------
    # Review agenda (from coherence flags + warns, ordered by severity)
    # -----------------------------------------------------------------------
    review_agenda = []
    priority = 1

    for f in flags:
        review_agenda.append({
            "priority": priority,
            "status":   "FLAG",
            "item":     f["title"],
            "value":    f["value"],
            "action":   _action_for(f["check"], f, valued),
        })
        priority += 1

    for w in warns:
        review_agenda.append({
            "priority": priority,
            "status":   "WARN",
            "item":     w["title"],
            "value":    w["value"],
            "action":   _action_for(w["check"], w, valued),
        })
        priority += 1

    if not review_agenda:
        review_agenda.append({
            "priority": 1,
            "status":   "PASS",
            "item":     "No items require review",
            "value":    "—",
            "action":   "Model is ready to present subject to standard analyst sign-off.",
        })

    return {
        "status":               "pass",
        "one_liner":            one_liner,
        "executive_summary":    exec_summary,
        "assumption_narrative": assumption_narrative,
        "value_drivers":        value_drivers,
        "review_agenda":        review_agenda,
        "overall_confidence":   overall_confidence,
        "generated_at":         datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Action text per coherence check type
# ---------------------------------------------------------------------------

def _action_for(check_id: str, chk: dict, valued: dict) -> str:
    ccy = valued.get("stats", {}).get("currency", "")
    actions = {
        "tax_rate":              (
            "Override the effective tax rate with the statutory rate for this jurisdiction "
            "(UK ~25%, US ~21%, Germany ~30%). Re-run the model to see the corrected NOPAT, "
            "FCFF, and EV figures."
        ),
        "nwc_pct":               (
            "Pull accounts receivable, inventory, and accounts payable directly from the "
            "latest annual report balance sheet. Recalculate NWC = AR + Inventory − AP and "
            "verify the ratio against reported revenue. If contract advances are material, "
            "consider excluding them from the NWC definition."
        ),
        "tv_concentration":      (
            "Do not present a single point estimate. Share the full sensitivity table "
            "(WACC × terminal growth grid) as the primary output. Consider running a "
            "downside case with terminal growth 0.5% lower than base."
        ),
        "vps_vs_price":          (
            f"Review implied return of {chk['value']} carefully. Check: (1) shares "
            "outstanding unit (should be raw count, not thousands/millions), "
            "(2) current price currency matches financial statement currency, "
            "(3) no double-counting in the equity bridge."
        ),
        "shares_cross_check":    (
            "Verify shares outstanding against the latest annual report or regulatory filing. "
            "Confirm units are raw share count (not millions or thousands). "
            f"Cross-check: market cap ÷ current price should equal shares."
        ),
        "wacc_bounds":           (
            "Review risk-free rate, equity risk premium, and beta inputs. "
            "Benchmark against comparable companies and current market data."
        ),
        "wacc_tg_spread":        (
            "Increase the terminal growth rate assumption or reduce WACC. "
            "A spread below 2% makes the model unstable — small input changes "
            "produce disproportionate output swings."
        ),
        "ev_revenue_multiple":   (
            "Cross-check the implied EV/Revenue multiple against listed comparables "
            "in the same sector. If significantly out of range, review classification "
            "or key revenue/EBIT inputs."
        ),
        "nopat_margin_expansion": (
            "Review the margin expansion path in the forecast. If terminal NOPAT margin "
            "materially exceeds the historical range, document the operating leverage "
            "or cost reduction thesis that supports it."
        ),
        "assumption_confidence":  (
            "Identify which assumptions are defaulted (not grounded in company data) "
            "and prioritise sourcing company-specific inputs for those fields."
        ),
    }
    return actions.get(check_id, chk.get("detail", "Review and verify this assumption."))


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_explainer_report(result: dict, valued: dict):
    company = valued.get("company_name", "")
    ticker  = valued.get("ticker", "")

    print()
    print("=" * 60)
    print(f"  Explainer  —  {company} ({ticker})")
    print("=" * 60)

    if result.get("error"):
        print(f"\n  ERROR: {result['error']}\n")
        return

    # One-liner
    print()
    _wrap(result["one_liner"], indent=2)

    # Executive Summary
    _print_section("EXECUTIVE SUMMARY")
    for bullet in result["executive_summary"]:
        _wrap(f"• {bullet}", indent=4, hanging=6)

    # Assumptions
    _print_section("KEY ASSUMPTIONS")
    for a in result["assumption_narrative"]:
        print(f"    {a['assumption']:30s}  {a['value']}")
        _wrap(a["narrative"], indent=6)
        print(f"      Source: {a['source']}")
        print()

    # Value Drivers
    _print_section("WHAT IS DRIVING THE VALUE")
    for d in result["value_drivers"]:
        _wrap(f"• {d}", indent=4, hanging=6)

    # Review Agenda
    _print_section("REVIEW AGENDA  —  address before presenting")
    for item in result["review_agenda"]:
        badge = f"[{item['status']}]"
        print(f"    {item['priority']}. {badge}  {item['item']}  —  {item['value']}")
        _wrap(item["action"], indent=10)
        print()

    print()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_section(title: str):
    print()
    print(f"  {title}")
    print("  " + "─" * (len(title)))


def _wrap(text: str, indent: int = 4, hanging: int = 0, width: int = 76):
    """Simple word-wrap with indent."""
    words = text.split()
    line  = " " * indent
    first = True
    for word in words:
        if len(line) + len(word) + 1 > width:
            print(line)
            line = " " * (indent + (hanging if not first else 0)) + word
        else:
            line += ("" if line.strip() == "" else " ") + word
        first = False
    if line.strip():
        print(line)


def _fb(val) -> str:
    if val is None:
        return "—"
    v = abs(float(val))
    s = f"{v/1e9:.2f}B" if v >= 1e9 else f"{v/1e6:.1f}M" if v >= 1e6 else f"{v:,.0f}"
    return f"({s})" if float(val) < 0 else s
