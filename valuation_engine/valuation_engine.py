"""
Sprint 3C — Valuation Engine

Purpose:
    Discount the projected free cash flows to enterprise value, then bridge
    to equity value and value per share. Run for all three scenarios and
    produce a WACC × terminal growth sensitivity table.

Inputs:
    forecasted output from forecaster.run_forecaster()

Outputs:
    {
        "status":        "pass" | "pass_with_caution" | "fail",
        "valuation": {
            "base": { ... },
            "bull": { ... },
            "bear": { ... },
        },
        "sensitivity":   { ... },    # base-case WACC × terminal growth grid
        "equity_bridge_inputs": { ... },
        "blockers":      [...],
        "warnings":      [...],
        + all pass-throughs
    }

Valuation mechanics:
    1. Discount each FCFF to today:
           PV_yr = FCFF_yr / (1 + WACC)^yr
    2. Terminal value (Gordon Growth Model):
           TV = FCFF_final × (1 + g) / (WACC − g)
    3. PV of TV:
           PV_TV = TV / (1 + WACC)^n
    4. Enterprise value:
           EV = Σ PV_yr + PV_TV
    5. Equity bridge:
           Equity Value = EV − Debt − Lease Liabilities + Cash
    6. Value per share:
           VPS = Equity Value / Shares Outstanding

Equity bridge inputs are pulled from the base year balance sheet.
Shares outstanding from stats (point-in-time).

Sensitivity table:
    Rows:    terminal growth  (base_g − 1.0% to base_g + 1.0%, step 0.5%)
    Columns: WACC             (base_wacc − 2.0% to base_wacc + 2.0%, step 1.0%)
    Values:  value per share (base case FCFs re-discounted at each WACC/g pair)
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_valuation_engine(forecasted: dict) -> dict:
    ticker        = forecasted.get("ticker", "")
    company_name  = forecasted.get("company_name", "")
    stats         = forecasted.get("stats", {})
    years_data    = forecasted.get("canonical_by_year", {})
    norm          = forecasted.get("normalised", {})
    base_year     = forecasted.get("base_year")
    assumptions   = forecasted.get("assumptions", {})
    scenarios     = forecasted.get("scenarios", {})
    forecast      = forecasted.get("forecast", {})
    wacc_components = forecasted.get("wacc_components", {})
    classification = forecasted.get("classification", "")
    template      = forecasted.get("template", "")
    quality_score = forecasted.get("quality_score", 0)
    n_years       = forecasted.get("forecast_years", 5)
    forecast_start = forecasted.get("forecast_start_year")

    blockers: list[str] = []
    warnings: list[str] = []

    if not forecast:
        blockers.append("BLOCKER: No forecast data — forecaster must pass first")
        return _fail(forecasted, blockers)

    # -------------------------------------------------------------------------
    # Extract WACC and terminal growth from base assumptions
    # -------------------------------------------------------------------------
    base_wacc = assumptions.get("wacc", {}).get("value")
    base_g    = assumptions.get("terminal_growth", {}).get("value")

    if base_wacc is None:
        blockers.append("BLOCKER: WACC not found in assumptions")
        return _fail(forecasted, blockers)
    if base_g is None:
        blockers.append("BLOCKER: Terminal growth rate not found in assumptions")
        return _fail(forecasted, blockers)
    if base_wacc <= base_g:
        blockers.append(
            f"BLOCKER: WACC ({base_wacc*100:.1f}%) ≤ terminal growth ({base_g*100:.1f}%) — "
            "terminal value is undefined. Reduce terminal growth or revise WACC."
        )
        return _fail(forecasted, blockers)

    # -------------------------------------------------------------------------
    # Equity bridge inputs — from base year balance sheet
    # -------------------------------------------------------------------------
    bridge_inputs = _get_bridge_inputs(years_data, base_year, norm, stats)

    if bridge_inputs["shares_outstanding"] is not None and bridge_inputs["shares_outstanding"] <= 0:
        warnings.append("WARNING: Shares outstanding ≤ 0 — value per share will be omitted")
        bridge_inputs["shares_outstanding"] = None

    if bridge_inputs["debt"] is None:
        warnings.append(
            "WARNING: Debt not found for equity bridge — treating as zero. "
            "Enterprise value will equal equity value."
        )
        bridge_inputs["debt"] = 0.0

    if bridge_inputs["cash"] is None:
        warnings.append(
            "WARNING: Cash not found for equity bridge — treating as zero."
        )
        bridge_inputs["cash"] = 0.0

    # -------------------------------------------------------------------------
    # Value each scenario
    # -------------------------------------------------------------------------
    valuation: dict[str, dict] = {}

    for scenario_name in ["base", "bull", "bear"]:
        if scenario_name not in forecast:
            continue

        proj      = forecast[scenario_name]
        s_assumptions = scenarios.get(scenario_name, {})
        s_wacc    = s_assumptions.get("wacc", {}).get("value", base_wacc)
        s_g       = s_assumptions.get("terminal_growth", {}).get("value", base_g)

        if s_wacc <= s_g:
            warnings.append(
                f"WARNING [{scenario_name}]: WACC ({s_wacc*100:.1f}%) ≤ terminal growth "
                f"({s_g*100:.1f}%) — skipping this scenario valuation"
            )
            continue

        val = _value_scenario(proj, s_wacc, s_g, n_years, bridge_inputs)
        valuation[scenario_name] = val

    if not valuation:
        blockers.append("BLOCKER: No scenarios could be valued — check WACC and terminal growth")
        return _fail(forecasted, blockers)

    # Terminal value as % of EV check
    base_val = valuation.get("base", {})
    tv_pct   = base_val.get("terminal_value_pct_ev")
    if tv_pct and tv_pct > 0.80:
        warnings.append(
            f"WARNING: Terminal value is {tv_pct*100:.0f}% of enterprise value — "
            "result is highly sensitive to terminal assumptions. "
            "Treat with caution and review sensitivity table."
        )

    # Current price check
    current_price = stats.get("current_price")
    if current_price and base_val.get("value_per_share"):
        implied_return = base_val["value_per_share"] / current_price - 1
        if abs(implied_return) > 0.50:
            warnings.append(
                f"WARNING: Implied return vs current price is {implied_return*100:.0f}% — "
                "large divergence may indicate model error, data issue, or genuine mispricing. "
                "Review all assumptions before drawing conclusions."
            )

    # -------------------------------------------------------------------------
    # Sensitivity table (base FCFs, varying WACC and terminal growth)
    # -------------------------------------------------------------------------
    sensitivity = _build_sensitivity(
        forecast.get("base", {}),
        base_wacc, base_g, n_years, bridge_inputs
    )

    status = "fail" if blockers else ("pass_with_caution" if warnings else "pass")

    return {
        "status":              status,
        "valuation":           valuation,
        "sensitivity":         sensitivity,
        "equity_bridge_inputs": bridge_inputs,
        "blockers":            blockers,
        "warnings":            warnings,
        "ticker":              ticker,
        "company_name":        company_name,
        "stats":               stats,
        "canonical_by_year":   years_data,
        "normalised":          norm,
        "assumptions":         assumptions,
        "scenarios":           scenarios,
        "forecast":            forecast,
        "wacc_components":     wacc_components,
        "base_year":           base_year,
        "classification":      classification,
        "template":            template,
        "quality_score":       quality_score,
        "forecast_years":      n_years,
        "forecast_start_year": forecast_start,
        "metadata": {
            "valued_at":   datetime.utcnow().isoformat() + "Z",
            "base_wacc":   base_wacc,
            "base_g":      base_g,
            "method":      "gordon_growth_model",
        },
    }


# ---------------------------------------------------------------------------
# Core DCF mechanics
# ---------------------------------------------------------------------------

def _value_scenario(proj: dict, wacc: float, g: float,
                    n_years: int, bridge: dict) -> dict:
    """Run a full DCF valuation for one scenario."""
    year_keys = sorted(proj.keys())

    # PV of each forecast year's FCFF
    pv_by_year: dict[str, float] = {}
    total_pv_forecast = 0.0
    fcff_final        = None

    for i, yk in enumerate(year_keys):
        fcff = proj[yk].get("fcff")
        if fcff is None:
            continue
        t  = i + 1
        pv = fcff / (1 + wacc) ** t
        pv_by_year[yk] = round(pv, 2)
        total_pv_forecast += pv
        fcff_final = fcff

    # Terminal value — Gordon Growth on final year FCFF
    terminal_value    = fcff_final * (1 + g) / (wacc - g)
    pv_terminal_value = terminal_value / (1 + wacc) ** n_years

    # Enterprise value
    enterprise_value = total_pv_forecast + pv_terminal_value

    # TV as share of EV
    tv_pct_ev = pv_terminal_value / enterprise_value if enterprise_value != 0 else None

    # Equity bridge
    debt              = bridge.get("debt", 0) or 0
    lease_liabilities = bridge.get("lease_liabilities", 0) or 0
    cash              = bridge.get("cash", 0) or 0
    minority_interests = bridge.get("minority_interests", 0) or 0

    equity_value = (enterprise_value
                    - debt
                    - lease_liabilities
                    + cash
                    - minority_interests)

    # Value per share
    shares = bridge.get("shares_outstanding")
    value_per_share = equity_value / shares if shares else None

    # Implied return vs current price
    current_price   = bridge.get("current_price")
    implied_return  = (value_per_share / current_price - 1) if (
        value_per_share and current_price and current_price > 0
    ) else None

    return {
        "pv_forecast_period":  round(total_pv_forecast, 2),
        "terminal_value":      round(terminal_value, 2),
        "pv_terminal_value":   round(pv_terminal_value, 2),
        "terminal_value_pct_ev": round(tv_pct_ev, 4) if tv_pct_ev else None,
        "enterprise_value":    round(enterprise_value, 2),
        "equity_bridge": {
            "enterprise_value":    round(enterprise_value, 2),
            "less_debt":           round(-debt, 2),
            "less_lease_liabilities": round(-lease_liabilities, 2),
            "plus_cash":           round(cash, 2),
            "less_minority":       round(-minority_interests, 2),
            "equity_value":        round(equity_value, 2),
        },
        "shares_outstanding":  shares,
        "value_per_share":     round(value_per_share, 2) if value_per_share else None,
        "current_price":       current_price,
        "implied_return":      round(implied_return, 4) if implied_return is not None else None,
        "wacc_used":           round(wacc, 4),
        "terminal_growth_used": round(g, 4),
        "pv_by_year":          pv_by_year,
    }


# ---------------------------------------------------------------------------
# Equity bridge inputs
# ---------------------------------------------------------------------------

def _get_bridge_inputs(years_data: dict, base_year: str,
                       norm: dict, stats: dict) -> dict:
    """Pull debt, cash, leases, shares from the base year balance sheet."""
    by = years_data.get(base_year, {}) if base_year else {}

    debt              = by.get("debt")        or norm.get("debt")
    cash              = by.get("cash")        or norm.get("cash")
    lease_liabilities = by.get("lease_liabilities")
    minority_interests = None   # not separately scraped — reserved for future

    # Shares: prefer canonical year data, fall back to stats
    shares = by.get("shares_outstanding") or stats.get("shares_outstanding")

    # Current price from stats
    current_price = stats.get("current_price")

    return {
        "debt":               debt,
        "lease_liabilities":  lease_liabilities or 0.0,
        "cash":               cash,
        "minority_interests": minority_interests or 0.0,
        "shares_outstanding": shares,
        "current_price":      current_price,
        "source_year":        base_year,
    }


# ---------------------------------------------------------------------------
# Sensitivity table
# ---------------------------------------------------------------------------

def _build_sensitivity(base_proj: dict, base_wacc: float, base_g: float,
                       n_years: int, bridge: dict) -> dict:
    """
    5×5 sensitivity table: WACC (cols) × terminal growth (rows).
    Values are value per share.
    """
    wacc_steps   = [-0.02, -0.01, 0.00, +0.01, +0.02]
    growth_steps = [-0.010, -0.005, 0.000, +0.005, +0.010]

    wacc_values   = [round(base_wacc + s, 4) for s in wacc_steps]
    growth_values = [round(base_g    + s, 4) for s in growth_steps]

    table: dict[str, dict[str, float | None]] = {}

    for g in growth_values:
        g_key = f"{g*100:.1f}%"
        table[g_key] = {}
        for w in wacc_values:
            w_key = f"{w*100:.1f}%"
            if w <= g:
                table[g_key][w_key] = None   # undefined — WACC ≤ g
                continue
            val = _value_scenario(base_proj, w, g, n_years, bridge)
            table[g_key][w_key] = val.get("value_per_share")

    return {
        "wacc_values":   [f"{w*100:.1f}%" for w in wacc_values],
        "growth_values": [f"{g*100:.1f}%" for g in growth_values],
        "table":         table,
        "note": (
            "Base case FCFs used throughout. "
            "Rows = terminal growth rate, Columns = WACC."
        ),
    }


# ---------------------------------------------------------------------------
# Fail result
# ---------------------------------------------------------------------------

def _fail(forecasted: dict, blockers: list) -> dict:
    return {
        "status":              "fail",
        "valuation":           {},
        "sensitivity":         {},
        "equity_bridge_inputs": {},
        "blockers":            blockers,
        "warnings":            [],
        "ticker":              forecasted.get("ticker", ""),
        "company_name":        forecasted.get("company_name", ""),
        "stats":               forecasted.get("stats", {}),
        "canonical_by_year":   forecasted.get("canonical_by_year", {}),
        "normalised":          forecasted.get("normalised", {}),
        "assumptions":         forecasted.get("assumptions", {}),
        "scenarios":           forecasted.get("scenarios", {}),
        "forecast":            forecasted.get("forecast", {}),
        "wacc_components":     forecasted.get("wacc_components", {}),
        "base_year":           forecasted.get("base_year"),
        "classification":      forecasted.get("classification", ""),
        "template":            forecasted.get("template", ""),
        "quality_score":       forecasted.get("quality_score", 0),
        "forecast_years":      forecasted.get("forecast_years", 5),
        "forecast_start_year": forecasted.get("forecast_start_year"),
        "metadata":            {"valued_at": datetime.utcnow().isoformat() + "Z"},
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_valuation_report(result: dict) -> None:
    status  = result["status"]
    company = result["company_name"]
    ticker  = result["ticker"]

    status_label = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}
    print(f"\n{'='*60}")
    print(f"  Valuation Engine — {company} ({ticker})")
    print(f"  Status   : [{status_label.get(status, status)}]")
    print(f"  Method   : Gordon Growth Model (perpetuity)")
    print(f"{'='*60}")

    if result["blockers"]:
        print("\n  Blockers:")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["warnings"]:
        print("\n  Warnings:")
        for w in result["warnings"]:
            print(f"    {w}")

    val   = result.get("valuation", {})
    bridge = result.get("equity_bridge_inputs", {})

    if not val:
        print(f"{'='*60}\n")
        return

    # ---- Scenario summary table ----
    scenarios = ["bear", "base", "bull"]
    print(f"\n  {'Scenario':<12} {'EV':>12} {'Equity Value':>14} {'Per Share':>12} {'vs Price':>10}")
    print(f"  {'-'*62}")
    for sc in scenarios:
        v = val.get(sc)
        if not v:
            continue
        ir = v.get("implied_return")
        vs_price = f"{ir*100:+.0f}%" if ir is not None else "—"
        print(f"  {sc.upper():<12} "
              f"{_fmt(v.get('enterprise_value')):>12} "
              f"{_fmt(v.get('equity_bridge',{}).get('equity_value')):>14} "
              f"{_fmt_share(v.get('value_per_share')):>12} "
              f"{vs_price:>10}")

    current_price = bridge.get("current_price")
    if current_price:
        print(f"\n  Current price  : {_fmt_share(current_price)}")

    # ---- Base case DCF breakdown ----
    base_v = val.get("base", {})
    if base_v:
        print(f"\n  Base case DCF breakdown:")
        print(f"    PV of forecast period : {_fmt(base_v.get('pv_forecast_period'))}")
        print(f"    PV of terminal value  : {_fmt(base_v.get('pv_terminal_value'))}")
        tv_pct = base_v.get("terminal_value_pct_ev")
        print(f"    Terminal value / EV   : {_pct(tv_pct)}")
        print(f"    Enterprise value      : {_fmt(base_v.get('enterprise_value'))}")

        bridge_detail = base_v.get("equity_bridge", {})
        print(f"\n  Equity bridge (base):")
        print(f"    Enterprise value      : {_fmt(bridge_detail.get('enterprise_value'))}")
        print(f"    Less: debt            : {_fmt(bridge_detail.get('less_debt'))}")
        ll = bridge_detail.get("less_lease_liabilities")
        if ll and ll != 0:
            print(f"    Less: lease liabilities: {_fmt(ll)}")
        print(f"    Plus: cash            : {_fmt(bridge_detail.get('plus_cash'))}")
        print(f"    = Equity value        : {_fmt(bridge_detail.get('equity_value'))}")
        shares = base_v.get("shares_outstanding")
        print(f"    ÷ Shares outstanding  : {_fmt(shares)}")
        print(f"    = Value per share     : {_fmt_share(base_v.get('value_per_share'))}")

    # ---- Sensitivity table ----
    sens = result.get("sensitivity", {})
    if sens and "table" in sens:
        wacc_vals  = sens["wacc_values"]
        growth_vals = sens["growth_values"]
        table      = sens["table"]

        col_w = 10
        print(f"\n  Sensitivity — Value per share (base FCFs)")
        print(f"  WACC →")
        header = f"  {'g ↓':<8}" + "".join(f"{w:>{col_w}}" for w in wacc_vals)
        print(header)
        print("  " + "-" * (8 + col_w * len(wacc_vals)))

        for g_key in growth_vals:
            row = f"  {g_key:<8}"
            for w_key in wacc_vals:
                vps = table.get(g_key, {}).get(w_key)
                cell = _fmt_share(vps) if vps is not None else "  n/a"
                # Highlight the base cell
                is_base = (g_key == f"{result['metadata'].get('base_g',0)*100:.1f}%" and
                           w_key == f"{result['metadata'].get('base_wacc',0)*100:.1f}%")
                row += f"{'['+cell+']' if is_base else cell:>{col_w}}"
            print(row)

        print(f"  {sens.get('note', '')}")

    print(f"\n  Valued at : {result['metadata'].get('valued_at', '—')}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt(val) -> str:
    if val is None:
        return "—"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    neg = v < 0
    v = abs(v)
    if v >= 1e12:
        s = f"{v/1e12:.2f}T"
    elif v >= 1e9:
        s = f"{v/1e9:.2f}B"
    elif v >= 1e6:
        s = f"{v/1e6:.0f}M"
    else:
        s = f"{v:,.0f}"
    return f"({s})" if neg else s


def _fmt_share(val) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return str(val)


def _pct(val) -> str:
    if val is None:
        return "—"
    return f"{float(val)*100:.1f}%"
