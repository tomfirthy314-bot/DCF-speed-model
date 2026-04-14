"""
Sprint 3B — Forecaster

Purpose:
    Translate the assumptions pack into a year-by-year projected income
    statement and free cash flow for each scenario (base, bull, bear).

    The forecaster is deliberately mechanical — it applies the assumptions
    exactly and does not second-guess them. All judgement was exercised in
    the assumption engine. Coherence checks flag any economically implausible
    outputs so they can be investigated before valuation.

Inputs:
    assumed output from assumption_engine.run_assumption_engine()

Outputs:
    {
        "status":        "pass" | "pass_with_caution" | "fail",
        "forecast": {
            "base": { "year_1": {...}, "year_2": {...}, ..., "year_N": {...} },
            "bull": { ... },
            "bear": { ... },
        },
        "base_year":        str,
        "forecast_start_year": int,   # calendar year of Year 1
        "coherence_checks": [...],
        "blockers":         [...],
        "warnings":         [...],
        + all pass-throughs (stats, canonical_by_year, normalised, assumptions, etc.)
    }

Free cash flow to firm (FCFF) formula:
    FCFF = NOPAT + D&A - Capex - ΔNWC

    where:
        NOPAT     = EBIT × (1 − tax_rate)          [if EBIT < 0, tax = 0]
        D&A       = Revenue × da_pct_revenue
        Capex     = Revenue × capex_pct_revenue     [positive; sign flipped in FCFF]
        ΔNWC      = (Revenue − Prior Revenue) × nwc_pct_revenue
                    [positive = NWC increased = cash outflow]

Sign convention in forecast output:
    revenue        > 0
    ebit           can be negative (loss-making)
    tax            ≥ 0 (no negative tax in this model)
    nopat          can be negative
    da             > 0 (non-cash add-back)
    capex          < 0 (cash outflow)
    delta_nwc      sign varies (positive = outflow when growing, negative = inflow when shrinking)
    fcff           can be negative
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_forecaster(assumed: dict) -> dict:
    ticker        = assumed.get("ticker", "")
    company_name  = assumed.get("company_name", "")
    stats         = assumed.get("stats", {})
    years_data    = assumed.get("canonical_by_year", {})
    norm          = assumed.get("normalised", {})
    base_year     = assumed.get("base_year")
    classification = assumed.get("classification", "hybrid")
    template      = assumed.get("template", "hybrid")
    quality_score = assumed.get("quality_score", 0)
    assumptions   = assumed.get("assumptions", {})
    scenarios     = assumed.get("scenarios", {})
    n_years       = assumed.get("forecast_years", 5)
    wacc_components = assumed.get("wacc_components", {})

    blockers: list[str] = []
    warnings: list[str] = []

    if not assumptions:
        blockers.append("BLOCKER: No assumptions received — assumption engine must pass first")
        return _fail(assumed, blockers)

    # Base year revenue and NWC as starting points
    base_revenue = norm.get("revenue")
    base_nwc     = norm.get("nwc")    # may be None

    if base_revenue is None:
        blockers.append("BLOCKER: Base year revenue missing — cannot project forward")
        return _fail(assumed, blockers)

    forecast_start_year = (int(base_year) + 1) if base_year else datetime.utcnow().year + 1

    # -------------------------------------------------------------------------
    # Run forecast for each scenario
    # -------------------------------------------------------------------------
    forecast: dict[str, dict] = {}
    all_coherence: list[dict] = []

    for scenario_name, scenario_assumptions in scenarios.items():
        projected, coherence_issues = _project(
            scenario_name      = scenario_name,
            assumptions        = scenario_assumptions,
            base_revenue       = base_revenue,
            base_nwc           = base_nwc,
            n_years            = n_years,
            forecast_start_year = forecast_start_year,
        )
        forecast[scenario_name]    = projected
        all_coherence.extend(coherence_issues)

    # Surface coherence warnings
    for issue in all_coherence:
        if issue["severity"] == "blocker":
            blockers.append(f"BLOCKER [{issue['scenario']}]: {issue['message']}")
        else:
            warnings.append(f"WARNING [{issue['scenario']}]: {issue['message']}")

    status = "fail" if blockers else ("pass_with_caution" if warnings else "pass")

    return {
        "status":             status,
        "forecast":           forecast,
        "base_year":          base_year,
        "forecast_start_year": forecast_start_year,
        "coherence_checks":   all_coherence,
        "blockers":           blockers,
        "warnings":           warnings,
        "ticker":             ticker,
        "company_name":       company_name,
        "stats":              stats,
        "canonical_by_year":  years_data,
        "normalised":         norm,
        "assumptions":        assumptions,
        "scenarios":          scenarios,
        "wacc_components":    wacc_components,
        "classification":     classification,
        "template":           template,
        "quality_score":      quality_score,
        "forecast_years":     n_years,
        "metadata": {
            "forecasted_at":    datetime.utcnow().isoformat() + "Z",
            "base_year":        base_year,
            "forecast_start":   forecast_start_year,
            "forecast_end":     forecast_start_year + n_years - 1,
            "scenarios":        list(scenarios.keys()),
        },
    }


# ---------------------------------------------------------------------------
# Projection engine — one scenario at a time
# ---------------------------------------------------------------------------

def _project(scenario_name: str, assumptions: dict, base_revenue: float,
             base_nwc: float | None, n_years: int,
             forecast_start_year: int) -> tuple[dict, list[dict]]:
    """
    Project n_years of financials from the assumptions.
    Returns (year_dict, coherence_issues).
    """
    # Extract flat assumptions
    tax_rate    = assumptions.get("tax_rate", {}).get("value", 0.25)
    da_pct      = assumptions.get("da_pct_revenue", {}).get("value", 0.04)
    capex_pct   = assumptions.get("capex_pct_revenue", {}).get("value", 0.05)
    nwc_pct     = assumptions.get("nwc_pct_revenue", {}).get("value", 0.05)

    rg_entry    = assumptions.get("revenue_growth", {})
    em_entry    = assumptions.get("ebit_margin", {})

    projected:   dict[str, dict] = {}
    coherence:   list[dict]      = []

    prior_revenue = base_revenue
    prior_nwc     = base_nwc if base_nwc is not None else base_revenue * nwc_pct

    for i in range(n_years):
        yr_key   = f"year_{i + 1}"
        cal_year = forecast_start_year + i

        # --- Revenue ---
        growth  = rg_entry.get(yr_key, 0.02)
        revenue = prior_revenue * (1 + growth)

        # --- EBIT ---
        margin  = em_entry.get(yr_key, 0.10)
        ebit    = revenue * margin

        # --- Tax and NOPAT ---
        # Only apply tax when EBIT is positive (no negative tax modelled here)
        tax     = ebit * tax_rate if ebit > 0 else 0.0
        nopat   = ebit - tax

        # --- D&A ---
        da      = revenue * da_pct

        # --- Capex (stored as negative — cash outflow) ---
        capex   = -(revenue * capex_pct)

        # --- Change in NWC ---
        # NWC = revenue × nwc_pct
        # ΔNWC = NWC_current − NWC_prior
        # A positive ΔNWC means working capital increased → cash outflow
        nwc_current = revenue * nwc_pct
        delta_nwc   = nwc_current - prior_nwc

        # --- FCFF ---
        # FCFF = NOPAT + D&A − Capex_absolute − ΔNWC
        capex_abs = abs(capex)
        fcff      = nopat + da - capex_abs - delta_nwc

        # --- Derived ratios ---
        cash_conversion = (fcff / nopat) if nopat != 0 else None
        fcff_margin     = fcff / revenue if revenue != 0 else None

        projected[yr_key] = {
            "calendar_year":       cal_year,
            "revenue":             _r(revenue),
            "revenue_growth":      _r(growth),
            "ebit_margin":         _r(margin),
            "ebit":                _r(ebit),
            "tax":                 _r(tax),
            "nopat":               _r(nopat),
            "da":                  _r(da),
            "da_pct_revenue":      _r(da_pct),
            "capex":               _r(capex),
            "capex_pct_revenue":   _r(capex_pct),
            "delta_nwc":           _r(delta_nwc),
            "nwc_pct_revenue":     _r(nwc_pct),
            "fcff":                _r(fcff),
            "fcff_margin":         _r(fcff_margin),
            "cash_conversion":     _r(cash_conversion),
        }

        # --- Coherence checks for this year ---
        _check_year_coherence(
            coherence, scenario_name, yr_key, cal_year,
            revenue, ebit, margin, nopat, da, capex_abs, delta_nwc,
            fcff, cash_conversion,
        )

        prior_revenue = revenue
        prior_nwc     = nwc_current

    return projected, coherence


# ---------------------------------------------------------------------------
# Coherence checks — per year
# ---------------------------------------------------------------------------

def _check_year_coherence(issues: list, scenario: str, yr_key: str, cal_year: int,
                          revenue, ebit, margin, nopat, da,
                          capex_abs, delta_nwc, fcff, cash_conversion):
    def _flag(msg: str, severity: str = "warning"):
        issues.append({
            "scenario": scenario,
            "year":     yr_key,
            "cal_year": cal_year,
            "message":  msg,
            "severity": severity,
        })

    # Revenue must be positive
    if revenue <= 0:
        _flag(f"{cal_year}: Revenue ≤ 0 ({_fmt(revenue)}) — growth path has driven revenue negative",
              severity="blocker")

    # D&A must be positive
    if da <= 0:
        _flag(f"{cal_year}: D&A ≤ 0 — check da_pct_revenue assumption")

    # Capex must be positive (absolute value)
    if capex_abs <= 0:
        _flag(f"{cal_year}: Capex absolute value ≤ 0 — check capex_pct_revenue assumption")

    # Negative FCFF while NOPAT is strongly positive — cash drain worth flagging
    if fcff < 0 and nopat > 0 and abs(fcff) > nopat * 0.5:
        _flag(
            f"{cal_year}: FCFF ({_fmt(fcff)}) is negative despite positive NOPAT ({_fmt(nopat)}) — "
            "capex or NWC investment is consuming >50% of operating profit"
        )

    # Cash conversion ratio check
    if cash_conversion is not None:
        if cash_conversion < -0.5:
            _flag(
                f"{cal_year}: Cash conversion ratio {cash_conversion:.2f} — "
                "FCFF is deeply negative relative to NOPAT; check capex/NWC assumptions"
            )
        if cash_conversion > 2.0 and nopat > 0:
            _flag(
                f"{cal_year}: Cash conversion ratio {cash_conversion:.2f} — "
                "FCFF > 2× NOPAT; unusually high cash conversion, verify D&A and NWC"
            )

    # EBIT margin sanity
    if margin > 0.60:
        _flag(f"{cal_year}: EBIT margin {margin*100:.1f}% exceeds 60% — verify assumption")
    if margin < -0.30:
        _flag(
            f"{cal_year}: EBIT margin {margin*100:.1f}% — deeply loss-making; "
            "confirm this reflects the intended scenario"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _r(val) -> float | None:
    """Round to 2 decimal places for currency, or 6 for ratios."""
    if val is None:
        return None
    if isinstance(val, float) and abs(val) < 10:
        return round(val, 6)
    return round(val, 2)


def _fail(assumed: dict, blockers: list) -> dict:
    return {
        "status":             "fail",
        "forecast":           {},
        "base_year":          assumed.get("base_year"),
        "forecast_start_year": None,
        "coherence_checks":   [],
        "blockers":           blockers,
        "warnings":           [],
        "ticker":             assumed.get("ticker", ""),
        "company_name":       assumed.get("company_name", ""),
        "stats":              assumed.get("stats", {}),
        "canonical_by_year":  assumed.get("canonical_by_year", {}),
        "normalised":         assumed.get("normalised", {}),
        "assumptions":        assumed.get("assumptions", {}),
        "scenarios":          assumed.get("scenarios", {}),
        "wacc_components":    assumed.get("wacc_components", {}),
        "classification":     assumed.get("classification", ""),
        "template":           assumed.get("template", ""),
        "quality_score":      assumed.get("quality_score", 0),
        "forecast_years":     assumed.get("forecast_years", 5),
        "metadata":           {"forecasted_at": datetime.utcnow().isoformat() + "Z"},
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_forecaster_report(result: dict) -> None:
    status  = result["status"]
    company = result["company_name"]
    ticker  = result["ticker"]
    n_years = result["forecast_years"]
    start   = result.get("forecast_start_year", "?")

    status_label = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}
    print(f"\n{'='*60}")
    print(f"  Forecaster — {company} ({ticker})")
    print(f"  Status   : [{status_label.get(status, status)}]")
    print(f"  Period   : {start}–{start + n_years - 1 if isinstance(start, int) else '?'}")
    print(f"{'='*60}")

    if result["blockers"]:
        print("\n  Blockers:")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["warnings"]:
        print("\n  Warnings:")
        for w in result["warnings"]:
            print(f"    {w}")

    forecast = result.get("forecast", {})
    if not forecast:
        print(f"{'='*60}\n")
        return

    # Print a table for each scenario
    for scenario in ["base", "bull", "bear"]:
        if scenario not in forecast:
            continue
        proj = forecast[scenario]
        years_keys = sorted(proj.keys())

        cal_years = [proj[k]["calendar_year"] for k in years_keys]
        col_w = 12

        header_fmt = "{:<24}" + (f"{{:>{col_w}}}" * len(years_keys))
        print(f"\n  [{scenario.upper()} CASE]")
        print("  " + header_fmt.format("", *cal_years))
        print("  " + "-" * (24 + col_w * len(years_keys)))

        rows = [
            ("Revenue",          "revenue",        _fmt),
            ("Revenue growth",   "revenue_growth", _pct),
            ("EBIT margin",      "ebit_margin",    _pct),
            ("EBIT",             "ebit",           _fmt),
            ("Tax",              "tax",            _fmt),
            ("NOPAT",            "nopat",          _fmt),
            ("D&A",              "da",             _fmt),
            ("Capex",            "capex",          _fmt),
            ("Δ NWC",            "delta_nwc",      _fmt),
            ("FCFF",             "fcff",           _fmt),
            ("FCFF margin",      "fcff_margin",    _pct),
            ("Cash conversion",  "cash_conversion", _ratio),
        ]

        for label, field, fmt_fn in rows:
            vals = [fmt_fn(proj[k].get(field)) for k in years_keys]
            print("  " + header_fmt.format(label, *vals))

    # Coherence check summary
    issues = result.get("coherence_checks", [])
    non_blocker = [i for i in issues if i["severity"] != "blocker"]
    if non_blocker:
        print(f"\n  Coherence flags ({len(non_blocker)}):")
        for i in non_blocker[:5]:   # show up to 5
            print(f"    [{i['scenario']} {i['year']}] {i['message']}")
        if len(non_blocker) > 5:
            print(f"    ... and {len(non_blocker)-5} more")

    print(f"\n  Forecasted at : {result['metadata'].get('forecasted_at', '—')}")
    print(f"{'='*60}\n")


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
        s = f"{v/1e12:.1f}T"
    elif v >= 1e9:
        s = f"{v/1e9:.1f}B"
    elif v >= 1e6:
        s = f"{v/1e6:.0f}M"
    else:
        s = f"{v:,.0f}"
    return f"({s})" if neg else s


def _pct(val) -> str:
    if val is None:
        return "—"
    return f"{float(val)*100:.1f}%"


def _ratio(val) -> str:
    if val is None:
        return "—"
    return f"{float(val):.2f}x"
