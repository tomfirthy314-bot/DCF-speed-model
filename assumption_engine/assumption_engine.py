"""
Sprint 3A — Assumption Engine

Purpose:
    Generate a complete, explicit, and fully-tagged set of forecast assumptions
    from historical data, classification, and rule-based defaults.

    No assumption is invented without a label. Every driver has a source tag,
    a method description, and a plain-English rationale. Analyst-override fields
    are reserved for future manual input.

Inputs:
    classified output from classifier.run_classifier()
    (which carries normalised metrics, stats, canonical_by_year, and classification)

Outputs:
    {
        "status":       "pass" | "pass_with_caution" | "fail",
        "assumptions":  { ... },     # full base-case assumptions pack
        "scenarios": {
            "base":     { ... },     # same as assumptions
            "bull":     { ... },     # optimistic variant
            "bear":     { ... },     # pessimistic variant
        },
        "forecast_years": int,
        "wacc_components": { ... },  # transparent WACC build
        "blockers":     [...],
        "warnings":     [...],
        "ticker":       str,
        "company_name": str,
        "stats":        {...},       # pass-through
        "canonical_by_year": {...},  # pass-through
        "normalised":   {...},       # pass-through
        "base_year":    str,
        "classification": str,
        "template":     str,
        "quality_score": float,
        "metadata":     {...},
    }

Assumption structure — every driver has:
    value / path   — the actual number(s)
    method         — how it was derived
    tag            — "sourced" | "rule_based" | "calibrated" | "default"
    rationale      — plain-English explanation

Tags:
    sourced        — taken directly from scraped data
    rule_based     — derived from historical data via a defined rule
    calibrated     — adjusted from a rule-based starting point using classification
    default        — no data available; a standard default was applied

WACC note:
    Risk-free rate and equity risk premium use country-based lookup tables
    (approximate values as of early 2026). These MUST be confirmed by an analyst
    against current market rates before any real valuation decision is made.

Forecast horizon: 5 years (Years 1–5), then terminal value.
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Country defaults (approximate as of early 2026 — MUST be confirmed by analyst)
# Source: approximate 10yr government bond yields + Damodaran ERP estimates
# ---------------------------------------------------------------------------

_COUNTRY_DEFAULTS: dict[str, dict] = {
    "United Kingdom":   {"rf": 0.043, "erp": 0.052, "terminal_gdp": 0.020},
    "United States":    {"rf": 0.043, "erp": 0.047, "terminal_gdp": 0.025},
    "Germany":          {"rf": 0.027, "erp": 0.055, "terminal_gdp": 0.018},
    "France":           {"rf": 0.033, "erp": 0.057, "terminal_gdp": 0.018},
    "Netherlands":      {"rf": 0.027, "erp": 0.054, "terminal_gdp": 0.020},
    "Switzerland":      {"rf": 0.008, "erp": 0.052, "terminal_gdp": 0.015},
    "Sweden":           {"rf": 0.025, "erp": 0.055, "terminal_gdp": 0.018},
    "Japan":            {"rf": 0.015, "erp": 0.062, "terminal_gdp": 0.010},
    "China":            {"rf": 0.025, "erp": 0.082, "terminal_gdp": 0.040},
    "India":            {"rf": 0.068, "erp": 0.092, "terminal_gdp": 0.055},
    "Australia":        {"rf": 0.043, "erp": 0.052, "terminal_gdp": 0.025},
    "Canada":           {"rf": 0.033, "erp": 0.050, "terminal_gdp": 0.022},
    "Brazil":           {"rf": 0.135, "erp": 0.110, "terminal_gdp": 0.030},
    "South Korea":      {"rf": 0.030, "erp": 0.065, "terminal_gdp": 0.025},
    "Taiwan":           {"rf": 0.015, "erp": 0.070, "terminal_gdp": 0.025},
    # Default fallback for unknown countries
    "_default":         {"rf": 0.040, "erp": 0.060, "terminal_gdp": 0.025},
}

# ---------------------------------------------------------------------------
# Template-based guardrails for revenue growth and margins
# These bound the rule-based outputs to economically sensible ranges per type
# ---------------------------------------------------------------------------

_TEMPLATE_BOUNDS: dict[str, dict] = {
    "asset_light": {
        "growth_floor": 0.00, "growth_cap": 0.25,
        "margin_floor": 0.05, "margin_cap": 0.50,
        "terminal_premium": 0.005,   # terminal growth slight premium to GDP
    },
    "industrial": {
        "growth_floor": -0.02, "growth_cap": 0.15,
        "margin_floor": 0.03, "margin_cap": 0.30,
        "terminal_premium": 0.000,
    },
    "consumer": {
        "growth_floor": -0.02, "growth_cap": 0.15,
        "margin_floor": 0.02, "margin_cap": 0.35,
        "terminal_premium": 0.000,
    },
    "resources": {
        "growth_floor": -0.05, "growth_cap": 0.12,
        "margin_floor": 0.00, "margin_cap": 0.40,
        "terminal_premium": -0.005,  # terminal below GDP (commodity maturity)
    },
    "utilities": {
        "growth_floor": 0.00, "growth_cap": 0.08,
        "margin_floor": 0.05, "margin_cap": 0.45,
        "terminal_premium": -0.005,
    },
    "hybrid": {
        "growth_floor": -0.02, "growth_cap": 0.18,
        "margin_floor": 0.02, "margin_cap": 0.40,
        "terminal_premium": 0.000,
    },
}

_FORECAST_YEARS = 5
_DEFAULT_BETA    = 1.0
_DEFAULT_COD     = 0.055    # 5.5% pre-tax cost of debt if not computable
_MAX_COD         = 0.15
_MIN_COD         = 0.02

# Blume beta adjustment toward 1.0: adjusted = (2/3 × raw) + (1/3 × 1.0)
# Reflects empirical mean-reversion of betas over time (Blume, 1975).
_BLUME_WEIGHT    = 2 / 3

# Scenario deltas — standard (low TV concentration) and tight (high TV concentration).
# When TV% > _TV_TIGHT_THRESHOLD, tighter deltas prevent disproportionate scenario spreads.
_TV_TIGHT_THRESHOLD      = 0.75   # above this TV%, use tight deltas
_SCENARIO_GROWTH_STD     = 0.015  # ±1.5% revenue growth per year (standard)
_SCENARIO_GROWTH_TIGHT   = 0.008  # ±0.8% (tight)
_SCENARIO_MARGIN_STD     = 0.010  # ±100bps EBIT margin by Year 5 (standard)
_SCENARIO_MARGIN_TIGHT   = 0.005  # ±50bps (tight)
_SCENARIO_WACC_BEAR_STD  = 0.010  # bear WACC +100bps (standard)
_SCENARIO_WACC_BEAR_TIGHT = 0.005 # bear WACC +50bps (tight)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_assumption_engine(classified: dict) -> dict:
    ticker        = classified.get("ticker", "")
    company_name  = classified.get("company_name", "")
    stats         = classified.get("stats", {})
    years_data    = classified.get("canonical_by_year", {})
    norm          = classified.get("normalised", {})
    base_year     = classified.get("base_year")
    classification = classified.get("classification", "hybrid")
    template      = classified.get("template", "hybrid")
    quality_score = classified.get("quality_score", 0)

    years = sorted(years_data.keys(), reverse=True)

    blockers: list[str] = []
    warnings: list[str] = []

    bounds  = _TEMPLATE_BOUNDS.get(template, _TEMPLATE_BOUNDS["hybrid"])
    country = stats.get("country", "") or ""
    c_def   = _COUNTRY_DEFAULTS.get(country, _COUNTRY_DEFAULTS["_default"])

    if country not in _COUNTRY_DEFAULTS:
        warnings.append(
            f"WARNING: Country '{country}' not in lookup table — "
            "using global default Rf/ERP. Analyst must confirm against current market rates."
        )

    # -------------------------------------------------------------------------
    # 1. Revenue growth path
    # -------------------------------------------------------------------------
    hist_cagr, growth_method = _calc_historical_cagr(years_data, years)

    if hist_cagr is None:
        blockers.append(
            "BLOCKER: Cannot compute historical revenue growth — "
            "need at least 2 years of revenue data"
        )
        return _fail(ticker, company_name, stats, years_data, norm,
                     base_year, classification, template, quality_score, blockers)

    # Clamp to template bounds
    g0 = max(bounds["growth_floor"], min(bounds["growth_cap"], hist_cagr))
    terminal_growth = c_def["terminal_gdp"] + bounds["terminal_premium"]

    # Linear fade from g0 in year 1 to terminal_growth by year 5
    growth_path = _linear_fade(g0, terminal_growth, _FORECAST_YEARS)

    growth_tag = "rule_based" if hist_cagr == g0 else "calibrated"
    growth_rationale = (
        f"Historical revenue CAGR: {hist_cagr*100:.1f}% ({growth_method}). "
        f"Clamped to template bounds [{bounds['growth_floor']*100:.0f}%, "
        f"{bounds['growth_cap']*100:.0f}%] → {g0*100:.1f}% in Year 1. "
        f"Fades linearly to terminal growth {terminal_growth*100:.1f}% by Year {_FORECAST_YEARS}."
    )

    revenue_growth_assumption = {
        **{f"year_{i+1}": round(v, 4) for i, v in enumerate(growth_path)},
        "method":    growth_method,
        "tag":       growth_tag,
        "rationale": growth_rationale,
    }

    # -------------------------------------------------------------------------
    # 2. EBIT margin path
    # -------------------------------------------------------------------------
    norm_margin     = norm.get("ebit_margin")
    hist_peak_margin = _calc_peak_margin(years_data, years)

    if norm_margin is None:
        blockers.append("BLOCKER: Normalised EBIT margin missing — cannot set margin assumptions")
        return _fail(ticker, company_name, stats, years_data, norm,
                     base_year, classification, template, quality_score, blockers)

    # If there is meaningful headroom to historical peak, allow gradual expansion
    expansion_room = (hist_peak_margin - norm_margin) if hist_peak_margin else 0
    if expansion_room > 0.02:
        target_margin = norm_margin + min(expansion_room * 0.5, 0.03)  # recover up to 50% of gap, max 3pts
        margin_note   = (
            f"Historical peak margin {hist_peak_margin*100:.1f}% suggests "
            f"{expansion_room*100:.1f}pts of recovery headroom — "
            f"assuming 50% recovery to {target_margin*100:.1f}% by Year {_FORECAST_YEARS}."
        )
    else:
        target_margin = norm_margin
        margin_note   = "No material headroom to historical peak — margin held flat."

    target_margin = max(bounds["margin_floor"], min(bounds["margin_cap"], target_margin))
    margin_path   = _linear_fade(norm_margin, target_margin, _FORECAST_YEARS)

    ebit_margin_assumption = {
        **{f"year_{i+1}": round(v, 4) for i, v in enumerate(margin_path)},
        "method":    "normalised_base_with_peak_mean_reversion",
        "tag":       "rule_based",
        "rationale": (
            f"Base: normalised EBIT margin {norm_margin*100:.1f}%. {margin_note}"
        ),
    }

    # -------------------------------------------------------------------------
    # 3. Tax rate
    # -------------------------------------------------------------------------
    tax_rate    = norm.get("tax_rate")
    tax_entry   = norm.get("tax_rate")
    if tax_rate is None:
        tax_rate   = 0.25
        tax_method = "rule_based_default"
        tax_tag    = "default"
        tax_note   = "No tax data available — defaulting to 25%. Analyst override recommended."
        warnings.append("WARNING: Tax rate defaulted to 25% — no historical tax data available")
    else:
        tax_method = "sourced_from_normaliser"
        tax_tag    = "rule_based"
        tax_note   = f"Effective tax rate {tax_rate*100:.1f}% from normaliser (median of historical rates)."

    tax_assumption = {
        "value":     round(tax_rate, 4),
        "method":    tax_method,
        "tag":       tax_tag,
        "rationale": tax_note,
    }

    # -------------------------------------------------------------------------
    # 4. D&A as % of revenue
    # -------------------------------------------------------------------------
    da_pct = norm.get("da_pct_revenue")
    if da_pct is not None:
        da_assumption = {
            "value":     round(da_pct, 4),
            "method":    "sourced_from_normaliser",
            "tag":       "rule_based",
            "rationale": f"D&A held at {da_pct*100:.1f}% of revenue (normalised median).",
        }
    else:
        warnings.append(
            "WARNING: D&A could not be normalised — using sector default. "
            "Analyst should input D&A estimate before forecasting."
        )
        da_pct_default = _da_sector_default(template)
        da_assumption = {
            "value":     da_pct_default,
            "method":    "template_default",
            "tag":       "default",
            "rationale": (
                f"D&A data unavailable. Using {template} template default "
                f"of {da_pct_default*100:.1f}% of revenue. "
                "Analyst override required."
            ),
        }

    # -------------------------------------------------------------------------
    # 5. Capex as % of revenue
    # -------------------------------------------------------------------------
    capex_pct = norm.get("capex_pct_revenue")
    if capex_pct is not None:
        capex_assumption = {
            "value":     round(capex_pct, 4),
            "method":    "sourced_from_normaliser",
            "tag":       "rule_based",
            "rationale": f"Capex held at {capex_pct*100:.1f}% of revenue (normalised median).",
        }
    else:
        warnings.append(
            "WARNING: Capex could not be normalised — using sector default. "
            "Analyst should input capex estimate before forecasting."
        )
        capex_pct_default = _capex_sector_default(template)
        capex_assumption = {
            "value":     capex_pct_default,
            "method":    "template_default",
            "tag":       "default",
            "rationale": (
                f"Capex data unavailable. Using {template} template default "
                f"of {capex_pct_default*100:.1f}% of revenue. "
                "Analyst override required."
            ),
        }

    # -------------------------------------------------------------------------
    # 6. NWC as % of revenue
    # -------------------------------------------------------------------------
    nwc_pct = norm.get("nwc_pct_revenue")
    if nwc_pct is not None:
        nwc_assumption = {
            "value":     round(nwc_pct, 4),
            "method":    "sourced_from_normaliser",
            "tag":       "rule_based",
            "rationale": f"NWC held at {nwc_pct*100:.1f}% of revenue (normalised median).",
        }
    else:
        nwc_assumption = {
            "value":     0.05,
            "method":    "template_default",
            "tag":       "default",
            "rationale": "NWC data unavailable. Defaulting to 5% of revenue. Analyst override required.",
        }
        warnings.append("WARNING: NWC defaulted to 5% of revenue — no working capital data available")

    # -------------------------------------------------------------------------
    # 7. WACC
    # -------------------------------------------------------------------------
    wacc, wacc_components, wacc_warnings = _build_wacc(
        stats, years_data, years, tax_rate, c_def, country
    )
    warnings.extend(wacc_warnings)

    wacc_assumption = {
        "value":     round(wacc, 4),
        "method":    "capm_blume_adjusted_beta_market_cap_weights",
        "tag":       "calibrated",
        "rationale": (
            f"WACC {wacc*100:.1f}% built from CAPM with Blume-adjusted beta. "
            f"Rf={wacc_components['risk_free_rate']*100:.1f}%, "
            f"ERP={wacc_components['equity_risk_premium']*100:.1f}%, "
            f"β(raw)={wacc_components['beta_raw']:.2f} → "
            f"β(adj)={wacc_components['beta_adjusted']:.2f} → "
            f"Ke={wacc_components['cost_of_equity']*100:.1f}%. "
            f"Kd(pre-tax)={wacc_components['cost_of_debt_pretax']*100:.1f}% "
            f"[{wacc_components['cost_of_debt_method']}], "
            f"Kd(post-tax)={wacc_components['cost_of_debt_aftertax']*100:.1f}%. "
            f"Weights: equity {wacc_components['weight_equity']*100:.1f}% "
            f"(market cap), debt {wacc_components['weight_debt']*100:.1f}%. "
            "Rf and ERP are country-default estimates — analyst must confirm."
        ),
    }

    # -------------------------------------------------------------------------
    # 8. Terminal growth rate
    # -------------------------------------------------------------------------
    terminal_assumption = {
        "value":     round(terminal_growth, 4),
        "method":    "country_gdp_default_with_template_adjustment",
        "tag":       "default",
        "rationale": (
            f"Terminal growth {terminal_growth*100:.1f}% based on long-run GDP estimate "
            f"for {country or 'global default'} ({c_def['terminal_gdp']*100:.1f}%) "
            + (f"with {template} template adjustment ({bounds['terminal_premium']*100:+.1f}%)."
               if bounds['terminal_premium'] != 0 else ".")
            + " Always confirm WACC > terminal growth: "
            + ("✓ satisfied." if wacc > terminal_growth else
               "⚠ NOT satisfied — WACC must exceed terminal growth.")
        ),
    }

    if wacc <= terminal_growth:
        blockers.append(
            f"BLOCKER: WACC ({wacc*100:.1f}%) ≤ terminal growth ({terminal_growth*100:.1f}%) — "
            "this makes the terminal value infinite. Reduce terminal growth or revise WACC."
        )

    # -------------------------------------------------------------------------
    # Assemble base-case assumptions pack
    # -------------------------------------------------------------------------
    base_assumptions = {
        "revenue_growth":    revenue_growth_assumption,
        "ebit_margin":       ebit_margin_assumption,
        "tax_rate":          tax_assumption,
        "da_pct_revenue":    da_assumption,
        "capex_pct_revenue": capex_assumption,
        "nwc_pct_revenue":   nwc_assumption,
        "wacc":              wacc_assumption,
        "terminal_growth":   terminal_assumption,
    }

    # -------------------------------------------------------------------------
    # 9. Scenarios (bull / bear)
    # -------------------------------------------------------------------------
    # Estimate TV concentration from WACC-growth spread to scale deltas.
    # We don't have the actual TV% yet (that comes from the valuation engine),
    # so we use a proxy: when the WACC-g spread is narrow (< 4%), the model
    # will almost always be TV-dominated and tight deltas are appropriate.
    wacc_g_spread = wacc - terminal_growth
    use_tight_deltas = wacc_g_spread < (1 - _TV_TIGHT_THRESHOLD) * wacc

    scenarios = _build_scenarios(
        base_assumptions, terminal_growth, bounds, wacc, use_tight_deltas
    )

    # -------------------------------------------------------------------------
    # Graduation check
    # -------------------------------------------------------------------------
    n_defaults = sum(
        1 for k, v in base_assumptions.items()
        if isinstance(v, dict) and v.get("tag") in ("default",)
    )
    if n_defaults >= 3:
        warnings.append(
            f"WARNING: {n_defaults} assumptions are using defaults with no historical data — "
            "confidence is low. Analyst review strongly recommended before modelling."
        )

    status = "fail" if blockers else ("pass_with_caution" if warnings else "pass")

    return {
        "status":          status,
        "assumptions":     base_assumptions,
        "scenarios":       scenarios,
        "forecast_years":  _FORECAST_YEARS,
        "wacc_components": wacc_components,
        "blockers":        blockers,
        "warnings":        warnings,
        "ticker":          ticker,
        "company_name":    company_name,
        "stats":           stats,
        "canonical_by_year": years_data,
        "normalised":      norm,
        "base_year":       base_year,
        "classification":  classification,
        "template":        template,
        "quality_score":   quality_score,
        "metadata": {
            "assumed_at":   datetime.utcnow().isoformat() + "Z",
            "template":     template,
            "country":      country,
            "forecast_years": _FORECAST_YEARS,
            "n_defaults":   n_defaults,
        },
    }


# ---------------------------------------------------------------------------
# Revenue growth helpers
# ---------------------------------------------------------------------------

def _calc_historical_cagr(years_data: dict, years: list) -> tuple[float | None, str]:
    """
    Compute a blended historical revenue CAGR.
    Weights: 1yr (40%), 3yr CAGR (35%), 5yr CAGR (25%) — using whatever is available.
    """
    rev = {}
    for y in years:
        v = years_data[y].get("revenue")
        if v and v > 0:
            rev[y] = v

    sorted_yrs = sorted(rev.keys(), reverse=True)
    if len(sorted_yrs) < 2:
        return None, "insufficient_data"

    cagrs  = {}
    labels = {}

    # 1yr
    if len(sorted_yrs) >= 2:
        r0, r1 = rev[sorted_yrs[0]], rev[sorted_yrs[1]]
        cagrs["1yr"] = (r0 - r1) / r1
        labels["1yr"] = f"1yr: {cagrs['1yr']*100:.1f}%"

    # 3yr CAGR
    if len(sorted_yrs) >= 4:
        r0, r3 = rev[sorted_yrs[0]], rev[sorted_yrs[3]]
        cagrs["3yr"] = (r0 / r3) ** (1/3) - 1
        labels["3yr"] = f"3yr CAGR: {cagrs['3yr']*100:.1f}%"

    # 5yr CAGR
    if len(sorted_yrs) >= 6:
        r0, r5 = rev[sorted_yrs[0]], rev[sorted_yrs[5]]
        cagrs["5yr"] = (r0 / r5) ** (1/5) - 1
        labels["5yr"] = f"5yr CAGR: {cagrs['5yr']*100:.1f}%"

    # Weighted blend
    weights = {"1yr": 0.40, "3yr": 0.35, "5yr": 0.25}
    total_w = sum(weights[k] for k in cagrs)
    blended = sum(v * weights[k] / total_w for k, v in cagrs.items())
    method  = "weighted_cagr (" + ", ".join(labels.values()) + ")"

    return blended, method


def _calc_peak_margin(years_data: dict, years: list) -> float | None:
    """Return the highest EBIT margin observed across all years."""
    margins = []
    for y in years:
        ebit = years_data[y].get("ebit")
        rev  = years_data[y].get("revenue")
        if ebit is not None and rev and rev > 0:
            margins.append(ebit / rev)
    return max(margins) if margins else None


def _linear_fade(start: float, end: float, n: int) -> list[float]:
    """Return n values fading linearly from start to end."""
    if n == 1:
        return [start]
    return [start + i * (end - start) / (n - 1) for i in range(n)]


# ---------------------------------------------------------------------------
# Sector defaults for missing D&A and Capex
# ---------------------------------------------------------------------------

def _da_sector_default(template: str) -> float:
    return {
        "asset_light": 0.04,
        "industrial":  0.05,
        "consumer":    0.03,
        "resources":   0.10,
        "utilities":   0.08,
        "hybrid":      0.05,
    }.get(template, 0.05)


def _capex_sector_default(template: str) -> float:
    return {
        "asset_light": 0.03,
        "industrial":  0.07,
        "consumer":    0.04,
        "resources":   0.18,
        "utilities":   0.15,
        "hybrid":      0.06,
    }.get(template, 0.06)


# ---------------------------------------------------------------------------
# WACC builder
# ---------------------------------------------------------------------------

def _build_wacc(stats: dict, years_data: dict, years: list,
                tax_rate: float, c_def: dict, country: str
                ) -> tuple[float, dict, list[str]]:
    """
    Build WACC using:
    - Blume-adjusted beta: (2/3 × raw) + (1/3) — pulls extremes toward market mean
    - Cost of debt from actuals: interest_expense / average_debt (last 2 years)
    - Equity weight from market cap (not book equity)
    """
    warnings = []

    rf  = c_def["rf"]
    erp = c_def["erp"]

    # --- Beta ---
    beta_raw = stats.get("beta")
    if beta_raw is None or beta_raw <= 0:
        beta_raw = _DEFAULT_BETA
        warnings.append(
            f"WARNING: Beta not available — using default β={_DEFAULT_BETA:.1f}. "
            "Analyst should confirm with market beta."
        )
    # Blume adjustment: regresses raw beta one third of the way toward 1.0
    beta_adjusted = _BLUME_WEIGHT * beta_raw + (1 - _BLUME_WEIGHT) * 1.0

    cost_of_equity = rf + beta_adjusted * erp

    # --- Cost of debt from actuals ---
    cod_actual, cod_detail = _calc_cost_of_debt_actual(years_data, years)
    if cod_actual is None:
        cost_of_debt_pretax = _DEFAULT_COD
        cod_method = "sector_default"
        warnings.append(
            f"WARNING: Cost of debt not computable (interest expense or debt missing) — "
            f"using default {_DEFAULT_COD*100:.1f}%. Analyst should confirm."
        )
    else:
        cost_of_debt_pretax = cod_actual
        cod_method = cod_detail   # e.g. "interest_expense/avg_debt_2yr"

    cost_of_debt_pretax   = max(_MIN_COD, min(_MAX_COD, cost_of_debt_pretax))
    cost_of_debt_aftertax = cost_of_debt_pretax * (1 - tax_rate)

    # --- Capital structure weights from market cap ---
    w_debt, w_equity, weight_method, market_cap_used, debt_used = _calc_weights_market(
        stats, years_data, years
    )

    wacc = cost_of_equity * w_equity + cost_of_debt_aftertax * w_debt

    components = {
        "risk_free_rate":         round(rf, 4),
        "equity_risk_premium":    round(erp, 4),
        "beta_raw":               round(beta_raw, 4),
        "beta_adjusted":          round(beta_adjusted, 4),
        # Keep 'beta' for any downstream code that reads the old key
        "beta":                   round(beta_adjusted, 4),
        "cost_of_equity":         round(cost_of_equity, 4),
        "cost_of_debt_pretax":    round(cost_of_debt_pretax, 4),
        "cost_of_debt_aftertax":  round(cost_of_debt_aftertax, 4),
        "cost_of_debt_method":    cod_method,
        "weight_equity":          round(w_equity, 4),
        "weight_debt":            round(w_debt, 4),
        "weight_method":          weight_method,
        "market_cap_used":        market_cap_used,
        "debt_used":              debt_used,
        "tax_rate":               round(tax_rate, 4),
        "country":                country,
        "rf_source":              "country_lookup_table_early_2026",
        "erp_source":             "damodaran_estimate_country_lookup",
    }

    return wacc, components, warnings


def _calc_cost_of_debt_actual(years_data: dict, years: list
                               ) -> tuple[float | None, str]:
    """
    Derive pre-tax cost of debt as: interest_expense / average_debt.
    Uses up to 2 most recent years. Returns (rate, method_description).
    Interest expense is stored as a negative number in financial statements.
    """
    pairs = []
    sorted_years = years[:4]  # look at up to 4 years to find 2 valid pairs
    for i in range(len(sorted_years) - 1):
        yr_now  = sorted_years[i]
        yr_prev = sorted_years[i + 1]
        debt_now  = years_data[yr_now].get("debt")
        debt_prev = years_data[yr_prev].get("debt")
        ie        = years_data[yr_now].get("interest_expense")
        if ie is not None and debt_now and debt_now > 0 and debt_prev and debt_prev > 0:
            avg_debt = (debt_now + debt_prev) / 2
            pairs.append(abs(ie) / avg_debt)
        if len(pairs) >= 2:
            break

    if not pairs:
        # Fallback: single-year simple ratio
        for y in sorted_years:
            ie   = years_data[y].get("interest_expense")
            debt = years_data[y].get("debt")
            if ie is not None and debt and debt > 0:
                return abs(ie) / debt, "interest_expense/debt_single_yr"
        return None, "unavailable"

    rate = sum(pairs) / len(pairs)
    return rate, f"interest_expense/avg_debt_{len(pairs)}yr"


def _calc_weights_market(stats: dict, years_data: dict, years: list
                          ) -> tuple[float, float, str, float | None, float | None]:
    """
    Return (w_debt, w_equity, method, market_cap_used, debt_used).

    Equity weight = market cap (price × shares).
    Debt = most recent year total_debt from balance sheet.
    Falls back to book equity if market cap unavailable.
    """
    market_cap = stats.get("market_cap")
    price      = stats.get("current_price")
    shares     = stats.get("shares_outstanding")

    # Prefer scraped market cap; compute from price×shares as fallback
    if not market_cap and price and shares:
        market_cap = price * shares

    # Get most recent debt
    debt_used = None
    for y in years:
        d = years_data[y].get("debt")
        if d is not None and d > 0:
            debt_used = d
            break

    if market_cap and market_cap > 0 and debt_used:
        total  = market_cap + debt_used
        w_debt = round(debt_used / total, 4)
        return w_debt, round(1.0 - w_debt, 4), "market_cap_plus_debt", market_cap, debt_used

    if market_cap and market_cap > 0:
        # No debt found — treat as all-equity
        return 0.0, 1.0, "market_cap_no_debt", market_cap, 0.0

    # Final fallback: book equity weights
    for y in years:
        debt   = years_data[y].get("debt")
        equity = years_data[y].get("total_equity")
        if debt is not None and equity is not None and (debt + abs(equity)) > 0:
            total  = debt + abs(equity)
            w_debt = round(debt / total, 4)
            return w_debt, round(1.0 - w_debt, 4), "book_equity_fallback", None, debt
    return 0.30, 0.70, "default_30_70", None, None


# ---------------------------------------------------------------------------
# Scenario builder
# ---------------------------------------------------------------------------

def _build_scenarios(base: dict, terminal_growth: float,
                     bounds: dict, base_wacc: float,
                     tight: bool = False) -> dict:
    """
    Build bull and bear variants from the base case.

    Standard deltas (low TV concentration):
        Bull: +1.5% revenue growth, +100bps EBIT margin by Yr5, WACC unchanged
        Bear: -1.5% revenue growth, -100bps EBIT margin by Yr5, WACC +100bps

    Tight deltas (high TV concentration, tight WACC-g spread):
        Bull: +0.8% revenue growth, +50bps EBIT margin by Yr5, WACC unchanged
        Bear: -0.8% revenue growth, -50bps EBIT margin by Yr5, WACC +50bps

    Tight mode prevents artificially wide scenario ranges when the model is
    heavily terminal-value-dominated. The sensitivity table already shows the
    WACC × growth grid — scenarios should represent plausible operating outcomes.
    """
    d_growth    = _SCENARIO_GROWTH_TIGHT   if tight else _SCENARIO_GROWTH_STD
    d_margin    = _SCENARIO_MARGIN_TIGHT   if tight else _SCENARIO_MARGIN_STD
    d_wacc_bear = _SCENARIO_WACC_BEAR_TIGHT if tight else _SCENARIO_WACC_BEAR_STD
    mode_label  = "tight" if tight else "standard"

    def _shift_path(base_entry: dict, delta_yr1: float, delta_yr5: float) -> dict:
        result = dict(base_entry)
        n = sum(1 for k in base_entry if k.startswith("year_"))
        for i in range(n):
            key = f"year_{i+1}"
            if key in result:
                delta = delta_yr1 + i * (delta_yr5 - delta_yr1) / max(n - 1, 1)
                result[key] = round(result[key] + delta, 4)
        result["tag"]    = "scenario"
        result["method"] = result.get("method", "") + f" [scenario_{mode_label}]"
        return result

    bull_growth   = _shift_path(base["revenue_growth"], +d_growth, +d_growth)
    bull_margin   = _shift_path(base["ebit_margin"],    0.0,       +d_margin)
    bull_wacc     = {**base["wacc"],
                     "value":     round(base_wacc, 4),
                     "tag":       "scenario",
                     "rationale": f"Bull: WACC unchanged ({mode_label} scenario mode)."}
    bull_terminal = {**base["terminal_growth"],
                     "value":     round(min(terminal_growth + 0.005,
                                            base_wacc - 0.01), 4),
                     "tag":       "scenario",
                     "rationale": "Bull: terminal growth +0.5%, capped below WACC."}

    bear_growth   = _shift_path(base["revenue_growth"], -d_growth, -d_growth)
    bear_margin   = _shift_path(base["ebit_margin"],    0.0,       -d_margin)
    bear_wacc     = {**base["wacc"],
                     "value":     round(base_wacc + d_wacc_bear, 4),
                     "tag":       "scenario",
                     "rationale": (
                         f"Bear: WACC +{d_wacc_bear*100:.0f}bps "
                         f"({mode_label} scenario mode)."
                     )}
    bear_terminal = {**base["terminal_growth"],
                     "value":     round(max(terminal_growth - 0.005, 0.005), 4),
                     "tag":       "scenario",
                     "rationale": "Bear: terminal growth -0.5%."}

    def _scenario(growth, margin, wacc_entry, terminal_entry) -> dict:
        return {
            "revenue_growth":    growth,
            "ebit_margin":       margin,
            "tax_rate":          base["tax_rate"],
            "da_pct_revenue":    base["da_pct_revenue"],
            "capex_pct_revenue": base["capex_pct_revenue"],
            "nwc_pct_revenue":   base["nwc_pct_revenue"],
            "wacc":              wacc_entry,
            "terminal_growth":   terminal_entry,
        }

    return {
        "base": base,
        "bull": _scenario(bull_growth, bull_margin, bull_wacc, bull_terminal),
        "bear": _scenario(bear_growth, bear_margin, bear_wacc, bear_terminal),
    }


# ---------------------------------------------------------------------------
# Fail result
# ---------------------------------------------------------------------------

def _fail(ticker, company_name, stats, years_data, norm,
          base_year, classification, template, quality_score, blockers) -> dict:
    return {
        "status":          "fail",
        "assumptions":     {},
        "scenarios":       {},
        "forecast_years":  _FORECAST_YEARS,
        "wacc_components": {},
        "blockers":        blockers,
        "warnings":        [],
        "ticker":          ticker,
        "company_name":    company_name,
        "stats":           stats,
        "canonical_by_year": years_data,
        "normalised":      norm,
        "base_year":       base_year,
        "classification":  classification,
        "template":        template,
        "quality_score":   quality_score,
        "metadata": {"assumed_at": datetime.utcnow().isoformat() + "Z"},
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_assumption_report(result: dict) -> None:
    status  = result["status"]
    company = result["company_name"]
    ticker  = result["ticker"]
    n_years = result["forecast_years"]
    a       = result.get("assumptions", {})
    wc      = result.get("wacc_components", {})

    status_label = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}
    print(f"\n{'='*60}")
    print(f"  Assumption Engine — {company} ({ticker})")
    print(f"  Status       : [{status_label.get(status, status)}]")
    print(f"  Template     : {result.get('template', '—')}")
    print(f"  Forecast     : {n_years} years + terminal")
    print(f"{'='*60}")

    if result["blockers"]:
        print("\n  Blockers:")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["warnings"]:
        print("\n  Warnings:")
        for w in result["warnings"]:
            print(f"    {w}")

    if not a:
        print(f"{'='*60}\n")
        return

    # Revenue growth path
    rg = a.get("revenue_growth", {})
    path_vals = [rg.get(f"year_{i+1}") for i in range(n_years)]
    path_str  = "  ".join(f"Y{i+1}:{v*100:.1f}%" for i, v in enumerate(path_vals) if v is not None)
    print(f"\n  Revenue growth path:  {path_str}")
    print(f"    [{rg.get('tag')}] {rg.get('method')}")

    # EBIT margin path
    em = a.get("ebit_margin", {})
    mpath = [em.get(f"year_{i+1}") for i in range(n_years)]
    mstr  = "  ".join(f"Y{i+1}:{v*100:.1f}%" for i, v in enumerate(mpath) if v is not None)
    print(f"\n  EBIT margin path:     {mstr}")
    print(f"    [{em.get('tag')}] {em.get('method')}")

    # Flat assumptions
    for key, label in [
        ("tax_rate",          "Tax rate"),
        ("da_pct_revenue",    "D&A % revenue"),
        ("capex_pct_revenue", "Capex % revenue"),
        ("nwc_pct_revenue",   "NWC % revenue"),
    ]:
        entry = a.get(key, {})
        val   = entry.get("value")
        tag   = entry.get("tag", "—")
        print(f"  {label:<22} {_pct(val):<10} [{tag}]")

    # WACC
    wacc_val = a.get("wacc", {}).get("value")
    term_val = a.get("terminal_growth", {}).get("value")
    print(f"\n  WACC:                 {_pct(wacc_val)}")
    print(f"  Terminal growth:      {_pct(term_val)}")

    if wc:
        print(f"\n  WACC components:")
        print(f"    Risk-free rate    : {_pct(wc.get('risk_free_rate'))}  [{wc.get('rf_source','')}]")
        print(f"    Equity risk prem  : {_pct(wc.get('equity_risk_premium'))}")
        print(f"    Beta              : {wc.get('beta', '—')}")
        print(f"    Cost of equity    : {_pct(wc.get('cost_of_equity'))}")
        print(f"    Cost of debt (AT) : {_pct(wc.get('cost_of_debt_aftertax'))}")
        print(f"    Weight equity     : {_pct(wc.get('weight_equity'))}")
        print(f"    Weight debt       : {_pct(wc.get('weight_debt'))}")

    # Scenarios summary
    scenarios = result.get("scenarios", {})
    if "bull" in scenarios and "bear" in scenarios:
        bull_wacc = scenarios["bull"].get("wacc", {}).get("value")
        bear_wacc = scenarios["bear"].get("wacc", {}).get("value")
        bull_g1   = scenarios["bull"].get("revenue_growth", {}).get("year_1")
        bear_g1   = scenarios["bear"].get("revenue_growth", {}).get("year_1")
        print(f"\n  Scenarios:")
        print(f"    Bull — Yr1 growth: {_pct(bull_g1)}, WACC: {_pct(bull_wacc)}")
        print(f"    Base — Yr1 growth: {_pct(rg.get('year_1'))}, WACC: {_pct(wacc_val)}")
        print(f"    Bear — Yr1 growth: {_pct(bear_g1)}, WACC: {_pct(bear_wacc)}")

    print(f"\n  Assumed at : {result['metadata'].get('assumed_at', '—')}")
    print(f"{'='*60}\n")


def _pct(val) -> str:
    if val is None:
        return "—"
    return f"{float(val)*100:.1f}%"
