"""
Sprint 2A — Normaliser

Purpose:
    Convert reported historical financials into a sustainable economic base
    for forecasting. The normaliser identifies the base year, detects anomalous
    one-off distortions, and produces a set of smoothed normalised metrics.

    It does NOT invent numbers. Every adjustment is logged, tagged, and
    reversible. Unresolved anomalies are flagged for analyst review rather
    than silently fixed.

Inputs:
    standardised  — output from standardiser.run_standardiser()
    validated     — output from validator.run_validator()

Outputs:
    {
        "status":         "pass" | "pass_with_caution" | "fail",
        "base_year":      str,          # year used as the starting point
        "normalised":     {             # the normalised base metrics
            "revenue":            float,
            "ebit_margin":        float,   # as a decimal e.g. 0.12 = 12%
            "ebit":               float,
            "tax_rate":           float,   # as a decimal e.g. 0.21 = 21%
            "da_pct_revenue":     float,
            "da":                 float,
            "capex_pct_revenue":  float,   # positive decimal (absolute value)
            "capex":              float,   # negative (cash outflow convention)
            "nwc_pct_revenue":    float,   # net working capital as % of revenue
            "nwc":                float,
        },
        "adjustment_log": [...],        # every normalisation decision
        "anomalies":      [...],        # years flagged as statistically unusual
        "confidence":     str,          # "high" | "medium" | "low"
        "blockers":       [...],
        "warnings":       [...],
        "ticker":         str,
        "company_name":   str,
        "stats":          {...},        # pass-through
        "canonical_by_year": {...},     # pass-through for downstream stages
        "quality_score":  float,        # from validator, passed through
        "metadata":       {...},
    }

Normalisation methods:
    revenue          — base year reported value; flagged if >40% above 3yr trend
    ebit_margin      — median of historical EBIT/revenue ratios
    tax_rate         — median of (tax_provision/pre_tax_income); capped 0–40%;
                       defaults to 25% if no data
    da_pct_revenue   — median of historical D&A/revenue ratios
    capex_pct_revenue— median of historical |capex|/revenue ratios
    nwc_pct_revenue  — median of (AR + inventory - AP) / revenue;
                       falls back to change_in_working_cap/revenue

Adjustment tags:
    rule_based       — applied automatically by statistical rule
    rule_based_default — applied because no data was available (e.g. tax default)
    source_based     — triggered by observed data pattern (e.g. spike in one year)
    analyst_override — reserved for future manual input

Anomaly detection:
    A year is flagged if its value deviates more than _ANOMALY_THRESHOLD
    from the historical median (relative basis). For small datasets (<4 years)
    a looser threshold is applied automatically.
"""

from datetime import datetime
import statistics


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

_ANOMALY_THRESHOLD       = 0.40   # >40% deviation from median → anomaly flag
_ANOMALY_THRESHOLD_SMALL = 0.60  # loosened for <4 years of data
_DEFAULT_TAX_RATE        = 0.25  # fallback if no tax data available
_MAX_TAX_RATE            = 0.40  # cap on effective tax rate (outlier protection)
_MIN_TAX_RATE            = 0.00
_MIN_PRACTICAL_TAX_RATE  = 0.15  # below this, warn — likely deferred-tax distortion
_REVENUE_TREND_FLAG      = 0.40  # base year >40% above 3yr avg → flag
_NWC_HIGH_THRESHOLD      = 0.25  # NWC/revenue above this, warn — possible data issue


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_normaliser(standardised: dict, validated: dict) -> dict:
    """
    Normalise the validated canonical dataset into a base year + smoothed metrics.
    """
    ticker        = standardised.get("ticker", "")
    company_name  = standardised.get("company_name", "")
    stats         = standardised.get("stats", {})
    years_data    = standardised.get("canonical_by_year", {})
    quality_score = validated.get("quality_score", 0)

    years = sorted(years_data.keys(), reverse=True)   # most recent first

    adjustment_log: list[dict] = []
    anomalies:      list[dict] = []
    blockers:       list[str]  = []
    warnings:       list[str]  = []

    # -------------------------------------------------------------------------
    # 1. Select base year
    # -------------------------------------------------------------------------
    base_year = _select_base_year(years, years_data)
    if base_year is None:
        blockers.append(
            "BLOCKER: No year found with revenue, EBIT, and capex all present — "
            "cannot define a base year for normalisation"
        )
        return _fail_result(ticker, company_name, stats, years_data,
                            quality_score, blockers)

    base = years_data[base_year]

    # -------------------------------------------------------------------------
    # 2. Normalise revenue
    # -------------------------------------------------------------------------
    revenue = base.get("revenue")
    if revenue is None:
        blockers.append("BLOCKER: Revenue missing in base year — cannot normalise")
        return _fail_result(ticker, company_name, stats, years_data,
                            quality_score, blockers)

    # Flag if base year revenue is much higher than 3yr average (acquisition noise)
    rev_series = _series(years_data, years, "revenue")
    if len(rev_series) >= 3:
        trailing_avg = statistics.mean([v for _, v in rev_series[1:4]])
        if trailing_avg > 0 and (revenue - trailing_avg) / trailing_avg > _REVENUE_TREND_FLAG:
            warnings.append(
                f"WARNING: Base year revenue ({_fmt(revenue)}) is >"
                f"{int(_REVENUE_TREND_FLAG*100)}% above 3yr trailing average "
                f"({_fmt(trailing_avg)}) — possible acquisition; "
                "treat revenue base with caution"
            )
            adjustment_log.append(_log(
                metric="revenue", raw=revenue, normalised=revenue,
                method="base_year_reported",
                tag="source_based",
                rationale="Revenue used as reported. Spike vs 3yr average flagged — "
                          "analyst should confirm whether acquisition inflates base.",
            ))
        else:
            adjustment_log.append(_log(
                metric="revenue", raw=revenue, normalised=revenue,
                method="base_year_reported",
                tag="rule_based",
                rationale="Revenue taken directly from base year — within normal trend range.",
            ))
    else:
        adjustment_log.append(_log(
            metric="revenue", raw=revenue, normalised=revenue,
            method="base_year_reported",
            tag="rule_based",
            rationale="Revenue taken from base year (insufficient history for trend check).",
        ))

    # -------------------------------------------------------------------------
    # 3. Normalise EBIT margin
    # -------------------------------------------------------------------------
    margin_series = _ratio_series(years_data, years, "ebit", "revenue")
    ebit_margin, margin_method, margin_anomalies = _normalise_ratio(
        margin_series, base_year, metric="ebit_margin"
    )
    anomalies.extend(margin_anomalies)

    if ebit_margin is None:
        blockers.append("BLOCKER: Cannot compute EBIT margin — EBIT or revenue missing")
        return _fail_result(ticker, company_name, stats, years_data,
                            quality_score, blockers)

    raw_margin = base.get("ebit", 0) / revenue if revenue else None
    adjustment_log.append(_log(
        metric="ebit_margin",
        raw=raw_margin,
        normalised=ebit_margin,
        method=margin_method,
        tag="rule_based",
        rationale=f"EBIT margin smoothed using {margin_method} across "
                  f"{len(margin_series)} year(s). "
                  + ("Anomalous years excluded from median." if margin_anomalies else ""),
    ))

    ebit = revenue * ebit_margin

    # -------------------------------------------------------------------------
    # 4. Normalise effective tax rate
    # -------------------------------------------------------------------------
    tax_series = _effective_tax_series(years_data, years)
    tax_rate, tax_method = _normalise_tax(tax_series)

    raw_tax = tax_series[0][1] if tax_series else None
    adjustment_log.append(_log(
        metric="tax_rate",
        raw=raw_tax,
        normalised=tax_rate,
        method=tax_method,
        tag="rule_based" if tax_series else "rule_based_default",
        rationale=(
            f"Effective tax rate smoothed using {tax_method} across "
            f"{len(tax_series)} year(s). Capped between "
            f"{int(_MIN_TAX_RATE*100)}%–{int(_MAX_TAX_RATE*100)}%."
        ) if tax_series else (
            f"No tax data available. Defaulting to {int(_DEFAULT_TAX_RATE*100)}% — "
            "analyst should override with statutory or peer rate."
        ),
    ))

    if not tax_series:
        warnings.append(
            f"WARNING: No effective tax rate could be computed — "
            f"defaulting to {int(_DEFAULT_TAX_RATE*100)}%. Override recommended."
        )
    elif tax_rate < _MIN_PRACTICAL_TAX_RATE:
        warnings.append(
            f"WARNING: Normalised effective tax rate ({tax_rate:.1%}) is below the practical "
            f"floor ({_MIN_PRACTICAL_TAX_RATE:.0%}). Likely distorted by deferred tax credits "
            "or loss-year anomalies. Consider overriding with the statutory rate (~25% for UK/US). "
            "This inflates NOPAT and all downstream FCF figures."
        )

    # -------------------------------------------------------------------------
    # 5. Normalise D&A (as % of revenue)
    # -------------------------------------------------------------------------
    da_series = _ratio_series(years_data, years, "da", "revenue", absolute=True)
    da_pct, da_method, da_anomalies = _normalise_ratio(
        da_series, base_year, metric="da_pct_revenue"
    )
    anomalies.extend(da_anomalies)

    if da_pct is None:
        da_pct = None
        da = None
        warnings.append(
            "WARNING: D&A data missing — da_pct_revenue cannot be normalised. "
            "EBITDA and FCFF calculations will be impaired downstream."
        )
        adjustment_log.append(_log(
            metric="da_pct_revenue", raw=None, normalised=None,
            method="unavailable", tag="rule_based",
            rationale="D&A not present in any year — marked as estimate needed.",
        ))
    else:
        da = revenue * da_pct
        raw_da_pct = (abs(base.get("da", 0) or 0) / revenue) if revenue else None
        adjustment_log.append(_log(
            metric="da_pct_revenue", raw=raw_da_pct, normalised=da_pct,
            method=da_method, tag="rule_based",
            rationale=f"D&A as % of revenue smoothed using {da_method}.",
        ))

    # -------------------------------------------------------------------------
    # 6. Normalise capex (as % of revenue, absolute value)
    # -------------------------------------------------------------------------
    capex_series = _ratio_series(years_data, years, "capex", "revenue", absolute=True)
    capex_pct, capex_method, capex_anomalies = _normalise_ratio(
        capex_series, base_year, metric="capex_pct_revenue"
    )
    anomalies.extend(capex_anomalies)

    if capex_pct is None:
        capex = None
        warnings.append(
            "WARNING: Capex data missing — capex_pct_revenue cannot be normalised. "
            "Free cash flow calculation will be blocked downstream."
        )
        adjustment_log.append(_log(
            metric="capex_pct_revenue", raw=None, normalised=None,
            method="unavailable", tag="rule_based",
            rationale="Capex not present in any year — marked as estimate needed.",
        ))
    else:
        capex = -(revenue * capex_pct)   # restore negative sign (cash outflow)
        raw_cpx_pct = (abs(base.get("capex", 0) or 0) / revenue) if revenue else None
        adjustment_log.append(_log(
            metric="capex_pct_revenue", raw=raw_cpx_pct, normalised=capex_pct,
            method=capex_method, tag="rule_based",
            rationale=f"Capex intensity smoothed using {capex_method}. "
                      + ("Investment spike year(s) detected and excluded from median."
                         if capex_anomalies else ""),
        ))

    # -------------------------------------------------------------------------
    # 7. Normalise net working capital (as % of revenue)
    # -------------------------------------------------------------------------
    nwc_series = _nwc_series(years_data, years)
    nwc_pct, nwc_method, nwc_anomalies = _normalise_ratio(
        nwc_series, base_year, metric="nwc_pct_revenue"
    )
    anomalies.extend(nwc_anomalies)

    if nwc_pct is None:
        nwc = None
        warnings.append(
            "WARNING: Working capital components (AR, AP) missing — "
            "NWC cannot be normalised. Change-in-working-capital estimation "
            "will rely on analyst input downstream."
        )
        adjustment_log.append(_log(
            metric="nwc_pct_revenue", raw=None, normalised=None,
            method="unavailable", tag="rule_based",
            rationale="Insufficient working capital data — marked as estimate needed.",
        ))
    else:
        nwc = revenue * nwc_pct
        adjustment_log.append(_log(
            metric="nwc_pct_revenue", raw=None, normalised=nwc_pct,
            method=nwc_method, tag="rule_based",
            rationale=f"Net working capital as % of revenue smoothed using {nwc_method}.",
        ))
        if abs(nwc_pct) > _NWC_HIGH_THRESHOLD:
            warnings.append(
                f"WARNING: NWC/Revenue ratio is {nwc_pct:.1%} — unusually high. "
                "This may reflect large contract receivables (common in long-cycle industrials) "
                "or a data quality issue with AR/AP scraping. Verify components against "
                "reported balance sheet before accepting this assumption."
            )

    # -------------------------------------------------------------------------
    # 8. Confidence rating
    # -------------------------------------------------------------------------
    n_missing = sum(1 for v in [ebit_margin, tax_rate, da_pct, capex_pct, nwc_pct]
                    if v is None)
    n_anomalies = len(anomalies)

    if n_missing == 0 and n_anomalies == 0 and quality_score >= 75:
        confidence = "high"
    elif n_missing <= 1 and quality_score >= 50:
        confidence = "medium"
    else:
        confidence = "low"

    if n_anomalies > 0:
        warnings.append(
            f"WARNING: {n_anomalies} anomalous year(s) detected across metrics — "
            "these were excluded from medians. See anomaly log for detail."
        )

    # -------------------------------------------------------------------------
    # 9. Graduation check
    # -------------------------------------------------------------------------
    if not blockers and capex_pct is None:
        warnings.append(
            "WARNING: Capex could not be normalised — "
            "FCF calculation downstream will need an analyst estimate"
        )

    status = _graduation_status(blockers, warnings, confidence)

    return {
        "status":       status,
        "base_year":    base_year,
        "normalised": {
            "revenue":           revenue,
            "ebit_margin":       ebit_margin,
            "ebit":              ebit,
            "tax_rate":          tax_rate,
            "da_pct_revenue":    da_pct,
            "da":                da,
            "capex_pct_revenue": capex_pct,
            "capex":             capex,
            "nwc_pct_revenue":   nwc_pct,
            "nwc":               nwc,
        },
        "adjustment_log":    adjustment_log,
        "anomalies":         anomalies,
        "confidence":        confidence,
        "blockers":          blockers,
        "warnings":          warnings,
        "ticker":            ticker,
        "company_name":      company_name,
        "stats":             stats,
        "canonical_by_year": years_data,    # pass-through for classifier + beyond
        "quality_score":     quality_score,
        "metadata": {
            "normalised_at": datetime.utcnow().isoformat() + "Z",
            "base_year":     base_year,
            "years_used":    years,
            "n_years":       len(years),
            "n_anomalies":   n_anomalies,
            "confidence":    confidence,
        },
    }


# ---------------------------------------------------------------------------
# Helpers — data extraction
# ---------------------------------------------------------------------------

def _select_base_year(years: list, years_data: dict) -> str | None:
    """Return the most recent year that has revenue, ebit, and capex."""
    for y in years:
        d = years_data[y]
        if d.get("revenue") and d.get("ebit") is not None and d.get("capex") is not None:
            return y
    # Relax: accept any year with just revenue
    for y in years:
        if years_data[y].get("revenue"):
            return y
    return None


def _series(years_data: dict, years: list, field: str) -> list[tuple[str, float]]:
    """Return [(year, value)] for a field across all years, most recent first."""
    return [
        (y, years_data[y][field])
        for y in years
        if field in years_data[y] and years_data[y][field] is not None
    ]


def _ratio_series(years_data: dict, years: list,
                  numerator: str, denominator: str,
                  absolute: bool = False) -> list[tuple[str, float]]:
    """Return [(year, ratio)] for numerator/denominator across years."""
    result = []
    for y in years:
        num = years_data[y].get(numerator)
        den = years_data[y].get(denominator)
        if num is not None and den and abs(den) > 0:
            ratio = (abs(num) if absolute else num) / abs(den)
            result.append((y, ratio))
    return result


def _effective_tax_series(years_data: dict, years: list) -> list[tuple[str, float]]:
    """Return [(year, effective_tax_rate)] — clamped to [0, 40%]."""
    result = []
    for y in years:
        tax = years_data[y].get("tax_provision")
        pti = years_data[y].get("pre_tax_income")
        if tax is not None and pti and abs(pti) > 0 and pti > 0:
            rate = tax / pti
            if _MIN_TAX_RATE <= rate <= _MAX_TAX_RATE:
                result.append((y, rate))
    return result


def _nwc_series(years_data: dict, years: list) -> list[tuple[str, float]]:
    """
    Return [(year, nwc_pct_revenue)].
    NWC = AR + inventory - AP, divided by revenue.
    Falls back to change_in_working_cap/revenue if components are missing.
    """
    result = []
    for y in years:
        d   = years_data[y]
        rev = d.get("revenue")
        if not rev or rev == 0:
            continue
        ar  = d.get("accounts_receivable")
        ap  = d.get("accounts_payable")
        inv = d.get("inventory") or 0

        if ar is not None and ap is not None:
            nwc = (ar + inv - ap) / rev
            result.append((y, nwc))
        elif d.get("change_in_working_cap") is not None:
            # Rough fallback: cumulative change isn't the same as NWC level,
            # but gives a directional signal
            nwc = d["change_in_working_cap"] / rev
            result.append((y, nwc))
    return result


# ---------------------------------------------------------------------------
# Helpers — normalisation
# ---------------------------------------------------------------------------

def _normalise_ratio(series: list[tuple[str, float]],
                     base_year: str,
                     metric: str) -> tuple[float | None, str, list[dict]]:
    """
    Compute a normalised ratio using the median of available years.
    Detects and flags anomalous years (large deviations from median).
    Returns (normalised_value, method_description, anomaly_list).
    """
    if not series:
        return None, "unavailable", []

    values = [v for _, v in series]
    med    = statistics.median(values)

    # Anomaly threshold — looser for small samples
    threshold = _ANOMALY_THRESHOLD_SMALL if len(values) < 4 else _ANOMALY_THRESHOLD

    anomaly_records = []
    clean_values    = []

    for y, v in series:
        if med != 0 and abs(v - med) / abs(med) > threshold:
            anomaly_records.append({
                "year":          y,
                "metric":        metric,
                "value":         round(v, 6),
                "median":        round(med, 6),
                "deviation_pct": round(abs(v - med) / abs(med) * 100, 1),
                "flag":          "excluded_from_median",
            })
        else:
            clean_values.append(v)

    # Recompute median without anomalous years
    if clean_values:
        final_median = statistics.median(clean_values)
        n_used = len(clean_values)
    else:
        final_median = med   # all years were anomalous — fall back to raw median
        n_used = len(values)

    method = f"median_{n_used}yr"
    return final_median, method, anomaly_records


def _normalise_tax(series: list[tuple[str, float]]) -> tuple[float, str]:
    """Return (normalised_tax_rate, method)."""
    if not series:
        return _DEFAULT_TAX_RATE, "rule_based_default_25pct"
    values = [v for _, v in series]
    rate   = statistics.median(values)
    rate   = max(_MIN_TAX_RATE, min(_MAX_TAX_RATE, rate))
    return rate, f"median_{len(values)}yr"


def _graduation_status(blockers: list, warnings: list, confidence: str) -> str:
    if blockers:
        return "fail"
    if confidence == "low" or warnings:
        return "pass_with_caution"
    return "pass"


def _fail_result(ticker, company_name, stats, years_data, quality_score, blockers):
    return {
        "status":            "fail",
        "base_year":         None,
        "normalised":        {},
        "adjustment_log":    [],
        "anomalies":         [],
        "confidence":        "low",
        "blockers":          blockers,
        "warnings":          [],
        "ticker":            ticker,
        "company_name":      company_name,
        "stats":             stats,
        "canonical_by_year": years_data,
        "quality_score":     quality_score,
        "metadata": {
            "normalised_at": datetime.utcnow().isoformat() + "Z",
            "base_year":     None,
        },
    }


# ---------------------------------------------------------------------------
# Adjustment log helper
# ---------------------------------------------------------------------------

def _log(metric, raw, normalised, method, tag, rationale) -> dict:
    return {
        "metric":     metric,
        "raw":        round(raw, 6) if isinstance(raw, float) else raw,
        "normalised": round(normalised, 6) if isinstance(normalised, float) else normalised,
        "method":     method,
        "tag":        tag,
        "rationale":  rationale,
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_normaliser_report(result: dict) -> None:
    status     = result["status"]
    base_year  = result.get("base_year", "—")
    confidence = result.get("confidence", "—")
    norm       = result.get("normalised", {})
    revenue    = norm.get("revenue")

    status_label = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}
    print(f"\n{'='*60}")
    print(f"  Normaliser — {result['company_name']} ({result['ticker']})")
    print(f"  Status     : [{status_label.get(status, status)}]")
    print(f"  Base year  : {base_year}")
    print(f"  Confidence : {confidence.upper()}")
    print(f"{'='*60}")

    if result["blockers"]:
        print("\n  Blockers:")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["warnings"]:
        print("\n  Warnings:")
        for w in result["warnings"]:
            print(f"    {w}")

    if norm and revenue:
        print(f"\n  Normalised base metrics ({base_year}):")
        print(f"    {'Revenue':<28} {_fmt(revenue)}")

        em = norm.get("ebit_margin")
        print(f"    {'EBIT margin':<28} {_pct(em):<14}  → EBIT {_fmt(norm.get('ebit'))}")

        tr = norm.get("tax_rate")
        print(f"    {'Tax rate':<28} {_pct(tr)}")

        da = norm.get("da_pct_revenue")
        print(f"    {'D&A (% revenue)':<28} {_pct(da):<14}  → D&A  {_fmt(norm.get('da'))}")

        cx = norm.get("capex_pct_revenue")
        print(f"    {'Capex (% revenue)':<28} {_pct(cx):<14}  → Capex {_fmt(norm.get('capex'))}")

        nw = norm.get("nwc_pct_revenue")
        print(f"    {'NWC (% revenue)':<28} {_pct(nw):<14}  → NWC  {_fmt(norm.get('nwc'))}")

    if result["anomalies"]:
        print(f"\n  Anomalies detected ({len(result['anomalies'])}):")
        for a in result["anomalies"]:
            print(f"    [{a['year']}] {a['metric']}: {_pct(a['value'])} vs median "
                  f"{_pct(a['median'])} ({a['deviation_pct']}% deviation) — {a['flag']}")

    print(f"\n  Adjustment log ({len(result['adjustment_log'])} entries):")
    for entry in result["adjustment_log"]:
        tag_label = f"[{entry['tag']}]"
        print(f"    {entry['metric']:<22} {entry['method']:<20} {tag_label}")

    print(f"\n  Normalised at : {result['metadata'].get('normalised_at', '—')}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Formatting helpers
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


def _pct(val) -> str:
    if val is None:
        return "—"
    return f"{val*100:.1f}%"
