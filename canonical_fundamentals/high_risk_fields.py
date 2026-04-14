"""
High-Risk Fields Validator  —  Stage 2.6

Validates and, where possible, re-derives the financial statement fields most
frequently wrong on free financial data websites.

Runs AFTER Stage 2.5 (canonical_fundamentals) so it has access to the
chosen, highest-confidence EBIT, EBITDA, and FCF values.

Why a separate module?
----------------------
Stage 2.5 handles EBIT/FCF/EBITDA derivation (multiple methods, confidence
scoring, comparison).  Stage 2.6 handles everything else that is high-risk
but follows a simpler validation-or-rebuild pattern.

Fields covered
--------------
  HRF-GP   Gross Profit       — validate for impossible values and sector plausibility
  HRF-ND   Net Debt           — rebuild from total_debt + lease_liabilities - cash
  HRF-SH   Shares (diluted)   — cross-check via net_income / eps_diluted; flag staleness
  HRF-TR   Tax Rate           — derive, smooth across years, flag spikes
  HRF-NWC  Working Capital    — derive from balance sheet; cross-check delta direction
  HRF-OCF  Cash from Ops      — supplemental sanity checks beyond anomaly_detector
  HRF-CPX  Capex              — scope and magnitude supplemental
  HRF-MUL  EV Multiples       — rebuild EV and ratios from validated components

Rule IDs
--------
  HRF-GP-001   GP > Revenue (impossible)
  HRF-GP-002   GP == Revenue exactly (mapping collision)
  HRF-GP-003   GP == EBIT exactly (possible row confusion)
  HRF-GP-004   Gross margin outside sector plausible range
  HRF-ND-001   Total debt < long-term debt (impossible)
  HRF-ND-002   Rebuilt net debt diverges from any stored value >25%
  HRF-ND-003   Lease liabilities are zero for a likely asset-heavy company
  HRF-SH-001   Implied diluted shares (NI / EPS) vs shares_outstanding diverge >15%
  HRF-SH-002   Shares unchanged for 3+ consecutive years (backfill suspect)
  HRF-SH-003   Share count YoY change >25%
  HRF-SH-004   Market-cap implied shares vs stored shares diverge >15%
  HRF-TR-001   Effective tax rate outside 0–55% range
  HRF-TR-002   Tax rate YoY swing >15pp
  HRF-TR-003   Negative tax rate with positive pre-tax income
  HRF-TR-004   Zero tax for 2+ profitable years
  HRF-NWC-001  ΔNWC direction mismatch between balance-sheet derivation and OCF statement
  HRF-NWC-002  ΔNWC exceeds 50% of revenue (implausible magnitude)
  HRF-NWC-003  Both current_assets and current_liabilities are zero
  HRF-OCF-001  OCF equals net_income exactly (D&A add-back absent)
  HRF-OCF-002  OCF is zero for a profitable company
  HRF-OCF-003  OCF exceeds 1.5× revenue
  HRF-CPX-001  Capex is zero for 2+ consecutive years (operating company)
  HRF-CPX-002  Capex exceeds revenue (impossible for standard industries)
  HRF-MUL-001  Rebuilt EV diverges from scraped EV >25%
  HRF-MUL-002  EV/EBITDA rebuilt vs any scraped ratio diverges >20%

Output
------
  data["high_risk_validation"] = {
    "status":          "pass" | "warn" | "fail",
    "issues":          [{ rule_id, field, year, severity, message, ... }],
    "year_reports":    { year: { gross_profit, net_debt, shares, tax_rate, nwc } },
    "summary": {
      "critical_count":          int,
      "warning_count":           int,
      "info_count":              int,
      "derived_net_debt_years":  int,
      "smoothed_tax_rate":       float | None,
      "must_review_fields":      [str],
    },
    "rebuilt_multiples": {        # stats-level, not per-year
      "rebuilt_ev":              float | None,
      "ev_ebitda":               float | None,
      "ev_ebit":                 float | None,
      "ev_revenue":              float | None,
      "pe":                      float | None,
      "notes":                   [str],
    }
  }

  canonical_by_year is updated with:
    - "derived_net_debt"  (new field) per year
    - "derived_nwc"       (new field) per year
    - "smoothed_tax_rate" (new field) per year — same value across all years

Pipeline position
-----------------
  Stage 2.5 → run_canonical_fundamentals  → augmented standardised dict
  Stage 2.6 → run_high_risk_validation    → further augmented dict   ← HERE
  Stage 3   → run_validator
"""

from __future__ import annotations

import copy
import statistics
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Sector plausibility bounds for Gross Margin  (fraction of revenue: lo, hi)
# ─────────────────────────────────────────────────────────────────────────────

_GM_BOUNDS: dict[str, tuple[float, float]] = {
    "technology":             (0.30, 0.95),
    "software":               (0.50, 0.95),
    "saas":                   (0.55, 0.95),
    "healthcare":             (0.30, 0.85),
    "pharma":                 (0.40, 0.90),
    "biotech":                (0.30, 0.95),
    "consumer defensive":     (0.15, 0.60),
    "consumer staples":       (0.15, 0.60),
    "consumer discretionary": (0.10, 0.55),
    "consumer cyclical":      (0.10, 0.55),
    "retail":                 (0.05, 0.45),
    "industrials":            (0.10, 0.50),
    "energy":                 (0.05, 0.50),
    "basic materials":        (0.05, 0.45),
    "materials":              (0.05, 0.45),
    "financials":             (0.20, 0.99),   # wide — interest margin varies
    "utilities":              (0.10, 0.60),
    "communication services": (0.30, 0.80),
    "real estate":            (0.30, 0.85),
    "default":                (0.05, 0.99),
}

# Gross margin sectors where zero/near-zero is normal (pass-through revenue)
_LOW_GM_SECTORS = {"retail", "energy", "basic materials", "materials"}

# Sectors where lease liabilities being zero is NOT suspicious
# (genuinely asset-light, service-only, no physical footprint)
_ASSET_LIGHT_SECTORS = {"asset_light", "software", "saas", "technology"}

# Confidence thresholds
_DIVERGE_WARN_PCT  = 15.0   # % divergence that triggers a warning
_DIVERGE_CRIT_PCT  = 25.0   # % divergence that triggers a critical flag
_TAX_RATE_SPIKE_PP = 0.15   # 15pp YoY tax rate swing
_NWC_TO_REV_MAX    = 0.50   # ΔNWC > 50% of revenue is implausible
_SHARES_DIVERGE    = 0.15   # 15% threshold for implied vs stored shares
_SHARES_YOY_CRIT   = 0.25   # 25% YoY share count change → flag


# ─────────────────────────────────────────────────────────────────────────────
# Micro-helpers
# ─────────────────────────────────────────────────────────────────────────────

def _s(v: Any) -> float | None:
    """Safe float conversion — returns None for None, non-numeric, NaN."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _pct_diff(a: float, b: float) -> float | None:
    """Unsigned % difference of a relative to b. None if b ≈ 0."""
    if b is None or abs(b) < 1:
        return None
    return abs(a - b) / abs(b) * 100


def _fmt(v: float | None) -> str:
    if v is None:
        return "—"
    try:
        v = float(v)
        if abs(v) >= 1e12:
            return f"{v / 1e12:.2f}T"
        if abs(v) >= 1e9:
            return f"{v / 1e9:.2f}B"
        if abs(v) >= 1e6:
            return f"{v / 1e6:.0f}M"
        return f"{v:,.1f}"
    except (TypeError, ValueError):
        return str(v)


def _gm_bounds(sector: str | None) -> tuple[float, float]:
    if not sector:
        return _GM_BOUNDS["default"]
    sl = sector.lower()
    for key, bounds in _GM_BOUNDS.items():
        if key in sl:
            return bounds
    return _GM_BOUNDS["default"]


def _is_asset_light(sector: str | None, classifier_template: str | None) -> bool:
    if classifier_template and "asset_light" in classifier_template.lower():
        return True
    if sector:
        sl = sector.lower()
        return any(k in sl for k in _ASSET_LIGHT_SECTORS)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Issue factory  (mirrors anomaly_detector pattern for consistency)
# ─────────────────────────────────────────────────────────────────────────────

def _iss(
    rule_id:  str,
    field:    str,
    year:     str | None,
    severity: str,        # "critical" | "warning" | "info"
    message:  str,
    observed: Any = None,
    expected: str | None = None,
    action:   str = "verify",
    cause:    str = "mapping",
) -> dict:
    _actions = {
        "accept":   "accept",
        "verify":   "verify against second source",
        "rescrape": "re-scrape from different source",
        "exclude":  "exclude from model and use fallback assumption",
        "derive":   "use derived value instead of scraped",
    }
    _causes = {
        "mapping":   "field-mapping error",
        "sign":      "sign convention error",
        "scaling":   "scaling or unit error",
        "stale":     "stale or backfilled data",
        "identity":  "accounting identity violation",
        "pattern":   "time-series pattern anomaly",
        "plausible": "extreme but economically plausible event",
        "onceoff":   "one-off item distorting the metric",
    }
    return {
        "rule_id":            rule_id,
        "field":              field,
        "year":               year,
        "severity":           severity,
        "message":            message,
        "observed_value":     observed,
        "expected_range":     expected,
        "recommended_action": _actions.get(action, action),
        "likely_cause":       _causes.get(cause, cause),
    }


# ─────────────────────────────────────────────────────────────────────────────
# A.  Gross Profit validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate_gross_profit(
    year: str,
    yr: dict,
    sector: str | None,
    issues: list,
) -> dict:
    """
    Validate Gross Profit for impossible values and sector plausibility.

    We cannot re-derive GP from primitives (no COGS in canonical schema), so
    this function:
      1. Flags impossible values (GP > Revenue, GP == Revenue, GP == EBIT)
      2. Flags implausible gross margins for the sector
      3. Returns a structured report for the year

    Returns
    -------
    {
      "gross_profit":    float | None,
      "revenue":         float | None,
      "gross_margin":    float | None,
      "status":          "ok" | "warn" | "critical",
      "flags":           [str],
    }
    """
    gp  = _s(yr.get("gross_profit"))
    rev = _s(yr.get("revenue"))
    ebit = _s(yr.get("ebit"))
    flags = []
    status = "ok"

    if gp is None or rev is None:
        return {
            "gross_profit": gp,
            "revenue": rev,
            "gross_margin": None,
            "status": "ok",
            "flags": ["gross_profit or revenue missing — cannot validate"],
        }

    gross_margin = gp / rev if rev else None

    # HRF-GP-001: GP > Revenue (impossible)
    if gp > rev and rev > 0:
        pct = _pct_diff(gp, rev)
        issues.append(_iss(
            "HRF-GP-001", "gross_profit", year, "critical",
            f"Gross Profit ({_fmt(gp)}) exceeds Revenue ({_fmt(rev)}) — "
            "impossible. Almost certainly a field-mapping error. "
            "Gross Profit will be excluded from margin calculations.",
            observed=f"{gross_margin:.1%}" if gross_margin else "—",
            expected="< 100% gross margin",
            action="rescrape", cause="mapping",
        ))
        flags.append("HRF-GP-001: GP > Revenue")
        status = "critical"

    # HRF-GP-002: GP == Revenue exactly (mapping collision — Revenue scraped into GP row)
    elif rev > 0 and abs(gp - rev) / rev < 0.001:
        issues.append(_iss(
            "HRF-GP-002", "gross_profit", year, "critical",
            f"Gross Profit ({_fmt(gp)}) equals Revenue ({_fmt(rev)}) exactly. "
            "Classic field-mapping error — Revenue was likely scraped into the "
            "Gross Profit row. Gross Profit is unreliable for this year.",
            observed=_fmt(gp),
            expected=f"< {_fmt(rev)} (non-zero cost of goods)",
            action="rescrape", cause="mapping",
        ))
        flags.append("HRF-GP-002: GP == Revenue")
        status = "critical"

    # HRF-GP-003: GP == EBIT exactly (possible row confusion)
    elif ebit is not None and abs(ebit) > 1 and abs(gp - ebit) / abs(ebit) < 0.005:
        issues.append(_iss(
            "HRF-GP-003", "gross_profit", year, "warning",
            f"Gross Profit ({_fmt(gp)}) equals EBIT ({_fmt(ebit)}) exactly. "
            "May indicate operating expenses were scraped into the wrong row, "
            "or that both values were sourced from the same incorrect field.",
            observed=_fmt(gp),
            expected="GP > EBIT for most companies",
            action="verify", cause="mapping",
        ))
        flags.append("HRF-GP-003: GP == EBIT")
        if status == "ok":
            status = "warn"

    # HRF-GP-004: Gross margin outside sector-plausible range
    if gross_margin is not None and status not in ("critical",):
        lo, hi = _gm_bounds(sector)
        if gross_margin > hi:
            issues.append(_iss(
                "HRF-GP-004", "gross_profit", year, "warning",
                f"Gross margin {gross_margin:.1%} exceeds sector ceiling "
                f"({hi:.0%}) for sector '{sector}'. "
                "May reflect an adjusted or mis-mapped gross profit figure.",
                observed=f"{gross_margin:.1%}",
                expected=f"{lo:.0%} – {hi:.0%}",
                action="verify", cause="mapping",
            ))
            flags.append(f"HRF-GP-004: margin {gross_margin:.1%} > sector max {hi:.0%}")
            if status == "ok":
                status = "warn"
        elif gross_margin < lo and rev > 0:
            issues.append(_iss(
                "HRF-GP-004", "gross_profit", year, "warning",
                f"Gross margin {gross_margin:.1%} is below sector floor "
                f"({lo:.0%}) for sector '{sector}'. "
                "Could indicate cost-of-revenue mis-classification or wrong period.",
                observed=f"{gross_margin:.1%}",
                expected=f"{lo:.0%} – {hi:.0%}",
                action="verify", cause="mapping",
            ))
            flags.append(f"HRF-GP-004: margin {gross_margin:.1%} < sector floor {lo:.0%}")
            if status == "ok":
                status = "warn"

    return {
        "gross_profit":  gp,
        "revenue":       rev,
        "gross_margin":  gross_margin,
        "status":        status,
        "flags":         flags,
    }


# ─────────────────────────────────────────────────────────────────────────────
# B.  Net Debt reconstruction
# ─────────────────────────────────────────────────────────────────────────────

def _rebuild_net_debt(
    year: str,
    yr: dict,
    sector: str | None,
    classifier_template: str | None,
    issues: list,
) -> dict:
    """
    Rebuild Net Debt from primitive balance sheet components.

    Net Debt = Total Debt + Lease Liabilities - Cash

    Where Total Debt is resolved as:
      - Use `debt` if debt >= long_term_debt (i.e. debt is total, includes ST+LT)
      - Use `long_term_debt` if debt is missing
      - Flag if debt < long_term_debt (impossible — LT cannot exceed total)

    The derived net_debt is stored as `derived_net_debt` in canonical_by_year.
    It is not written back to `debt` (that field is preserved as-is).

    Returns
    -------
    {
      "total_debt":       float | None,
      "lease_liabilities": float | None,
      "cash":             float | None,
      "derived_net_debt": float | None,
      "status":           "ok" | "warn" | "critical",
      "notes":            [str],
      "flags":            [str],
    }
    """
    debt    = _s(yr.get("debt"))
    lt_debt = _s(yr.get("long_term_debt"))
    leases  = _s(yr.get("lease_liabilities"))
    cash    = _s(yr.get("cash"))
    notes   = []
    flags   = []
    status  = "ok"

    # Resolve total debt
    total_debt = None

    if debt is not None and lt_debt is not None:
        if lt_debt > debt * 1.05:
            # HRF-ND-001: long_term_debt > total debt — impossible
            issues.append(_iss(
                "HRF-ND-001", "debt", year, "critical",
                f"Long-term debt ({_fmt(lt_debt)}) exceeds total debt ({_fmt(debt)}). "
                "Long-term debt cannot exceed total debt — indicates a field-mapping "
                "error, incorrect field labelling, or missing current-portion-of-debt. "
                "Using long_term_debt as total debt floor.",
                observed=f"LT={_fmt(lt_debt)}, Total={_fmt(debt)}",
                expected="long_term_debt ≤ total debt",
                action="verify", cause="mapping",
            ))
            flags.append("HRF-ND-001: LT debt > total debt")
            total_debt = lt_debt   # safer floor
            status = "critical"
        else:
            total_debt = debt   # debt >= long_term_debt — looks correct
            notes.append(f"Total debt resolved from `debt` field ({_fmt(debt)}); "
                         f"long-term portion: {_fmt(lt_debt)}")
    elif debt is not None:
        total_debt = debt
        notes.append(f"Total debt from `debt` field ({_fmt(debt)}); `long_term_debt` absent")
    elif lt_debt is not None:
        total_debt = lt_debt
        notes.append(f"Total debt from `long_term_debt` only ({_fmt(lt_debt)}); "
                     "may exclude short-term portion — net debt understated")
        issues.append(_iss(
            "HRF-ND-001", "debt", year, "warning",
            f"`debt` field absent — using `long_term_debt` ({_fmt(lt_debt)}) as total debt. "
            "This may exclude the current portion of long-term debt and any "
            "short-term borrowings, leading to understated net debt.",
            observed=f"LT={_fmt(lt_debt)}, Total=missing",
            action="verify", cause="mapping",
        ))
        if status == "ok":
            status = "warn"
        flags.append("HRF-ND-001: using LT only — ST portion may be missing")
    else:
        notes.append("Both `debt` and `long_term_debt` absent — cannot derive net debt")

    # Lease liabilities
    lease_amount = leases if (leases is not None and leases >= 0) else 0.0
    if leases is None:
        notes.append("Lease liabilities absent — excluded from net debt (treated as zero)")
        # HRF-ND-003: missing leases for asset-heavy companies
        if not _is_asset_light(sector, classifier_template) and total_debt is not None:
            issues.append(_iss(
                "HRF-ND-003", "lease_liabilities", year, "info",
                "Lease liabilities are absent. For asset-heavy companies, "
                "IFRS 16 / ASC 842 lease obligations can be material. "
                "Net debt may be understated if the company has significant leases.",
                action="verify", cause="mapping",
            ))
    else:
        notes.append(f"Lease liabilities: {_fmt(leases)}")

    # Derive net debt
    derived_net_debt = None
    if total_debt is not None and cash is not None:
        derived_net_debt = total_debt + lease_amount - cash
        notes.append(
            f"Net Debt = {_fmt(total_debt)} (debt) "
            f"+ {_fmt(lease_amount)} (leases) "
            f"- {_fmt(cash)} (cash) "
            f"= {_fmt(derived_net_debt)}"
        )

        # HRF-ND-002: Compare against any pre-existing net debt in the data
        # (some scrapers provide net_debt directly — not a canonical field but may be stored)
        stored_nd = _s(yr.get("net_debt"))
        if stored_nd is not None and abs(stored_nd) > 1:
            pd = _pct_diff(derived_net_debt, stored_nd)
            if pd is not None and pd > _DIVERGE_CRIT_PCT:
                issues.append(_iss(
                    "HRF-ND-002", "net_debt", year, "warning",
                    f"Rebuilt net debt ({_fmt(derived_net_debt)}) diverges from "
                    f"stored net_debt ({_fmt(stored_nd)}) by {pd:.1f}%. "
                    "Likely difference in lease inclusion, current-portion treatment, "
                    "or cash definition (restricted vs unrestricted).",
                    observed=_fmt(derived_net_debt),
                    expected=f"≈ {_fmt(stored_nd)}",
                    action="verify", cause="identity",
                ))
                flags.append(f"HRF-ND-002: {pd:.0f}% divergence from stored net_debt")
                if status == "ok":
                    status = "warn"
    elif total_debt is None:
        notes.append("Net debt cannot be derived — total debt unavailable")
    elif cash is None:
        notes.append("Net debt cannot be derived — cash unavailable")

    return {
        "total_debt":        total_debt,
        "lease_liabilities": leases,
        "cash":              cash,
        "derived_net_debt":  derived_net_debt,
        "status":            status,
        "notes":             notes,
        "flags":             flags,
    }


# ─────────────────────────────────────────────────────────────────────────────
# C.  Shares Outstanding validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate_shares(
    year: str,
    yr: dict,
    prior_yr: dict | None,
    stats: dict,
    issues: list,
) -> dict:
    """
    Validate shares_outstanding for:
      - Basic vs diluted confusion (cross-check via NI / EPS)
      - Staleness (unchanged for 3+ years — detected in multi-year pass)
      - Large YoY swings
      - Market-cap-implied cross-check (if market_cap and current_price in stats)

    Returns
    -------
    {
      "shares_outstanding":   float | None,
      "implied_from_eps":     float | None,
      "implied_from_price":   float | None,
      "eps_divergence_pct":   float | None,
      "price_divergence_pct": float | None,
      "yoy_change_pct":       float | None,
      "status":               "ok" | "warn" | "critical",
      "flags":                [str],
    }
    """
    shares = _s(yr.get("shares_outstanding"))
    ni     = _s(yr.get("net_income"))
    eps    = _s(yr.get("eps_diluted"))
    flags  = []
    status = "ok"

    implied_from_eps   = None
    implied_from_price = None
    eps_div_pct        = None
    price_div_pct      = None
    yoy_pct            = None

    if shares is None:
        return {
            "shares_outstanding": None,
            "implied_from_eps": None,
            "implied_from_price": None,
            "eps_divergence_pct": None,
            "price_divergence_pct": None,
            "yoy_change_pct": None,
            "status": "ok",
            "flags": ["shares_outstanding absent — cannot validate"],
        }

    # HRF-SH-001: Implied diluted shares from NI / EPS
    if ni is not None and eps is not None and abs(eps) > 0.001:
        implied_from_eps = ni / eps
        eps_div_pct = _pct_diff(implied_from_eps, shares)
        if eps_div_pct is not None and eps_div_pct > _SHARES_DIVERGE * 100:
            sev = "critical" if eps_div_pct > 30 else "warning"
            issues.append(_iss(
                "HRF-SH-001", "shares_outstanding", year, sev,
                f"Stored shares_outstanding ({_fmt(shares)}) diverges "
                f"{eps_div_pct:.1f}% from shares implied by NI/EPS_diluted "
                f"({_fmt(implied_from_eps)}). "
                "Likely cause: basic vs diluted confusion, stale share count, "
                "or ADR conversion mismatch. DCF per-share calculation may be wrong.",
                observed=_fmt(shares),
                expected=f"≈ {_fmt(implied_from_eps)} (NI/EPS implied)",
                action="verify", cause="mapping",
            ))
            flags.append(f"HRF-SH-001: {eps_div_pct:.0f}% divergence from NI/EPS implied")
            status = sev

    # HRF-SH-003: YoY change
    if prior_yr is not None:
        prior_shares = _s(prior_yr.get("shares_outstanding"))
        if prior_shares is not None and prior_shares > 0:
            yoy_pct = (shares - prior_shares) / prior_shares
            if abs(yoy_pct) > _SHARES_YOY_CRIT:
                issues.append(_iss(
                    "HRF-SH-003", "shares_outstanding", year, "warning",
                    f"Shares outstanding changed {yoy_pct:+.1%} YoY "
                    f"({_fmt(prior_shares)} → {_fmt(shares)}). "
                    "Large changes may reflect a stock split, reverse split, "
                    "major buyback, or share issuance. Verify the cause before "
                    "using this as the denominator in per-share calculations.",
                    observed=f"{yoy_pct:+.1%}",
                    expected=f"< ±{_SHARES_YOY_CRIT:.0%} without corporate action",
                    action="verify", cause="plausible",
                ))
                flags.append(f"HRF-SH-003: {yoy_pct:+.0%} YoY share count change")
                if status == "ok":
                    status = "warn"

    # HRF-SH-004: Market-cap implied shares (stats-level, only for most recent year)
    # This check is done here and stored; the multi-year stale check is done in the
    # main loop after all years are processed.
    mc    = _s(stats.get("market_cap"))
    price = _s(stats.get("current_price"))
    if mc is not None and price is not None and price > 0:
        implied_from_price = mc / price
        price_div_pct = _pct_diff(implied_from_price, shares)
        if price_div_pct is not None and price_div_pct > _SHARES_DIVERGE * 100:
            issues.append(_iss(
                "HRF-SH-004", "shares_outstanding", year, "warning",
                f"Stored shares_outstanding ({_fmt(shares)}) diverges "
                f"{price_div_pct:.1f}% from market-cap-implied shares "
                f"({_fmt(implied_from_price)} = market_cap / current_price). "
                "May indicate stale share count or ADR conversion factor applied "
                "to one but not the other.",
                observed=_fmt(shares),
                expected=f"≈ {_fmt(implied_from_price)} (market_cap / price implied)",
                action="verify", cause="stale",
            ))
            flags.append(f"HRF-SH-004: {price_div_pct:.0f}% divergence from market-cap implied")
            if status == "ok":
                status = "warn"

    return {
        "shares_outstanding":   shares,
        "implied_from_eps":     implied_from_eps,
        "implied_from_price":   implied_from_price,
        "eps_divergence_pct":   eps_div_pct,
        "price_divergence_pct": price_div_pct,
        "yoy_change_pct":       yoy_pct,
        "status":               status,
        "flags":                flags,
    }


def _check_shares_staleness(
    canonical_by_year: dict,
    issues: list,
    min_unchanged_years: int = 3,
) -> None:
    """
    HRF-SH-002: Flag if shares_outstanding is identical across 3+ consecutive years.
    This pattern strongly suggests the share count was backfilled from a single
    snapshot (e.g. current shares pulled into all historical rows).
    """
    years_sorted = sorted(canonical_by_year.keys())
    if len(years_sorted) < min_unchanged_years:
        return

    share_vals = []
    for yr_key in years_sorted:
        v = _s(canonical_by_year[yr_key].get("shares_outstanding"))
        share_vals.append((yr_key, v))

    # Slide a window looking for min_unchanged_years identical non-None values
    for i in range(len(share_vals) - min_unchanged_years + 1):
        window = share_vals[i : i + min_unchanged_years]
        vals   = [v for _, v in window if v is not None]
        years  = [y for y, v in window if v is not None]
        if len(vals) < min_unchanged_years:
            continue
        if len(set(vals)) == 1:   # all identical
            issues.append(_iss(
                "HRF-SH-002", "shares_outstanding", None, "warning",
                f"shares_outstanding is identical ({_fmt(vals[0])}) across "
                f"{len(years)} consecutive years ({years[0]}–{years[-1]}). "
                "Strong indicator that the current share count was backfilled "
                "into all historical rows. Per-share metrics in earlier years "
                "will be wrong if buybacks or issuances occurred.",
                observed=f"{_fmt(vals[0])} × {len(years)} years",
                expected="Varying share counts reflecting actual buybacks/issuances",
                action="verify", cause="stale",
            ))
            break   # one flag is enough


# ─────────────────────────────────────────────────────────────────────────────
# D.  Tax Rate validation and smoothing
# ─────────────────────────────────────────────────────────────────────────────

def _validate_tax_rate(
    year: str,
    yr: dict,
    issues: list,
) -> dict:
    """
    Derive, validate, and flag effective tax rate for a single year.

    Returns
    -------
    {
      "tax_provision":  float | None,
      "pre_tax_income": float | None,
      "effective_rate": float | None,
      "status":         "ok" | "warn" | "critical" | "n/a",
      "notes":          [str],
      "flags":          [str],
    }
    """
    tax     = _s(yr.get("tax_provision"))
    pre_tax = _s(yr.get("pre_tax_income"))
    flags   = []
    status  = "ok"
    notes   = []

    if tax is None or pre_tax is None:
        return {
            "tax_provision":  tax,
            "pre_tax_income": pre_tax,
            "effective_rate": None,
            "status":         "n/a",
            "notes":          ["tax_provision or pre_tax_income absent"],
            "flags":          [],
        }

    # No meaningful rate if pre-tax income is ~zero
    if abs(pre_tax) < 1:
        return {
            "tax_provision":  tax,
            "pre_tax_income": pre_tax,
            "effective_rate": None,
            "status":         "n/a",
            "notes":          ["pre_tax_income ≈ 0 — rate undefined"],
            "flags":          [],
        }

    effective_rate = tax / pre_tax

    # HRF-TR-003: Negative tax rate with positive pre-tax income
    if pre_tax > 0 and effective_rate < 0:
        issues.append(_iss(
            "HRF-TR-003", "tax_provision", year, "warning",
            f"Negative effective tax rate ({effective_rate:.1%}) with positive "
            f"pre-tax income ({_fmt(pre_tax)}). This can occur from large tax "
            "credits, deferred tax reversals, or R&D credits, but is unusual. "
            "Verify that tax_provision is correctly signed.",
            observed=f"{effective_rate:.1%}",
            expected="0%–55% for profitable company",
            action="verify", cause="onceoff",
        ))
        flags.append(f"HRF-TR-003: negative rate {effective_rate:.1%}")
        status = "warn"
        notes.append("Negative effective rate — tax credit or deferred tax reversal likely")

    # HRF-TR-001: Rate outside plausible range (0–55%)
    elif effective_rate > 0.55:
        issues.append(_iss(
            "HRF-TR-001", "tax_provision", year, "warning",
            f"Effective tax rate ({effective_rate:.1%}) exceeds 55%. "
            "Unusually high rates typically reflect valuation allowance charges, "
            "goodwill impairments reducing pre-tax income, or one-off tax assessments. "
            "Use a smoothed rate rather than this year's figure for DCF assumptions.",
            observed=f"{effective_rate:.1%}",
            expected="0%–55%",
            action="verify", cause="onceoff",
        ))
        flags.append(f"HRF-TR-001: rate {effective_rate:.1%} > 55%")
        status = "warn"
        notes.append("Rate >55% — likely one-off tax charge; smoothed rate preferred")

    elif effective_rate < 0 and pre_tax < 0:
        # Loss year: tax charge on loss is common (valuation allowance)
        notes.append(
            f"Loss year: effective rate {effective_rate:.1%} (pre-tax loss, "
            "tax charge — likely valuation allowance or non-deductible items)"
        )
    else:
        notes.append(f"Effective rate: {effective_rate:.1%} (plausible)")

    return {
        "tax_provision":  tax,
        "pre_tax_income": pre_tax,
        "effective_rate": effective_rate,
        "status":         status,
        "notes":          notes,
        "flags":          flags,
    }


def _check_tax_rate_yoy(
    year:      str,
    curr_rate: float | None,
    prior_rate: float | None,
    issues:    list,
) -> None:
    """HRF-TR-002: Flag large YoY tax rate swings."""
    if curr_rate is None or prior_rate is None:
        return
    swing = abs(curr_rate - prior_rate)
    if swing > _TAX_RATE_SPIKE_PP:
        issues.append(_iss(
            "HRF-TR-002", "tax_provision", year, "warning",
            f"Effective tax rate swung {swing:.1%}pp YoY "
            f"({prior_rate:.1%} → {curr_rate:.1%}). "
            "Large swings usually reflect one-off items (deferred tax reversals, "
            "valuation allowances, R&D credits). Use a multi-year smoothed rate "
            "for DCF tax assumption rather than the spike year.",
            observed=f"{curr_rate:.1%} (was {prior_rate:.1%})",
            expected=f"< {_TAX_RATE_SPIKE_PP:.0%}pp YoY change",
            action="verify", cause="onceoff",
        ))


def _check_zero_tax(canonical_by_year: dict, issues: list) -> None:
    """
    HRF-TR-004: Flag 2+ consecutive years of zero (or near-zero) tax
    for a company with consistently positive pre-tax income.
    """
    years_sorted = sorted(canonical_by_year.keys())
    zero_tax_profitable = []
    for yr_key in years_sorted:
        d       = canonical_by_year[yr_key]
        tax     = _s(d.get("tax_provision"))
        pre_tax = _s(d.get("pre_tax_income"))
        if tax is not None and pre_tax is not None:
            if pre_tax > 1 and abs(tax) < pre_tax * 0.01:
                zero_tax_profitable.append(yr_key)

    if len(zero_tax_profitable) >= 2:
        issues.append(_iss(
            "HRF-TR-004", "tax_provision", None, "warning",
            f"Near-zero tax provision in {len(zero_tax_profitable)} profitable year(s) "
            f"({', '.join(zero_tax_profitable)}). "
            "Possible causes: large loss carryforwards, substantial R&D credits, "
            "or tax_provision field being misread as zero. "
            "A 0% rate will over-state FCF in the DCF if assumed going-forward.",
            observed=f"~0% tax in years: {', '.join(zero_tax_profitable)}",
            expected="Non-zero tax for profitable company",
            action="verify", cause="mapping",
        ))


def _smooth_tax_rate(effective_rates: list[float]) -> float | None:
    """
    Compute a robust smoothed tax rate from a list of per-year effective rates.

    Filters out extreme values (< 0 or > 0.55) before taking the median.
    Falls back to 25% if no plausible rates are available.
    """
    plausible = [r for r in effective_rates if r is not None and 0.0 <= r <= 0.55]
    if not plausible:
        return None
    return statistics.median(plausible)


# ─────────────────────────────────────────────────────────────────────────────
# E.  Working Capital validation
# ─────────────────────────────────────────────────────────────────────────────

def _derive_and_validate_nwc(
    year: str,
    yr: dict,
    prior_yr: dict | None,
    issues: list,
) -> dict:
    """
    Derive NWC from balance sheet and cross-check ΔNWC direction.

    NWC (derived) = current_assets - current_liabilities

    Cross-check:
      delta_nwc_bs   = NWC(t) - NWC(t-1)          from balance sheet derivation
      delta_nwc_ocf  = change_in_working_cap field  from OCF statement

    Sign convention warning: OCF statements often report ΔNWC with the opposite
    sign to the balance sheet movement (a working capital increase absorbs cash,
    so appears as negative in OCF). We handle this by checking DIRECTION only,
    not magnitude, and noting the convention.

    Returns
    -------
    {
      "current_assets":      float | None,
      "current_liabilities": float | None,
      "derived_nwc":         float | None,
      "prior_derived_nwc":   float | None,
      "delta_nwc_bs":        float | None,   # balance sheet ΔNWC
      "delta_nwc_ocf":       float | None,   # from OCF statement
      "direction_mismatch":  bool,
      "status":              "ok" | "warn" | "critical",
      "notes":               [str],
      "flags":               [str],
    }
    """
    ca  = _s(yr.get("current_assets"))
    cl  = _s(yr.get("current_liabilities"))
    dnwc_ocf = _s(yr.get("change_in_working_cap"))
    rev = _s(yr.get("revenue"))
    flags = []
    notes = []
    status = "ok"

    derived_nwc       = None
    prior_derived_nwc = None
    delta_nwc_bs      = None
    direction_mismatch = False

    # HRF-NWC-003: Both zero — probably missing data, not real
    if ca is not None and cl is not None:
        if ca == 0 and cl == 0:
            issues.append(_iss(
                "HRF-NWC-003", "current_assets", year, "info",
                "Both current_assets and current_liabilities are zero. "
                "This almost certainly means the balance sheet data was not "
                "scraped rather than reflecting a genuine zero balance. "
                "NWC cannot be derived reliably for this year.",
                action="rescrape", cause="mapping",
            ))
            flags.append("HRF-NWC-003: both CA and CL are zero")
        else:
            derived_nwc = ca - cl
            notes.append(
                f"Derived NWC = CA ({_fmt(ca)}) - CL ({_fmt(cl)}) = {_fmt(derived_nwc)}"
            )
    elif ca is None and cl is None:
        notes.append("current_assets and current_liabilities both absent — NWC not derivable")
    else:
        notes.append(
            f"Partial balance sheet: CA={_fmt(ca)}, CL={_fmt(cl)} — NWC not derived"
        )

    # Balance sheet ΔNWC
    if prior_yr is not None and derived_nwc is not None:
        p_ca = _s(prior_yr.get("current_assets"))
        p_cl = _s(prior_yr.get("current_liabilities"))
        if p_ca is not None and p_cl is not None:
            prior_derived_nwc = p_ca - p_cl
            delta_nwc_bs = derived_nwc - prior_derived_nwc
            notes.append(f"Balance sheet ΔNWC: {_fmt(delta_nwc_bs)}")

    # HRF-NWC-001: Direction mismatch between balance sheet ΔNWC and OCF statement
    # Convention: OCF statement shows ΔNWC as the CASH IMPACT (opposite sign to NWC change).
    #   NWC rises (more working capital tied up) → cash absorbed → OCF ΔNWC negative
    #   NWC falls (working capital released)     → cash released  → OCF ΔNWC positive
    # So: bs_direction and ocf_direction should be OPPOSITE signs for typical convention.
    if delta_nwc_bs is not None and dnwc_ocf is not None:
        same_sign = (delta_nwc_bs > 0) == (dnwc_ocf > 0)
        if same_sign and abs(delta_nwc_bs) > 1 and abs(dnwc_ocf) > 1:
            direction_mismatch = True
            issues.append(_iss(
                "HRF-NWC-001", "change_in_working_cap", year, "warning",
                f"Balance sheet ΔNWC ({_fmt(delta_nwc_bs)}) and OCF statement "
                f"ΔNWC ({_fmt(dnwc_ocf)}) have the SAME sign, but they should "
                "be opposite under standard cash-flow sign conventions. "
                "This may mean the change_in_working_cap field has an incorrect "
                "sign, or was sourced from a different line item.",
                observed=f"BS ΔNWC={_fmt(delta_nwc_bs)}, OCF ΔNWC={_fmt(dnwc_ocf)}",
                expected="Opposite signs (NWC build = cash absorbed = negative OCF impact)",
                action="verify", cause="sign",
            ))
            flags.append("HRF-NWC-001: ΔNWC direction mismatch")
            if status == "ok":
                status = "warn"

    # HRF-NWC-002: ΔNWC implausibly large vs revenue
    for label, delta in [("BS ΔNWC", delta_nwc_bs), ("OCF ΔNWC", dnwc_ocf)]:
        if delta is not None and rev is not None and rev > 0:
            if abs(delta) > rev * _NWC_TO_REV_MAX:
                issues.append(_iss(
                    "HRF-NWC-002", "change_in_working_cap", year, "warning",
                    f"{label} ({_fmt(delta)}) is {abs(delta)/rev:.0%} of revenue "
                    f"({_fmt(rev)}), exceeding the plausibility ceiling of "
                    f"{_NWC_TO_REV_MAX:.0%}. May indicate a one-off acquisition "
                    "or disposal of working capital, or a mapping error.",
                    observed=f"{abs(delta)/rev:.0%} of revenue",
                    expected=f"< {_NWC_TO_REV_MAX:.0%} of revenue",
                    action="verify", cause="plausible",
                ))
                flags.append(f"HRF-NWC-002: {label} is {abs(delta)/rev:.0%} of revenue")
                if status == "ok":
                    status = "warn"

    return {
        "current_assets":      ca,
        "current_liabilities": cl,
        "derived_nwc":         derived_nwc,
        "prior_derived_nwc":   prior_derived_nwc,
        "delta_nwc_bs":        delta_nwc_bs,
        "delta_nwc_ocf":       dnwc_ocf,
        "direction_mismatch":  direction_mismatch,
        "status":              status,
        "notes":               notes,
        "flags":               flags,
    }


# ─────────────────────────────────────────────────────────────────────────────
# F.  OCF supplemental checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_ocf_supplemental(
    year: str,
    yr: dict,
    issues: list,
) -> list[str]:
    """
    Supplemental OCF sanity checks beyond what anomaly_detector covers.

    Checks:
      HRF-OCF-001  OCF == Net Income exactly  (D&A add-back absent)
      HRF-OCF-002  OCF is zero for a profitable company
      HRF-OCF-003  OCF > 1.5× Revenue

    Returns list of flags triggered.
    """
    ocf = _s(yr.get("operating_cash_flow"))
    ni  = _s(yr.get("net_income"))
    rev = _s(yr.get("revenue"))
    flags = []

    if ocf is None:
        return flags

    # HRF-OCF-001: OCF == Net Income exactly
    if ni is not None and abs(ni) > 1 and abs(ocf - ni) / abs(ni) < 0.001:
        issues.append(_iss(
            "HRF-OCF-001", "operating_cash_flow", year, "warning",
            f"Operating Cash Flow ({_fmt(ocf)}) equals Net Income ({_fmt(ni)}) exactly. "
            "A genuine OCF should differ via D&A add-back, working capital movements, "
            "and non-cash charges. This exact match strongly suggests the OCF field "
            "was mis-mapped (Net Income scraped into Operating Cash Flow).",
            observed=_fmt(ocf),
            expected="OCF ≠ Net Income (should include D&A and WC movements)",
            action="rescrape", cause="mapping",
        ))
        flags.append("HRF-OCF-001: OCF == Net Income")

    # HRF-OCF-002: OCF is zero for a profitable company
    elif ni is not None and ni > 1 and abs(ocf) < 1:
        issues.append(_iss(
            "HRF-OCF-002", "operating_cash_flow", year, "warning",
            f"Operating Cash Flow is zero (or near-zero) while Net Income is "
            f"{_fmt(ni)}. For most businesses, OCF should be positive when NI "
            "is positive (before large working capital changes). Possible "
            "mis-mapping or missing cash flow statement data.",
            observed=_fmt(ocf),
            expected=f"≈ {_fmt(ni)} adjusted for non-cash items",
            action="rescrape", cause="mapping",
        ))
        flags.append("HRF-OCF-002: OCF is zero for profitable company")

    # HRF-OCF-003: OCF > 1.5× Revenue
    if rev is not None and rev > 0 and ocf > rev * 1.5:
        issues.append(_iss(
            "HRF-OCF-003", "operating_cash_flow", year, "warning",
            f"Operating Cash Flow ({_fmt(ocf)}) is {ocf/rev:.0%} of Revenue "
            f"({_fmt(rev)}), which is extreme. OCF can exceed 100% of revenue "
            "in unusual circumstances (e.g. collecting deferred revenue), "
            "but >150% is highly suspect.",
            observed=f"{ocf/rev:.0%} of revenue",
            expected="< 150% of revenue",
            action="verify", cause="mapping",
        ))
        flags.append(f"HRF-OCF-003: OCF is {ocf/rev:.0%} of revenue")

    return flags


# ─────────────────────────────────────────────────────────────────────────────
# G.  Capex supplemental checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_capex_supplemental(
    canonical_by_year: dict,
    classifier_template: str | None,
    issues: list,
) -> list[str]:
    """
    Supplemental Capex checks:
      HRF-CPX-001  Capex == 0 for 2+ consecutive years (operating company)
      HRF-CPX-002  Capex > Revenue (impossible for standard industries)

    These are multi-year checks, so we pass the full dict.
    Returns list of flags triggered.
    """
    flags = []
    years_sorted = sorted(canonical_by_year.keys())

    # HRF-CPX-002: Per-year — Capex > Revenue
    for yr_key in years_sorted:
        d     = canonical_by_year[yr_key]
        capex = _s(d.get("capex"))
        rev   = _s(d.get("revenue"))
        if capex is None or rev is None or rev <= 0:
            continue
        capex_abs = abs(capex)   # handle negative convention
        if capex_abs > rev:
            issues.append(_iss(
                "HRF-CPX-002", "capex", yr_key, "critical",
                f"Capex ({_fmt(capex_abs)}) exceeds Revenue ({_fmt(rev)}) — "
                "impossible for standard operations. Almost certainly reflects "
                "a scaling error, sign error, or inclusion of a large acquisition "
                "that should be in investing activities, not maintenance capex.",
                observed=_fmt(capex_abs),
                expected=f"< {_fmt(rev)} (< 100% of revenue)",
                action="rescrape", cause="scaling",
            ))
            flags.append(f"HRF-CPX-002 ({yr_key}): capex > revenue")

    # HRF-CPX-001: 2+ consecutive zero-capex years (for non-asset-light)
    asset_light = _is_asset_light(None, classifier_template)
    if not asset_light:
        zero_capex_years = []
        for yr_key in years_sorted:
            d     = canonical_by_year[yr_key]
            capex = _s(d.get("capex"))
            if capex is not None and abs(capex) < 1:
                zero_capex_years.append(yr_key)

        if len(zero_capex_years) >= 2:
            # Check they are consecutive
            consec_groups = []
            group = [zero_capex_years[0]]
            for i in range(1, len(zero_capex_years)):
                if (
                    zero_capex_years[i].isdigit()
                    and zero_capex_years[i - 1].isdigit()
                    and int(zero_capex_years[i]) - int(zero_capex_years[i - 1]) == 1
                ):
                    group.append(zero_capex_years[i])
                else:
                    consec_groups.append(group)
                    group = [zero_capex_years[i]]
            consec_groups.append(group)

            for grp in consec_groups:
                if len(grp) >= 2:
                    issues.append(_iss(
                        "HRF-CPX-001", "capex", None, "warning",
                        f"Capex is zero in {len(grp)} consecutive year(s) "
                        f"({grp[0]}–{grp[-1]}) for a non-asset-light company. "
                        "Operating businesses always have some maintenance capex. "
                        "This likely means capex data is missing or was scraped "
                        "as zero from an incomplete source.",
                        observed=f"Capex=0 in years: {', '.join(grp)}",
                        expected="Non-zero capex (maintenance + growth investment)",
                        action="rescrape", cause="mapping",
                    ))
                    flags.append(f"HRF-CPX-001: zero capex in {', '.join(grp)}")

    return flags


# ─────────────────────────────────────────────────────────────────────────────
# H.  EV Multiples rebuild from validated components
# ─────────────────────────────────────────────────────────────────────────────

def _rebuild_ev_multiples(
    canonical_by_year: dict,
    stats: dict,
    issues: list,
) -> dict:
    """
    Rebuild Enterprise Value and EV-based multiples from validated components.

    EV = Market Cap + Total Debt + Lease Liabilities - Cash

    Uses the most recent year's derived net debt components and the current
    market cap from stats (which is live/current, not historical).

    This is a stats-level calculation (one per company, not per-year).

    Returns
    -------
    {
      "rebuilt_ev":   float | None,
      "ev_ebitda":    float | None,
      "ev_ebit":      float | None,
      "ev_revenue":   float | None,
      "pe":           float | None,
      "components": {
        "market_cap": float | None,
        "total_debt": float | None,
        "leases":     float | None,
        "cash":       float | None,
      },
      "notes": [str],
    }
    """
    notes = []
    mc = _s(stats.get("market_cap"))
    if mc is None:
        notes.append("market_cap absent from stats — cannot rebuild EV")
        return {
            "rebuilt_ev": None,
            "ev_ebitda":  None,
            "ev_ebit":    None,
            "ev_revenue": None,
            "pe":         None,
            "components": {},
            "notes":      notes,
        }

    # Use the most recent year for financial components
    years_sorted = sorted(canonical_by_year.keys(), reverse=True)
    if not years_sorted:
        notes.append("No years in canonical_by_year — cannot rebuild EV")
        return {
            "rebuilt_ev": None,
            "ev_ebitda":  None,
            "ev_ebit":    None,
            "ev_revenue": None,
            "pe":         None,
            "components": {"market_cap": mc},
            "notes":      notes,
        }

    latest_yr     = years_sorted[0]
    d             = canonical_by_year[latest_yr]
    total_debt    = _s(d.get("derived_net_debt"))   # if ND module already ran
    # Fall back to raw components if derived_net_debt not yet stored
    debt_raw      = _s(d.get("debt"))
    leases_raw    = _s(d.get("lease_liabilities"))
    cash_raw      = _s(d.get("cash"))

    # Use derived net debt if available, otherwise rebuild inline
    if total_debt is not None:
        rebuilt_ev  = mc + total_debt
        total_d_for_note = total_debt
        lease_note  = "(leases already included in derived_net_debt)"
        cash_note   = "(cash already deducted in derived_net_debt)"
        notes.append(
            f"EV = market_cap ({_fmt(mc)}) + derived_net_debt ({_fmt(total_debt)}) "
            f"= {_fmt(mc + total_debt)} [{latest_yr} balance sheet data]"
        )
    else:
        # Direct rebuild
        debt_val   = debt_raw   if debt_raw   is not None else 0.0
        leases_val = leases_raw if leases_raw is not None else 0.0
        cash_val   = cash_raw   if cash_raw   is not None else 0.0
        rebuilt_ev = mc + debt_val + leases_val - cash_val
        total_d_for_note = debt_val
        lease_note  = f"lease_liabilities={_fmt(leases_val)}"
        cash_note   = f"cash={_fmt(cash_val)}"
        notes.append(
            f"EV = {_fmt(mc)} (mkt_cap) + {_fmt(debt_val)} (debt) "
            f"+ {_fmt(leases_val)} (leases) - {_fmt(cash_val)} (cash) "
            f"= {_fmt(rebuilt_ev)} [{latest_yr}]"
        )

    if rebuilt_ev is None or rebuilt_ev <= 0:
        notes.append("Rebuilt EV is zero or negative — multiples undefined")
        return {
            "rebuilt_ev": rebuilt_ev,
            "ev_ebitda":  None,
            "ev_ebit":    None,
            "ev_revenue": None,
            "pe":         None,
            "components": {
                "market_cap":  mc,
                "total_debt":  total_d_for_note,
                "leases":      leases_raw,
                "cash":        cash_raw,
            },
            "notes": notes,
        }

    # Compute multiples from validated (chosen) fields
    ebitda = _s(d.get("ebitda"))
    ebit   = _s(d.get("ebit"))
    rev    = _s(d.get("revenue"))
    ni     = _s(d.get("net_income"))

    ev_ebitda  = rebuilt_ev / ebitda if (ebitda and ebitda > 0) else None
    ev_ebit    = rebuilt_ev / ebit   if (ebit   and ebit   > 0) else None
    ev_revenue = rebuilt_ev / rev    if (rev    and rev    > 0) else None
    pe         = mc / ni             if (ni     and ni     > 0) else None

    # Cap implausible multiples — these indicate a bad component, not a real ratio
    def _cap(v: float | None, ceiling: float) -> float | None:
        return None if (v is not None and v > ceiling) else v

    ev_ebitda  = _cap(ev_ebitda,  500.0)
    ev_ebit    = _cap(ev_ebit,    500.0)
    ev_revenue = _cap(ev_revenue, 100.0)
    pe         = _cap(pe,         500.0)

    if ev_ebitda:
        notes.append(f"EV/EBITDA (rebuilt): {ev_ebitda:.1f}×")
    if ev_ebit:
        notes.append(f"EV/EBIT (rebuilt): {ev_ebit:.1f}×")
    if ev_revenue:
        notes.append(f"EV/Revenue (rebuilt): {ev_revenue:.2f}×")
    if pe:
        notes.append(f"P/E (rebuilt): {pe:.1f}×")

    # HRF-MUL-001: Compare rebuilt EV to any scraped EV in stats
    scraped_ev = _s(stats.get("enterprise_value")) or _s(stats.get("enterpriseValue"))
    if scraped_ev is not None and abs(scraped_ev) > 1:
        pd = _pct_diff(rebuilt_ev, scraped_ev)
        if pd is not None and pd > _DIVERGE_CRIT_PCT:
            issues.append(_iss(
                "HRF-MUL-001", "enterprise_value", latest_yr, "warning",
                f"Rebuilt EV ({_fmt(rebuilt_ev)}) diverges {pd:.1f}% from "
                f"scraped EV ({_fmt(scraped_ev)}). "
                "Common causes: stale market cap in scraped figure, different "
                "debt/lease definition, or restricted cash excluded from one "
                "but not the other. Use rebuilt EV for comparables.",
                observed=_fmt(rebuilt_ev),
                expected=f"≈ {_fmt(scraped_ev)}",
                action="verify", cause="identity",
            ))

    return {
        "rebuilt_ev":  rebuilt_ev,
        "ev_ebitda":   ev_ebitda,
        "ev_ebit":     ev_ebit,
        "ev_revenue":  ev_revenue,
        "pe":          pe,
        "components": {
            "market_cap":  mc,
            "total_debt":  total_d_for_note,
            "leases":      leases_raw,
            "cash":        cash_raw,
        },
        "notes": notes,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_high_risk_validation(standardised: dict) -> dict:
    """
    Run high-risk field validation on the output of run_canonical_fundamentals.

    Parameters
    ----------
    standardised : dict
        Output of run_canonical_fundamentals() (Stage 2.5).
        Must contain "canonical_by_year" and "stats".

    Returns
    -------
    Augmented standardised dict with:
      - "high_risk_validation" key containing the full audit report
      - canonical_by_year updated with derived_net_debt, derived_nwc,
        and smoothed_tax_rate per year
    """
    raw_cby    = standardised.get("canonical_by_year", {})
    stats      = standardised.get("stats", {})
    sector     = stats.get("sector")
    classifier_template = standardised.get("classifier", {}).get("template")

    canonical_by_year = copy.deepcopy(raw_cby)
    issues: list[dict] = []
    year_reports: dict[str, dict] = {}

    years_sorted = sorted(canonical_by_year.keys(), reverse=True)

    # ── Per-year pass ──────────────────────────────────────────────────────────
    effective_rates: list[float] = []

    for i, year in enumerate(years_sorted):
        yr       = canonical_by_year[year]
        prior_yr = canonical_by_year.get(years_sorted[i + 1]) if i + 1 < len(years_sorted) else None

        # A. Gross Profit
        gp_report = _validate_gross_profit(year, yr, sector, issues)

        # B. Net Debt
        nd_report = _rebuild_net_debt(year, yr, sector, classifier_template, issues)
        if nd_report["derived_net_debt"] is not None:
            yr["derived_net_debt"] = nd_report["derived_net_debt"]

        # C. Shares  (market-cap check only for most recent year to avoid stale-price issue)
        do_price_check = (i == 0)   # only latest year
        sh_report = _validate_shares(
            year, yr,
            prior_yr if prior_yr else None,
            stats if do_price_check else {},
            issues,
        )

        # D. Tax Rate
        tr_report = _validate_tax_rate(year, yr, issues)
        if tr_report["effective_rate"] is not None:
            effective_rates.append(tr_report["effective_rate"])
        prior_rate = None
        if prior_yr is not None:
            p_tax     = _s(prior_yr.get("tax_provision"))
            p_pretax  = _s(prior_yr.get("pre_tax_income"))
            if p_tax is not None and p_pretax is not None and abs(p_pretax) > 1:
                prior_rate = p_tax / p_pretax
        if prior_rate is not None and tr_report["effective_rate"] is not None:
            _check_tax_rate_yoy(year, tr_report["effective_rate"], prior_rate, issues)

        # E. NWC
        nwc_report = _derive_and_validate_nwc(year, yr, prior_yr, issues)
        if nwc_report["derived_nwc"] is not None:
            yr["derived_nwc"] = nwc_report["derived_nwc"]

        # F. OCF supplemental
        ocf_flags = _check_ocf_supplemental(year, yr, issues)

        year_reports[year] = {
            "gross_profit":   gp_report,
            "net_debt":       nd_report,
            "shares":         sh_report,
            "tax_rate":       tr_report,
            "nwc":            nwc_report,
            "ocf_flags":      ocf_flags,
        }

    # ── Multi-year checks ─────────────────────────────────────────────────────

    # C (continued): Shares staleness
    _check_shares_staleness(canonical_by_year, issues)

    # D (continued): Zero-tax check across years
    _check_zero_tax(canonical_by_year, issues)

    # G. Capex supplemental (multi-year)
    capex_flags = _check_capex_supplemental(canonical_by_year, classifier_template, issues)

    # H. EV Multiples rebuild
    rebuilt_multiples = _rebuild_ev_multiples(canonical_by_year, stats, issues)

    # ── Smooth tax rate and write back ────────────────────────────────────────
    smoothed_tax_rate = _smooth_tax_rate(effective_rates)
    if smoothed_tax_rate is not None:
        for yr_key in canonical_by_year:
            canonical_by_year[yr_key]["smoothed_tax_rate"] = smoothed_tax_rate

    # ── Summary stats ─────────────────────────────────────────────────────────
    n_crit = sum(1 for iss in issues if iss["severity"] == "critical")
    n_warn = sum(1 for iss in issues if iss["severity"] == "warning")
    n_info = sum(1 for iss in issues if iss["severity"] == "info")

    n_net_debt_years = sum(
        1 for yr in canonical_by_year.values()
        if yr.get("derived_net_debt") is not None
    )

    must_review = list({
        iss["field"]
        for iss in issues
        if iss["severity"] == "critical"
    })

    # Overall status
    if n_crit > 0:
        overall_status = "warn"       # non-blocking but flagged
    elif n_warn > 3:
        overall_status = "warn"
    else:
        overall_status = "pass"

    # ── Assemble output ───────────────────────────────────────────────────────
    out = dict(standardised)
    out["canonical_by_year"] = canonical_by_year
    out["high_risk_validation"] = {
        "status":          overall_status,
        "issues":          issues,
        "year_reports":    year_reports,
        "rebuilt_multiples": rebuilt_multiples,
        "summary": {
            "critical_count":          n_crit,
            "warning_count":           n_warn,
            "info_count":              n_info,
            "derived_net_debt_years":  n_net_debt_years,
            "smoothed_tax_rate":       smoothed_tax_rate,
            "must_review_fields":      must_review,
        },
    }
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Terminal report
# ─────────────────────────────────────────────────────────────────────────────

def print_high_risk_report(result: dict) -> None:
    """Print a structured summary to stdout."""
    hrv   = result.get("high_risk_validation", {})
    summ  = hrv.get("summary", {})
    years = sorted(hrv.get("year_reports", {}).keys(), reverse=True)
    mult  = hrv.get("rebuilt_multiples", {})

    print()
    print("=" * 72)
    print("  High-Risk Field Validation  —  Stage 2.6")
    print("=" * 72)

    if not years:
        print("  No years processed.\n")
        return

    print(
        f"  Years processed : {len(years)}  ({years[-1]}–{years[0]})\n"
        f"  Issues          : "
        f"{summ.get('critical_count', 0)} critical  "
        f"{summ.get('warning_count', 0)} warning  "
        f"{summ.get('info_count', 0)} info\n"
        f"  Status          : {hrv.get('status', '—').upper()}"
    )

    # Smoothed tax rate
    str_rate = summ.get("smoothed_tax_rate")
    if str_rate is not None:
        print(f"  Smoothed tax rate (3-yr median, plausible range) : {str_rate:.1%}")

    # Net debt derived
    nd_yrs = summ.get("derived_net_debt_years", 0)
    print(f"  Net debt derived : {nd_yrs}/{len(years)} year(s)")

    # Per-year table for key derived fields
    print()
    hdr = (
        f"  {'Year':<6} {'GP margin':>10} {'Gross status':>14} "
        f"{'Derived ND':>14} {'Tax rate':>10} {'Derived NWC':>14}"
    )
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))

    for year in years:
        rpt = hrv["year_reports"][year]

        gp_m = rpt["gross_profit"].get("gross_margin")
        gp_s = rpt["gross_profit"].get("status", "—")
        nd   = rpt["net_debt"].get("derived_net_debt")
        tr   = rpt["tax_rate"].get("effective_rate")
        nwc  = rpt["nwc"].get("derived_nwc")

        gp_m_str = f"{gp_m:.1%}" if gp_m is not None else "—"
        nd_str   = _fmt(nd)  if nd  is not None else "—"
        tr_str   = f"{tr:.1%}" if tr is not None else "—"
        nwc_str  = _fmt(nwc) if nwc is not None else "—"

        gp_flag = " ⚠" if gp_s in ("warn", "critical") else ""
        print(
            f"  {year:<6} {gp_m_str:>10}{gp_flag:2} {gp_s:>14} "
            f"{nd_str:>14} {tr_str:>10} {nwc_str:>14}"
        )

    # Rebuilt multiples
    if mult.get("rebuilt_ev") is not None:
        print()
        print("  Rebuilt EV & Multiples (from validated components):")
        print(f"    EV         : {_fmt(mult['rebuilt_ev'])}")
        if mult.get("ev_ebitda"):
            print(f"    EV/EBITDA  : {mult['ev_ebitda']:.1f}×")
        if mult.get("ev_ebit"):
            print(f"    EV/EBIT    : {mult['ev_ebit']:.1f}×")
        if mult.get("ev_revenue"):
            print(f"    EV/Revenue : {mult['ev_revenue']:.2f}×")
        if mult.get("pe"):
            print(f"    P/E        : {mult['pe']:.1f}×")

    # Issues
    issues = hrv.get("issues", [])
    critical = [i for i in issues if i["severity"] == "critical"]
    warnings = [i for i in issues if i["severity"] == "warning"]

    if critical:
        print(f"\n  Critical issues ({len(critical)}):")
        for iss in critical:
            yr = f"[{iss['year']}] " if iss.get("year") else ""
            print(f"    ✖  {iss['rule_id']}  {yr}{iss['field']}: {iss['message'][:80]}")

    if warnings:
        print(f"\n  Warnings ({len(warnings)}):")
        for iss in warnings:
            yr = f"[{iss['year']}] " if iss.get("year") else ""
            print(f"    ⚠  {iss['rule_id']}  {yr}{iss['field']}: {iss['message'][:80]}")

    must_rev = summ.get("must_review_fields", [])
    if must_rev:
        print(f"\n  Fields requiring review: {', '.join(must_rev)}")

    print(f"\n  Status: {hrv.get('status', '—').upper()}")
    print("=" * 72 + "\n")
