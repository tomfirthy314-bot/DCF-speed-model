"""
Anomaly Detector  —  Stage 5.5  (post-classifier, pre-assumption-engine)

Comprehensive data quality checker for scraped financial data.

Philosophy
----------
  - Sceptical by default: flag anything economically or mathematically unlikely
  - Optimise for catching suspicious data, accepting some false positives
  - Mature consumer staples (Unilever-style) are the "normal" benchmark —
    tighter thresholds are applied to consumer sector; industrial/resources
    thresholds are deliberately looser
  - Sector overrides loosen thresholds where appropriate

Output
------
  The result dict is stored at data["data_quality"] and also returned.

  {
    "status":         "pass" | "warn" | "fail",
    "quality_score":  0–100,
    "score_band":     "high confidence" | "usable with review" | ...,
    "overall_status": "pass" | "warn" | "critical",
    "issues":         [{ rule_id, field, year, severity, message, ... }],
    "summary":        { critical_count, warning_count, info_count,
                        must_recheck_fields }
  }

Pipeline gating
---------------
  "fail"  → score < 50  (do not rely on valuation)
  "warn"  → score 50–74 (low confidence — pipeline continues with flag)
  "pass"  → score ≥ 75
"""

from __future__ import annotations
from collections import defaultdict
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Sector thresholds
# ─────────────────────────────────────────────────────────────────────────────

_BASE_T: dict[str, float] = {
    # Revenue YoY — all movement thresholds loosened ~15% vs original
    "rev_yoy_warn":                 0.29,
    "rev_yoy_crit":                 0.46,
    "rev_spike_crit":               1.50,   # absolute +150% — keep hard ceiling
    "rev_drop_crit":                0.69,   # absolute -69%
    # EBITDA
    "ebitda_yoy_warn":              0.40,
    "ebitda_yoy_crit":              0.86,
    "ebitda_margin_max":            0.85,
    "ebitda_margin_move_crit":      0.12,   # 12pp in one yr
    # EBIT
    "ebit_yoy_warn":                0.46,
    "ebit_yoy_crit":                1.15,
    "ebit_margin_max":              0.69,
    "ebit_margin_move_crit":        0.09,   # 9pp in one yr
    # Gross margin
    "gm_move_warn":                 0.09,
    "gm_move_crit":                 0.17,
    # Net income
    "ni_yoy_warn":                  0.58,
    "ni_yoy_crit":                  1.50,   # keep — already very lenient
    "net_margin_move_warn":         0.12,
    # OCF
    "ocf_yoy_warn":                 0.46,
    "ocf_yoy_crit":                 0.92,
    "ocf_over_ebitda_warn":         1.60,   # OCF > 160% EBITDA
    # Capex
    "capex_pct_rev_warn":           0.12,
    "capex_pct_rev_crit":           0.23,
    "capex_yoy_warn":               0.86,
    "capex_pct_move_warn":          0.06,   # 6pp move in capex/rev
    # D&A
    "da_yoy_warn":                  0.46,
    "da_yoy_crit":                  1.15,
    "da_pct_move_warn":             0.035,
    # Working capital items
    "wc_item_yoy_warn":             0.40,
    "wc_item_yoy_crit":             0.86,
    "nwc_pct_move_warn":            0.09,   # 9pp move in nwc/rev
    # Cash
    "cash_yoy_warn":                0.46,
    "cash_yoy_crit":                1.15,
    # Debt
    "debt_yoy_warn":                0.46,
    "debt_yoy_crit":                1.15,
    # Shares
    "shares_yoy_warn":              0.06,
    "shares_yoy_crit":              0.17,
    "shares_large_change_crit":     0.29,
    # Spike detection (1 yr vs avg of neighbours)
    "spike_ratio":                  11.5,
    # Share-price implied check
    "price_implied_tol":            0.10,
    # WACC / DCF — model inputs, not data quality; keep unchanged
    "wacc_min":                     0.05,
    "wacc_max":                     0.15,
    "beta_min":                     0.30,
    "beta_max":                     2.50,
    "tgr_max_warn":                 0.035,
    "tgr_max_crit":                 0.045,
    "tv_pct_warn":                  0.75,
    "tv_pct_crit":                  0.85,
    "fcst_rev_step_max":            0.12,   # max pp change per yr
    "fcst_margin_expand_max":       0.035,  # 3.5pp/yr expansion
    "fcst_margin_over_peak_max":    0.06,   # max above hist peak
}

_SECTOR_OVERRIDES: dict[str, dict] = {
    "asset_light": {
        "ebit_margin_max":              0.80,
        "ebitda_margin_max":            0.90,
        "capex_pct_rev_warn":           0.05,
        "capex_pct_rev_crit":           0.08,
    },
    "utilities": {
        "capex_pct_rev_warn":           0.20,
        "capex_pct_rev_crit":           0.35,
        "ebit_margin_max":              0.40,
        "debt_yoy_crit":                1.50,
        "da_yoy_warn":                  0.60,
        "rev_yoy_warn":                 0.12,
        "rev_yoy_crit":                 0.25,
    },
    "resources": {
        "rev_yoy_warn":                 0.35,
        "rev_yoy_crit":                 0.60,
        "capex_pct_rev_warn":           0.18,
        "capex_pct_rev_crit":           0.35,
        "ebitda_yoy_crit":              1.00,
        "ebitda_margin_move_crit":      0.15,
    },
    "industrial": {
        "capex_pct_rev_warn":           0.12,
        "capex_pct_rev_crit":           0.22,
        "ebit_margin_max":              0.45,
    },
    "consumer": {
        # Tighter than base but still 15% looser than original
        "rev_yoy_warn":                 0.17,
        "rev_yoy_crit":                 0.35,
        "ebitda_yoy_warn":              0.29,
        "ebitda_yoy_crit":              0.58,
        "gm_move_warn":                 0.06,
        "gm_move_crit":                 0.12,
        "ebit_margin_move_crit":        0.07,
        "capex_pct_rev_crit":           0.14,
        "ni_yoy_warn":                  0.35,
        "ni_yoy_crit":                  0.92,
    },
}


def _thresholds(sector: str | None) -> dict:
    t = dict(_BASE_T)
    if sector and sector in _SECTOR_OVERRIDES:
        t.update(_SECTOR_OVERRIDES[sector])
    return t


# ─────────────────────────────────────────────────────────────────────────────
# Issue factory
# ─────────────────────────────────────────────────────────────────────────────

_ACTIONS = {
    "accept":   "accept",
    "verify":   "verify against second source",
    "rescrape": "re-scrape from different source",
    "exclude":  "exclude from model and use fallback assumption",
}

_CAUSES = {
    "scaling":   "scaling",
    "sign":      "sign",
    "mapping":   "mapping",
    "stale":     "stale data",
    "partial":   "partial period",
    "classif":   "classification",
    "plausible": "extreme but plausible business event",
    "identity":  "accounting identity violation",
    "pattern":   "time-series pattern anomaly",
}


def _iss(
    rule_id: str,
    field: str,
    year: str | None,
    severity: str,          # "critical" | "warning" | "info"
    message: str,
    observed: Any = None,
    expected: str | None = None,
    action: str = "verify",
    cause: str = "mapping",
) -> dict:
    return {
        "rule_id":            rule_id,
        "field":              field,
        "year":               year,
        "severity":           severity,
        "message":            message,
        "observed_value":     observed,
        "expected_range":     expected,
        "recommended_action": _ACTIONS.get(action, action),
        "likely_cause":       _CAUSES.get(cause, cause),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Value helpers
# ─────────────────────────────────────────────────────────────────────────────

def _f(d: dict, field: str) -> float | None:
    """Safe float from dict, returns None if missing or unconvertible."""
    if d is None:
        return None
    v = d.get(field)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pct_change(new: float | None, old: float | None) -> float | None:
    if new is None or old is None or old == 0:
        return None
    return (new - old) / abs(old)


def _margin(num: float | None, denom: float | None) -> float | None:
    if num is None or not denom:
        return None
    return num / denom


def _disp(val: Any) -> str:
    """Auto-scale to B/M for readable output."""
    if val is None:
        return "n/a"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if abs(v) >= 1_000_000_000:
        return f"{v / 1e9:.2f}B"
    if abs(v) >= 1_000_000:
        return f"{v / 1e6:.1f}M"
    return f"{v:,.0f}"


def _rel(v: float | None) -> str:
    if v is None:
        return "n/a"
    return f"{v:+.1%}"


# ─────────────────────────────────────────────────────────────────────────────
# A.  Accounting identity checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_accounting_identities(years: dict, issues: list, t: dict) -> None:
    for yr, d in years.items():
        rev     = _f(d, "revenue")
        ebit    = _f(d, "ebit")
        ebitda  = _f(d, "ebitda")
        da      = _f(d, "da")
        ocf     = _f(d, "operating_cash_flow")
        capex   = _f(d, "capex")
        fcf     = _f(d, "free_cash_flow")
        gp      = _f(d, "gross_profit")
        debt    = _f(d, "debt")
        lt_debt = _f(d, "long_term_debt")
        cash    = _f(d, "cash")
        assets  = _f(d, "total_assets")

        # Provenance flags: if a field was produced by canonical_fundamentals,
        # the accounting-identity checks that test that field are redundant —
        # the derivation already guarantees the identity (M1) or used a
        # superior multi-source method (M2/M3) that legitimately diverges.
        sources        = d.get("_sources", {})
        ebitda_derived = str(sources.get("ebitda", "")).startswith("derived:")
        fcf_derived    = str(sources.get("free_cash_flow", "")).startswith("derived:")

        # ACCT_001: EBITDA ≈ EBIT + D&A  (tolerance 10%)
        # Skipped when EBITDA is derived — by construction EBITDA = EBIT + D&A.
        # D&A is always treated as a positive magnitude regardless of how the
        # source stored it (some sources record it as a negative cash outflow).
        # Using abs(da) matches the convention used by canonical_fundamentals.
        if not ebitda_derived and ebitda is not None and ebit is not None and da is not None:
            expected_ebitda = ebit + abs(da)
            if expected_ebitda != 0:
                diff = abs(ebitda - expected_ebitda) / abs(expected_ebitda)
                if diff > 0.10:
                    issues.append(_iss(
                        "ACCT_001", "ebitda", yr, "critical",
                        f"EBITDA ({_disp(ebitda)}) diverges from EBIT+D&A "
                        f"({_disp(expected_ebitda)}) by {diff:.0%}. "
                        "Likely field-mapping or partial-period error.",
                        observed=_disp(ebitda),
                        expected=f"≈ {_disp(expected_ebitda)}",
                        action="rescrape", cause="identity",
                    ))

        # ACCT_002: FCF ≈ OCF - Capex  (tolerance 15%)
        # Skipped when FCF is derived — M1 derivation uses this formula exactly
        # (result matches by construction), and M2/M3 derivations legitimately
        # diverge from OCF - Capex by design (they account for NWC changes and
        # taxes directly rather than trusting the reported OCF figure).
        # Capex sign convention varies by source: Yahoo stores it as a positive
        # purchase amount; cash flow statements store it as a negative outflow.
        if not fcf_derived and fcf is not None and ocf is not None and capex is not None:
            expected_fcf = ocf - abs(capex)
            if expected_fcf != 0:
                diff = abs(fcf - expected_fcf) / abs(expected_fcf)
                if diff > 0.15:
                    issues.append(_iss(
                        "ACCT_002", "free_cash_flow", yr, "warning",
                        f"FCF ({_disp(fcf)}) diverges from OCF minus Capex "
                        f"({_disp(expected_fcf)}) by {diff:.0%}. "
                        "May reflect working capital items, lease payments, or financing.",
                        observed=_disp(fcf),
                        expected=f"≈ {_disp(expected_fcf)}",
                        action="verify", cause="identity",
                    ))

        # ACCT_003: Gross Profit > Revenue
        if gp is not None and rev is not None and rev > 0 and gp > rev:
            issues.append(_iss(
                "ACCT_003", "gross_profit", yr, "critical",
                f"Gross Profit ({_disp(gp)}) exceeds Revenue ({_disp(rev)}). "
                "Impossible — almost certainly a field-mapping error.",
                observed=_disp(gp), expected=f"< {_disp(rev)}",
                action="rescrape", cause="mapping",
            ))

        # ACCT_004: Gross Profit = Revenue exactly  (mapping collision)
        if gp is not None and rev is not None and rev > 0:
            if abs(gp - rev) / rev < 0.001:
                issues.append(_iss(
                    "ACCT_004", "gross_profit", yr, "critical",
                    f"Gross Profit equals Revenue ({_disp(rev)}) exactly. "
                    "Classic field-mapping error — Revenue likely scraped into Gross Profit slot.",
                    observed=_disp(gp),
                    action="rescrape", cause="mapping",
                ))

        # ACCT_005: EBIT = Revenue exactly
        if ebit is not None and rev is not None and rev > 0:
            if abs(ebit - rev) / rev < 0.001:
                issues.append(_iss(
                    "ACCT_005", "ebit", yr, "critical",
                    f"EBIT equals Revenue ({_disp(rev)}) exactly. "
                    "100% EBIT margin is impossible — field-mapping error.",
                    observed=_disp(ebit),
                    action="rescrape", cause="mapping",
                ))

        # ACCT_006: EBITDA = Revenue exactly
        if ebitda is not None and rev is not None and rev > 0:
            if abs(ebitda - rev) / rev < 0.001:
                issues.append(_iss(
                    "ACCT_006", "ebitda", yr, "critical",
                    f"EBITDA equals Revenue ({_disp(rev)}) exactly — likely mapping error.",
                    observed=_disp(ebitda),
                    action="rescrape", cause="mapping",
                ))

        # ACCT_007: EBITDA < EBIT materially (impossible by definition)
        # Skipped when EBITDA is derived — derived EBITDA = EBIT + D&A ≥ EBIT always.
        if not ebitda_derived and ebitda is not None and ebit is not None and ebit > 0:
            if ebitda < ebit * 0.90:
                issues.append(_iss(
                    "ACCT_007", "ebitda", yr, "critical",
                    f"EBITDA ({_disp(ebitda)}) is materially below EBIT ({_disp(ebit)}). "
                    "Impossible by definition — scrape or mapping error.",
                    observed=_disp(ebitda), expected=f"≥ {_disp(ebit)}",
                    action="rescrape", cause="mapping",
                ))

        # ACCT_008: Cash > Total Assets
        if cash is not None and assets is not None and assets > 0 and cash > assets:
            issues.append(_iss(
                "ACCT_008", "cash", yr, "critical",
                f"Cash ({_disp(cash)}) exceeds Total Assets ({_disp(assets)}). "
                "Mathematically impossible — likely a unit or scaling error.",
                observed=_disp(cash), expected=f"< {_disp(assets)}",
                action="rescrape", cause="scaling",
            ))

        # ACCT_009: Total Debt < Long-term Debt
        if debt is not None and lt_debt is not None and lt_debt > 0:
            if debt < lt_debt * 0.95:
                issues.append(_iss(
                    "ACCT_009", "debt", yr, "critical",
                    f"Total Debt ({_disp(debt)}) < Long-term Debt ({_disp(lt_debt)}). "
                    "Total must include short-term debt — likely mapping error.",
                    observed=_disp(debt), expected=f"≥ {_disp(lt_debt)}",
                    action="rescrape", cause="mapping",
                ))

        # ACCT_010: FCF > Revenue
        if fcf is not None and rev is not None and rev > 0 and fcf > rev:
            issues.append(_iss(
                "ACCT_010", "free_cash_flow", yr, "critical",
                f"FCF ({_disp(fcf)}) exceeds Revenue ({_disp(rev)}). "
                "Economically impossible for a normal operating business.",
                observed=_disp(fcf), expected=f"< {_disp(rev)}",
                action="rescrape", cause="scaling",
            ))

        # ACCT_011: Revenue negative
        if rev is not None and rev < 0:
            issues.append(_iss(
                "ACCT_011", "revenue", yr, "critical",
                f"Revenue is negative ({_disp(rev)}). Sign or mapping error.",
                observed=_disp(rev), action="rescrape", cause="sign",
            ))

        # ACCT_012: Cash negative
        if cash is not None and cash < 0:
            issues.append(_iss(
                "ACCT_012", "cash", yr, "critical",
                f"Cash is negative ({_disp(cash)}). Sign error.",
                observed=_disp(cash), action="rescrape", cause="sign",
            ))

        # ACCT_013: Debt negative
        if debt is not None and debt < 0:
            issues.append(_iss(
                "ACCT_013", "debt", yr, "critical",
                f"Total Debt is negative ({_disp(debt)}). Sign error.",
                observed=_disp(debt), action="rescrape", cause="sign",
            ))


# ─────────────────────────────────────────────────────────────────────────────
# B.  Scaling and sign checks  (also checks stats-level fields once)
# ─────────────────────────────────────────────────────────────────────────────

def _check_scaling_signs(years: dict, stats: dict, issues: list, t: dict) -> None:

    # ── Stats-level (once, not per year) ─────────────────────────────────────
    mc     = stats.get("market_cap")
    price  = stats.get("current_price")
    shares = stats.get("shares_outstanding")

    # SIGN_010: Market cap negative
    if mc is not None and mc < 0:
        issues.append(_iss(
            "SIGN_010", "market_cap", None, "critical",
            f"Market cap is negative ({_disp(mc)}). Sign or data error.",
            observed=_disp(mc), action="rescrape", cause="sign",
        ))

    # SCAL_010: Implied price vs scraped price
    if mc and shares and price and shares > 0 and price > 0 and mc > 0:
        implied = mc / shares
        tol = t["price_implied_tol"]
        if abs(implied - price) / price > tol:
            issues.append(_iss(
                "SCAL_010", "shares_outstanding", None, "critical",
                f"Implied price (Market Cap ÷ Shares) = {implied:.2f} vs "
                f"scraped price {price:.2f}. Divergence {abs(implied - price)/price:.0%}. "
                "Likely a unit error in shares (thousands vs millions vs actual).",
                observed=f"{implied:.2f}",
                expected=f"≈ {price:.2f}",
                action="rescrape", cause="scaling",
            ))

    # ── Per-year checks ───────────────────────────────────────────────────────
    for yr, d in years.items():
        rev    = _f(d, "revenue")
        gp     = _f(d, "gross_profit")
        ebit   = _f(d, "ebit")
        ebitda = _f(d, "ebitda")
        capex  = _f(d, "capex")
        da     = _f(d, "da")

        # SIGN_001: Capex positive (should be negative cash outflow)
        if capex is not None and capex > 0:
            issues.append(_iss(
                "SIGN_001", "capex", yr, "warning",
                f"Capex is positive ({_disp(capex)}). Expected negative (cash outflow). "
                "Sign convention may be inverted.",
                observed=_disp(capex), action="verify", cause="sign",
            ))

        # SIGN_002: D&A negative (should be positive non-cash charge)
        if da is not None and da < 0:
            issues.append(_iss(
                "SIGN_002", "da", yr, "critical",
                f"D&A is negative ({_disp(da)}). Expected positive non-cash charge. "
                "Sign error.",
                observed=_disp(da), action="rescrape", cause="sign",
            ))

        # SCAL_001: Gross margin > 100%
        if gp is not None and rev is not None and rev > 0:
            gm = gp / rev
            if gm >= 1.0:
                issues.append(_iss(
                    "SCAL_001", "gross_profit", yr, "critical",
                    f"Gross margin is {gm:.0%} — cannot exceed 100%. Scaling or sign error.",
                    observed=f"{gm:.0%}", expected="< 100%",
                    action="rescrape", cause="scaling",
                ))
            elif gm < -0.20:
                issues.append(_iss(
                    "SCAL_002", "gross_profit", yr, "warning",
                    f"Gross margin is {gm:.0%} — extremely negative. "
                    "Plausible in a distressed year but verify.",
                    observed=f"{gm:.0%}", action="verify", cause="plausible",
                ))

        # SCAL_003: EBIT margin > sector max
        if ebit is not None and rev is not None and rev > 0:
            em = ebit / rev
            if em > t["ebit_margin_max"]:
                issues.append(_iss(
                    "SCAL_003", "ebit", yr, "critical",
                    f"EBIT margin is {em:.0%}, exceeds sector maximum of "
                    f"{t['ebit_margin_max']:.0%}. Likely mapping or scaling error.",
                    observed=f"{em:.0%}", expected=f"< {t['ebit_margin_max']:.0%}",
                    action="rescrape", cause="scaling",
                ))

        # SCAL_004: EBITDA margin > sector max
        if ebitda is not None and rev is not None and rev > 0:
            em = ebitda / rev
            if em > t["ebitda_margin_max"]:
                issues.append(_iss(
                    "SCAL_004", "ebitda", yr, "critical",
                    f"EBITDA margin is {em:.0%}, exceeds sector max of "
                    f"{t['ebitda_margin_max']:.0%}.",
                    observed=f"{em:.0%}", expected=f"< {t['ebitda_margin_max']:.0%}",
                    action="rescrape", cause="scaling",
                ))

        # SCAL_005: D&A = 0 for operating company with material revenue
        if da is not None and da == 0 and rev is not None and rev > 100_000_000:
            issues.append(_iss(
                "SCAL_005", "da", yr, "warning",
                f"D&A is exactly zero for a company with {_disp(rev)} revenue. "
                "Unlikely for a material operating business — possible missing field.",
                observed="0", action="verify", cause="mapping",
            ))

        # SCAL_006: D&A > Revenue (impossible)
        if da is not None and rev is not None and rev > 0 and da > rev:
            issues.append(_iss(
                "SCAL_006", "da", yr, "critical",
                f"D&A ({_disp(da)}) exceeds Revenue ({_disp(rev)}). Scaling error.",
                observed=_disp(da), expected=f"< {_disp(rev)}",
                action="rescrape", cause="scaling",
            ))


# ─────────────────────────────────────────────────────────────────────────────
# C.  Historical YoY movement checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_historical_movements(years: dict, issues: list, t: dict) -> None:
    sorted_yrs = sorted(years.keys())
    pairs = list(zip(sorted_yrs[:-1], sorted_yrs[1:]))  # (prior_yr, curr_yr)

    for prior_yr, curr_yr in pairs:
        p = years[prior_yr]
        c = years[curr_yr]

        def _yoy(field: str) -> float | None:
            return _pct_change(_f(c, field), _f(p, field))

        def _marg(d: dict, num: str, denom: str = "revenue") -> float | None:
            return _margin(_f(d, num), _f(d, denom))

        def _margin_move(num: str, denom: str = "revenue") -> float | None:
            pm = _marg(p, num, denom)
            cm = _marg(c, num, denom)
            return (cm - pm) if (pm is not None and cm is not None) else None

        yr_tag = f"{prior_yr}→{curr_yr}"

        # ── Revenue ──────────────────────────────────────────────────────────
        rev_chg = _yoy("revenue")
        if rev_chg is not None:
            if rev_chg > t["rev_spike_crit"]:
                issues.append(_iss(
                    "HIST_001", "revenue", curr_yr, "critical",
                    f"Revenue jumped {_rel(rev_chg)} ({yr_tag}). "
                    "Exceeds +150% critical threshold — possible unit or period mismatch.",
                    observed=_rel(rev_chg), expected=f"< +{t['rev_spike_crit']:.0%}",
                    action="rescrape", cause="scaling",
                ))
            elif rev_chg < -t["rev_drop_crit"]:
                issues.append(_iss(
                    "HIST_002", "revenue", curr_yr, "critical",
                    f"Revenue fell {_rel(rev_chg)} ({yr_tag}). "
                    "Exceeds -60% critical threshold — verify period or unit.",
                    observed=_rel(rev_chg), expected=f"> -{t['rev_drop_crit']:.0%}",
                    action="verify", cause="plausible",
                ))
            elif abs(rev_chg) > t["rev_yoy_crit"]:
                issues.append(_iss(
                    "HIST_003", "revenue", curr_yr, "critical",
                    f"Revenue changed {_rel(rev_chg)} ({yr_tag}). "
                    f"Exceeds {t['rev_yoy_crit']:.0%} critical threshold.",
                    observed=_rel(rev_chg), action="verify", cause="plausible",
                ))
            elif abs(rev_chg) > t["rev_yoy_warn"]:
                issues.append(_iss(
                    "HIST_004", "revenue", curr_yr, "warning",
                    f"Revenue changed {_rel(rev_chg)} ({yr_tag}). "
                    f"Exceeds {t['rev_yoy_warn']:.0%} warning threshold.",
                    observed=_rel(rev_chg), action="accept", cause="plausible",
                ))

        # ── EBITDA ───────────────────────────────────────────────────────────
        ebitda_chg = _yoy("ebitda")
        ebitda_mm  = _margin_move("ebitda")
        rev_c = _f(c, "revenue")
        rev_p = _f(p, "revenue")

        if ebitda_chg is not None:
            if abs(ebitda_chg) > t["ebitda_yoy_crit"]:
                issues.append(_iss(
                    "HIST_010", "ebitda", curr_yr, "critical",
                    f"EBITDA changed {_rel(ebitda_chg)} ({yr_tag}). "
                    f"Exceeds {t['ebitda_yoy_crit']:.0%} critical threshold.",
                    observed=_rel(ebitda_chg), action="verify", cause="plausible",
                ))
            elif abs(ebitda_chg) > t["ebitda_yoy_warn"]:
                issues.append(_iss(
                    "HIST_011", "ebitda", curr_yr, "warning",
                    f"EBITDA changed {_rel(ebitda_chg)} ({yr_tag}).",
                    observed=_rel(ebitda_chg), action="accept", cause="plausible",
                ))

        # EBITDA doubles while revenue flat — classic fishy signal
        ebitda_c = _f(c, "ebitda")
        ebitda_p = _f(p, "ebitda")
        if (ebitda_c and ebitda_p and rev_c and rev_p and
                ebitda_p > 0 and rev_p > 0):
            ebitda_ratio = ebitda_c / ebitda_p
            rev_ratio    = rev_c / rev_p
            if ebitda_ratio >= 1.90 and abs(rev_ratio - 1.0) < 0.10:
                issues.append(_iss(
                    "HIST_012", "ebitda", curr_yr, "critical",
                    f"EBITDA approximately doubled ({ebitda_ratio:.1f}x) while revenue "
                    f"was nearly flat ({rev_ratio:.2f}x) ({yr_tag}). "
                    "Classic suspicious pattern — verify against source.",
                    observed=f"EBITDA ×{ebitda_ratio:.1f}, Rev ×{rev_ratio:.2f}",
                    action="rescrape", cause="pattern",
                ))

        if ebitda_mm is not None and abs(ebitda_mm) > t["ebitda_margin_move_crit"]:
            issues.append(_iss(
                "HIST_013", "ebitda", curr_yr, "critical",
                f"EBITDA margin moved {_rel(ebitda_mm)} in one year ({yr_tag}). "
                f"Exceeds {t['ebitda_margin_move_crit']:.0%} threshold.",
                observed=_rel(ebitda_mm), action="verify", cause="plausible",
            ))

        # ── EBIT ─────────────────────────────────────────────────────────────
        ebit_chg = _yoy("ebit")
        ebit_mm  = _margin_move("ebit")

        if ebit_chg is not None:
            if abs(ebit_chg) > t["ebit_yoy_crit"]:
                issues.append(_iss(
                    "HIST_020", "ebit", curr_yr, "critical",
                    f"EBIT changed {_rel(ebit_chg)} ({yr_tag}). "
                    f"Exceeds {t['ebit_yoy_crit']:.0%} threshold.",
                    observed=_rel(ebit_chg), action="verify", cause="plausible",
                ))
            elif abs(ebit_chg) > t["ebit_yoy_warn"]:
                issues.append(_iss(
                    "HIST_021", "ebit", curr_yr, "warning",
                    f"EBIT changed {_rel(ebit_chg)} ({yr_tag}).",
                    observed=_rel(ebit_chg), action="accept", cause="plausible",
                ))

        if ebit_mm is not None and abs(ebit_mm) > t["ebit_margin_move_crit"]:
            issues.append(_iss(
                "HIST_022", "ebit", curr_yr, "critical",
                f"EBIT margin moved {_rel(ebit_mm)} in one year ({yr_tag}). "
                f"Exceeds {t['ebit_margin_move_crit']:.0%} threshold.",
                observed=_rel(ebit_mm), action="verify", cause="plausible",
            ))

        # ── Gross margin ──────────────────────────────────────────────────────
        gm_move = _margin_move("gross_profit")
        if gm_move is not None:
            if abs(gm_move) > t["gm_move_crit"]:
                issues.append(_iss(
                    "HIST_030", "gross_profit", curr_yr, "critical",
                    f"Gross margin moved {_rel(gm_move)} ({yr_tag}). "
                    f"Exceeds {t['gm_move_crit']:.0%} critical threshold.",
                    observed=_rel(gm_move), action="verify", cause="plausible",
                ))
            elif abs(gm_move) > t["gm_move_warn"]:
                issues.append(_iss(
                    "HIST_031", "gross_profit", curr_yr, "warning",
                    f"Gross margin moved {_rel(gm_move)} ({yr_tag}).",
                    observed=_rel(gm_move), action="accept", cause="plausible",
                ))

        # ── Net income ────────────────────────────────────────────────────────
        ni_chg = _yoy("net_income")
        ni_mm  = _margin_move("net_income")

        if ni_chg is not None:
            if abs(ni_chg) > t["ni_yoy_crit"]:
                issues.append(_iss(
                    "HIST_040", "net_income", curr_yr, "critical",
                    f"Net income changed {_rel(ni_chg)} ({yr_tag}). "
                    f"Exceeds {t['ni_yoy_crit']:.0%} threshold.",
                    observed=_rel(ni_chg), action="verify", cause="plausible",
                ))
            elif abs(ni_chg) > t["ni_yoy_warn"]:
                issues.append(_iss(
                    "HIST_041", "net_income", curr_yr, "warning",
                    f"Net income changed {_rel(ni_chg)} ({yr_tag}).",
                    observed=_rel(ni_chg), action="accept", cause="plausible",
                ))

        if ni_mm is not None and abs(ni_mm) > t["net_margin_move_warn"]:
            issues.append(_iss(
                "HIST_042", "net_income", curr_yr, "warning",
                f"Net margin moved {_rel(ni_mm)} ({yr_tag}).",
                observed=_rel(ni_mm), action="accept", cause="plausible",
            ))

        # ── Operating Cash Flow ───────────────────────────────────────────────
        ocf_chg = _yoy("operating_cash_flow")
        if ocf_chg is not None:
            if abs(ocf_chg) > t["ocf_yoy_crit"]:
                issues.append(_iss(
                    "HIST_050", "operating_cash_flow", curr_yr, "critical",
                    f"OCF changed {_rel(ocf_chg)} ({yr_tag}). "
                    f"Exceeds {t['ocf_yoy_crit']:.0%} threshold.",
                    observed=_rel(ocf_chg), action="verify", cause="plausible",
                ))
            elif abs(ocf_chg) > t["ocf_yoy_warn"]:
                issues.append(_iss(
                    "HIST_051", "operating_cash_flow", curr_yr, "warning",
                    f"OCF changed {_rel(ocf_chg)} ({yr_tag}).",
                    observed=_rel(ocf_chg), action="accept", cause="plausible",
                ))

        # OCF persistently far above EBITDA
        ocf_c    = _f(c, "operating_cash_flow")
        ebitda_c = _f(c, "ebitda")
        if ocf_c is not None and ebitda_c is not None and ebitda_c > 0:
            if ocf_c > ebitda_c * t["ocf_over_ebitda_warn"]:
                issues.append(_iss(
                    "HIST_052", "operating_cash_flow", curr_yr, "warning",
                    f"OCF ({_disp(ocf_c)}) is more than "
                    f"{t['ocf_over_ebitda_warn']:.0%}× EBITDA ({_disp(ebitda_c)}). "
                    "May reflect working capital unwind or non-recurring items.",
                    observed=_disp(ocf_c), expected=f"≤ {_disp(ebitda_c * t['ocf_over_ebitda_warn'])}",
                    action="verify", cause="plausible",
                ))

        # ── Capex ────────────────────────────────────────────────────────────
        capex_chg = _yoy("capex")
        if capex_chg is not None and abs(capex_chg) > t["capex_yoy_warn"]:
            issues.append(_iss(
                "HIST_060", "capex", curr_yr, "warning",
                f"Capex changed {_rel(capex_chg)} ({yr_tag}).",
                observed=_rel(capex_chg), action="verify", cause="plausible",
            ))

        # Capex flips sign (positive → negative or vice versa)
        capex_p_v = _f(p, "capex")
        capex_c_v = _f(c, "capex")
        if capex_p_v is not None and capex_c_v is not None:
            if (capex_p_v < 0 and capex_c_v > 0) or (capex_p_v > 0 and capex_c_v < 0):
                issues.append(_iss(
                    "HIST_061", "capex", curr_yr, "critical",
                    f"Capex flipped sign: {_disp(capex_p_v)} → {_disp(capex_c_v)} ({yr_tag}). "
                    "Sign convention inconsistency between years.",
                    observed=f"{_disp(capex_p_v)} → {_disp(capex_c_v)}",
                    action="rescrape", cause="sign",
                ))

        # Capex as % revenue — absolute level and movement
        rev_c = _f(c, "revenue")
        if capex_c_v is not None and rev_c and rev_c > 0:
            cap_pct = abs(capex_c_v) / rev_c
            if cap_pct > t["capex_pct_rev_crit"]:
                issues.append(_iss(
                    "HIST_062", "capex", curr_yr, "critical",
                    f"Capex is {cap_pct:.1%} of revenue — exceeds "
                    f"{t['capex_pct_rev_crit']:.0%} sector threshold.",
                    observed=f"{cap_pct:.1%}", action="verify", cause="plausible",
                ))
            elif cap_pct > t["capex_pct_rev_warn"]:
                issues.append(_iss(
                    "HIST_063", "capex", curr_yr, "warning",
                    f"Capex is {cap_pct:.1%} of revenue.",
                    observed=f"{cap_pct:.1%}", action="accept", cause="plausible",
                ))

        rev_p = _f(p, "revenue")
        if capex_p_v and rev_p and rev_c and rev_c > 0 and rev_p > 0:
            cap_pct_p = abs(capex_p_v) / rev_p
            cap_pct_c = abs(capex_c_v or 0) / rev_c
            if abs(cap_pct_c - cap_pct_p) > t["capex_pct_move_warn"]:
                issues.append(_iss(
                    "HIST_064", "capex", curr_yr, "warning",
                    f"Capex as % revenue moved {cap_pct_c - cap_pct_p:+.1%} ({yr_tag}).",
                    observed=f"{cap_pct_c - cap_pct_p:+.1%}", action="accept", cause="plausible",
                ))

        # ── D&A ──────────────────────────────────────────────────────────────
        da_chg = _yoy("da")
        da_mm  = _margin_move("da")

        if da_chg is not None:
            if abs(da_chg) > t["da_yoy_crit"]:
                issues.append(_iss(
                    "HIST_070", "da", curr_yr, "critical",
                    f"D&A changed {_rel(da_chg)} ({yr_tag}). "
                    "Unusual for a stable company.",
                    observed=_rel(da_chg), action="verify", cause="plausible",
                ))
            elif abs(da_chg) > t["da_yoy_warn"]:
                issues.append(_iss(
                    "HIST_071", "da", curr_yr, "warning",
                    f"D&A changed {_rel(da_chg)} ({yr_tag}).",
                    observed=_rel(da_chg), action="accept", cause="plausible",
                ))

        if da_mm is not None and abs(da_mm) > t["da_pct_move_warn"]:
            issues.append(_iss(
                "HIST_072", "da", curr_yr, "warning",
                f"D&A as % revenue moved {_rel(da_mm)} ({yr_tag}).",
                observed=_rel(da_mm), action="accept", cause="plausible",
            ))

        # ── Working capital items ─────────────────────────────────────────────
        for wc_f, label, rule_base in [
            ("accounts_receivable", "Accounts Receivable", "HIST_080"),
            ("accounts_payable",    "Accounts Payable",    "HIST_081"),
            ("inventory",           "Inventory",           "HIST_082"),
        ]:
            wc_chg = _yoy(wc_f)
            if wc_chg is None:
                continue
            if abs(wc_chg) > t["wc_item_yoy_crit"]:
                issues.append(_iss(
                    rule_base, wc_f, curr_yr, "critical",
                    f"{label} changed {_rel(wc_chg)} ({yr_tag}). "
                    f"Exceeds {t['wc_item_yoy_crit']:.0%} critical threshold.",
                    observed=_rel(wc_chg), action="verify", cause="plausible",
                ))
            elif abs(wc_chg) > t["wc_item_yoy_warn"]:
                issues.append(_iss(
                    rule_base + "w", wc_f, curr_yr, "warning",
                    f"{label} changed {_rel(wc_chg)} ({yr_tag}).",
                    observed=_rel(wc_chg), action="accept", cause="plausible",
                ))

        # ── Cash and Debt ─────────────────────────────────────────────────────
        for cf_f, label, w, k, rule_id in [
            ("cash", "Cash", t["cash_yoy_warn"], t["cash_yoy_crit"], "HIST_090"),
            ("debt", "Debt", t["debt_yoy_warn"], t["debt_yoy_crit"], "HIST_091"),
        ]:
            chg = _yoy(cf_f)
            if chg is None:
                continue
            if abs(chg) > k:
                issues.append(_iss(
                    rule_id, cf_f, curr_yr, "critical",
                    f"{label} changed {_rel(chg)} ({yr_tag}). "
                    f"Exceeds {k:.0%} threshold.",
                    observed=_rel(chg), action="verify", cause="plausible",
                ))
            elif abs(chg) > w:
                issues.append(_iss(
                    rule_id + "w", cf_f, curr_yr, "warning",
                    f"{label} changed {_rel(chg)} ({yr_tag}).",
                    observed=_rel(chg), action="accept", cause="plausible",
                ))

        # Debt disappears to near-zero from material level
        debt_p_v = _f(p, "debt")
        debt_c_v = _f(c, "debt")
        if debt_p_v is not None and debt_c_v is not None:
            if debt_p_v > 100_000_000 and debt_c_v < 1_000_000:
                issues.append(_iss(
                    "HIST_092", "debt", curr_yr, "warning",
                    f"Debt fell from {_disp(debt_p_v)} to near-zero in {curr_yr}. "
                    "Sudden elimination is unusual — verify against balance sheet.",
                    observed=_disp(debt_c_v), action="verify", cause="stale",
                ))

        # ── Shares outstanding ────────────────────────────────────────────────
        shares_chg = _yoy("shares_outstanding")
        if shares_chg is not None:
            if abs(shares_chg) > t["shares_large_change_crit"]:
                issues.append(_iss(
                    "HIST_100", "shares_outstanding", curr_yr, "critical",
                    f"Shares outstanding changed {_rel(shares_chg)} ({yr_tag}). "
                    "Exceeds 25% — major dilutive event or scrape error.",
                    observed=_rel(shares_chg), action="verify", cause="plausible",
                ))
            elif abs(shares_chg) > t["shares_yoy_crit"]:
                issues.append(_iss(
                    "HIST_101", "shares_outstanding", curr_yr, "critical",
                    f"Shares outstanding changed {_rel(shares_chg)} ({yr_tag}). "
                    f"Exceeds {t['shares_yoy_crit']:.0%} threshold.",
                    observed=_rel(shares_chg), action="verify", cause="plausible",
                ))
            elif abs(shares_chg) > t["shares_yoy_warn"]:
                issues.append(_iss(
                    "HIST_102", "shares_outstanding", curr_yr, "warning",
                    f"Shares changed {_rel(shares_chg)} ({yr_tag}). "
                    "Check for buybacks or issuances.",
                    observed=_rel(shares_chg), action="accept", cause="plausible",
                ))

    # ── Multi-year checks (after loop) ────────────────────────────────────────

    # Tax expense = 0 for multiple profitable years
    zero_tax_yrs = []
    for yr in sorted_yrs:
        d = years[yr]
        tax  = _f(d, "tax_provision")
        ebit = _f(d, "ebit")
        if tax is not None and ebit is not None and tax == 0 and ebit > 0:
            zero_tax_yrs.append(yr)
    if len(zero_tax_yrs) >= 2:
        issues.append(_iss(
            "HIST_110", "tax_provision", None, "warning",
            f"Tax expense is zero in {len(zero_tax_yrs)} profitable years "
            f"({', '.join(zero_tax_yrs)}). Likely missing or stale data.",
            observed="0 (multiple years)", action="verify", cause="stale",
        ))

    # Capex = 0 for multiple years in operating business
    sorted_yrs = sorted(years.keys())
    zero_capex_yrs = []
    for yr in sorted_yrs:
        capex = _f(years[yr], "capex")
        rev   = _f(years[yr], "revenue")
        if capex is not None and capex == 0 and rev and rev > 1_000_000:
            zero_capex_yrs.append(yr)
    if len(zero_capex_yrs) >= 2:
        issues.append(_iss(
            "HIST_111", "capex", None, "critical",
            f"Capex is exactly zero in {len(zero_capex_yrs)} years "
            f"({', '.join(zero_capex_yrs)}) for a company with material revenue. "
            "Unlikely — likely missing field.",
            observed="0 (multiple years)", action="rescrape", cause="mapping",
        ))


# ─────────────────────────────────────────────────────────────────────────────
# D.  Time-series pattern checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_timeseries_patterns(years: dict, issues: list, t: dict) -> None:
    sorted_yrs = sorted(years.keys())

    key_fields = [
        "revenue", "gross_profit", "ebit", "ebitda", "net_income",
        "operating_cash_flow", "capex", "free_cash_flow", "da",
        "debt", "cash", "shares_outstanding",
    ]

    for field in key_fields:
        vals     = [(yr, _f(years[yr], field)) for yr in sorted_yrs]
        non_null = [(yr, v) for yr, v in vals if v is not None]

        if len(non_null) < 2:
            continue

        raw_vals = [v for _, v in non_null]

        # PATT_001: Identical value across all available years
        if len(raw_vals) >= 3 and len(set(raw_vals)) == 1:
            issues.append(_iss(
                "PATT_001", field, None, "warning",
                f"{field}: identical value ({_disp(raw_vals[0])}) across all "
                f"{len(raw_vals)} years. Almost certainly backfilled/stale data.",
                observed=_disp(raw_vals[0]),
                action="rescrape", cause="stale",
            ))

        # PATT_002: Last two years identical while earlier values differed
        elif len(raw_vals) >= 4:
            if raw_vals[-1] == raw_vals[-2] and raw_vals[-2] != raw_vals[-3]:
                issues.append(_iss(
                    "PATT_002", field, sorted_yrs[-1], "info",
                    f"{field}: last two years have identical value "
                    f"({_disp(raw_vals[-1])}). May be a backfill — "
                    "verify recency of scrape.",
                    observed=_disp(raw_vals[-1]),
                    action="verify", cause="stale",
                ))

        # PATT_003: Single-year spike — value is spike_ratio× the avg of neighbours
        if len(non_null) >= 3:
            ratio = t["spike_ratio"]
            for idx, (yr, v) in enumerate(non_null):
                if v is None or v == 0:
                    continue
                neighbours = [
                    abs(nv) for nyr, nv in non_null
                    if nyr != yr and nv is not None and nv != 0
                ]
                if not neighbours:
                    continue
                avg_n = sum(neighbours) / len(neighbours)
                if avg_n > 0 and abs(v) / avg_n > ratio:
                    issues.append(_iss(
                        "PATT_003", field, yr, "critical",
                        f"{field} in {yr} ({_disp(v)}) is "
                        f"{abs(v) / avg_n:.0f}× the average of surrounding years "
                        f"({_disp(avg_n)}). Classic unit or period mismatch.",
                        observed=_disp(v),
                        expected=f"≈ {_disp(avg_n)}",
                        action="rescrape", cause="scaling",
                    ))

        # PATT_004: Value only in latest year, blanks historically
        if len(vals) >= 3:
            yr_latest, v_latest = vals[-1]
            prior_all_none = all(v is None for _, v in vals[:-1])
            if v_latest is not None and prior_all_none:
                issues.append(_iss(
                    "PATT_004", field, yr_latest, "warning",
                    f"{field} only appears in {yr_latest} with no historical data. "
                    "May be a new disclosure or partial period.",
                    action="verify", cause="partial",
                ))

    # PATT_010: Chronological gaps in years
    int_yrs = []
    for yr in sorted_yrs:
        try:
            int_yrs.append(int(yr))
        except ValueError:
            pass
    if int_yrs:
        expected = list(range(int_yrs[0], int_yrs[-1] + 1))
        missing  = [y for y in expected if y not in int_yrs]
        if missing:
            issues.append(_iss(
                "PATT_010", "years", None, "info",
                f"Missing fiscal year(s) in dataset: {missing}. "
                "May affect trend and CAGR calculations.",
                observed=str(missing), action="verify", cause="partial",
            ))

    # PATT_011: Shares unchanged across 5+ years (backfill signal)
    shares_vals = [_f(years[yr], "shares_outstanding") for yr in sorted_yrs]
    non_null_s  = [v for v in shares_vals if v is not None]
    if len(non_null_s) >= 5 and len(set(non_null_s)) == 1:
        issues.append(_iss(
            "PATT_011", "shares_outstanding", None, "warning",
            f"Shares outstanding are identical across all {len(non_null_s)} years "
            f"({_disp(non_null_s[0])}). Backfilled from current snapshot — "
            "historical share counts may differ materially.",
            observed=_disp(non_null_s[0]),
            action="verify", cause="stale",
        ))

    # PATT_012: Row duplication across fields in same year
    # Flag if revenue = EBIT = EBITDA = gross_profit simultaneously
    for yr in sorted_yrs:
        d = years[yr]
        point_vals = {
            f: _f(d, f) for f in ["revenue", "gross_profit", "ebit", "ebitda"]
        }
        non_none = {f: v for f, v in point_vals.items() if v is not None}
        if len(non_none) >= 3:
            unique_vals = set(round(v, -3) for v in non_none.values())
            if len(unique_vals) == 1:
                issues.append(_iss(
                    "PATT_012", "multiple_fields", yr, "critical",
                    f"Revenue, Gross Profit, EBIT and EBITDA all have the same value "
                    f"in {yr} ({_disp(list(non_none.values())[0])}). "
                    "Strong indication of field-mapping collision or copy error.",
                    observed=f"All ≈ {_disp(list(non_none.values())[0])}",
                    action="rescrape", cause="mapping",
                ))


# ─────────────────────────────────────────────────────────────────────────────
# E.  DCF-specific checks  (only active when assumptions/valuation present)
# ─────────────────────────────────────────────────────────────────────────────

def _check_dcf_assumptions(
    assumptions: dict,
    valuation:   dict | None,
    forecast:    dict | None,
    years:       dict,
    issues:      list,
    t:           dict,
) -> None:
    # assumptions["wacc"] may be a raw float (Stage 5.5 run) or a dict with a
    # "value" key (final re-run on the full valued dict from the assumption engine).
    _wacc_raw = assumptions.get("wacc")
    wacc = _wacc_raw.get("value") if isinstance(_wacc_raw, dict) else _wacc_raw

    _tgr_raw  = assumptions.get("terminal_growth_rate") or assumptions.get("terminal_growth")
    tgr  = _tgr_raw.get("value") if isinstance(_tgr_raw, dict) else _tgr_raw

    wc     = assumptions.get("wacc_components") or {}
    beta   = wc.get("beta_adjusted")
    cod    = wc.get("cost_of_debt_pretax")
    base_v = (valuation or {}).get("base", {})
    tv_pct = base_v.get("terminal_value_pct_ev")

    # DCF_001: WACC below floor
    if wacc is not None and wacc < t["wacc_min"]:
        issues.append(_iss("DCF_001", "wacc", None, "warning",
            f"WACC ({wacc:.1%}) is below {t['wacc_min']:.0%}. "
            "Unusually low — verify risk-free rate and beta.",
            observed=f"{wacc:.1%}", action="verify", cause="identity"))

    # DCF_002: WACC above ceiling
    if wacc is not None and wacc > t["wacc_max"]:
        issues.append(_iss("DCF_002", "wacc", None, "warning",
            f"WACC ({wacc:.1%}) exceeds {t['wacc_max']:.0%}. "
            "High cost of capital — check equity risk premium and beta.",
            observed=f"{wacc:.1%}", action="verify", cause="identity"))

    # DCF_003: WACC ≤ TGR — Gordon Growth denominator is zero or negative
    if wacc is not None and tgr is not None and wacc <= tgr:
        issues.append(_iss("DCF_003", "wacc", None, "critical",
            f"WACC ({wacc:.1%}) ≤ Terminal Growth Rate ({tgr:.1%}). "
            "Gordon Growth denominator is zero or negative — valuation is undefined.",
            observed=f"WACC={wacc:.1%}, TGR={tgr:.1%}",
            action="exclude", cause="identity"))

    # DCF_004/005: Terminal growth rate
    if tgr is not None:
        if tgr > t["tgr_max_crit"]:
            issues.append(_iss("DCF_004", "terminal_growth_rate", None, "critical",
                f"Terminal growth ({tgr:.1%}) exceeds {t['tgr_max_crit']:.1%}. "
                "Above long-run nominal GDP for developed markets.",
                observed=f"{tgr:.1%}", action="exclude", cause="identity"))
        elif tgr > t["tgr_max_warn"]:
            issues.append(_iss("DCF_005", "terminal_growth_rate", None, "warning",
                f"Terminal growth ({tgr:.1%}) exceeds {t['tgr_max_warn']:.1%}. "
                "Approaching upper bound for a mature company.",
                observed=f"{tgr:.1%}", action="verify", cause="identity"))

    # DCF_006/007: Beta
    if beta is not None:
        if beta < t["beta_min"]:
            issues.append(_iss("DCF_006", "beta", None, "warning",
                f"Beta ({beta:.2f}) < {t['beta_min']} — unusually low. Verify source.",
                observed=f"{beta:.2f}", action="verify", cause="stale"))
        elif beta > t["beta_max"]:
            issues.append(_iss("DCF_007", "beta", None, "warning",
                f"Beta ({beta:.2f}) > {t['beta_max']} — unusually high. May inflate WACC.",
                observed=f"{beta:.2f}", action="verify", cause="stale"))

    # DCF_008: Cost of debt negative
    if cod is not None and cod < 0:
        issues.append(_iss("DCF_008", "cost_of_debt", None, "critical",
            f"Pre-tax cost of debt is negative ({cod:.1%}). Sign or calculation error.",
            observed=f"{cod:.1%}", action="rescrape", cause="sign"))

    # DCF_009/010: Terminal value concentration
    if tv_pct is not None:
        if tv_pct > t["tv_pct_crit"]:
            issues.append(_iss("DCF_009", "terminal_value_pct", None, "critical",
                f"Terminal value is {tv_pct:.0%} of EV. "
                "Extremely high concentration — valuation hypersensitive to TGR/WACC.",
                observed=f"{tv_pct:.0%}", action="verify", cause="identity"))
        elif tv_pct > t["tv_pct_warn"]:
            issues.append(_iss("DCF_010", "terminal_value_pct", None, "warning",
                f"Terminal value is {tv_pct:.0%} of EV. "
                "Material portion in terminal — always present with sensitivity table.",
                observed=f"{tv_pct:.0%}", action="accept", cause="identity"))

    # DCF_011: Forecast revenue growth — large step changes
    if forecast:
        base_proj = forecast.get("base", {})
        yr_keys   = sorted(base_proj.keys())
        for i in range(1, len(yr_keys)):
            g_p = base_proj[yr_keys[i - 1]].get("revenue_growth")
            g_c = base_proj[yr_keys[i]].get("revenue_growth")
            if g_p is not None and g_c is not None:
                step = abs(g_c - g_p)
                if step > t["fcst_rev_step_max"]:
                    issues.append(_iss(
                        "DCF_011", "revenue_growth", yr_keys[i], "warning",
                        f"Forecast revenue growth jumps from {g_p:.1%} to {g_c:.1%} "
                        f"(step: {step:.1%}). Smooth fade is more defensible.",
                        observed=f"{step:.1%} step", action="accept", cause="classif"))

    # DCF_012: Forecast EBIT margins vs historical peak
    if forecast and years:
        base_proj = forecast.get("base", {})
        yr_keys   = sorted(base_proj.keys())
        hist_margins = [
            _margin(_f(years[yr], "ebit"), _f(years[yr], "revenue"))
            for yr in years
            if _f(years[yr], "revenue")
        ]
        hist_peak = max((m for m in hist_margins if m is not None), default=None)
        if hist_peak is not None:
            for yk in yr_keys:
                fcst_m = base_proj[yk].get("ebit_margin")
                if fcst_m is not None and fcst_m > hist_peak + t["fcst_margin_over_peak_max"]:
                    issues.append(_iss(
                        "DCF_012", "ebit_margin", yk, "warning",
                        f"Forecast EBIT margin ({fcst_m:.1%}) exceeds historical peak "
                        f"({hist_peak:.1%}) by more than {t['fcst_margin_over_peak_max']:.0%}.",
                        observed=f"{fcst_m:.1%}",
                        expected=f"≤ {hist_peak + t['fcst_margin_over_peak_max']:.1%}",
                        action="verify", cause="classif"))


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

_CORE_FIELDS = frozenset({
    "revenue", "ebit", "ebitda", "free_cash_flow",
    "debt", "shares_outstanding", "operating_cash_flow",
})


def _calc_score(issues: list, valuation: dict | None = None) -> int:
    score = 100

    criticals = [i for i in issues if i["severity"] == "critical"]
    warnings  = [i for i in issues if i["severity"] == "warning"]
    infos     = [i for i in issues if i["severity"] == "info"]

    # Deduct per issue — calibrated for free data sources where some noise is expected.
    # Criticals are capped at 6 to prevent a volatile multi-year dataset from bottoming
    # out at 0 purely from YoY movement flags.
    capped_criticals = min(len(criticals), 6)
    score -= capped_criticals  * 10
    score -= len(warnings)     * 4
    score -= len(infos)        * 1

    # Extra: multiple criticals on core valuation fields (genuine data problem)
    if sum(1 for i in criticals if i["field"] in _CORE_FIELDS) > 3:
        score -= 7

    # Extra: multiple scaling issues (likely unit mismatch across sources)
    if sum(1 for i in issues if i.get("likely_cause") == "scaling") >= 3:
        score -= 5

    # Extra: TV concentration (model output quality, not data quality)
    if valuation:
        tv_pct = (valuation.get("base") or {}).get("terminal_value_pct_ev")
        if tv_pct and tv_pct > 0.75:
            score -= 5

    return max(0, min(100, score))


def _score_band(score: int) -> str:
    if score >= 90:
        return "high confidence"
    if score >= 75:
        return "usable with review"
    if score >= 50:
        return "low confidence"
    return "do not rely on valuation"


def _pipeline_status(score: int, issues: list) -> str:
    """Pipeline gating status returned to run.py."""
    if score < 50:
        return "fail"
    if any(i["severity"] in ("critical", "warning") for i in issues):
        return "warn"
    return "pass"


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_anomaly_detector(data: dict) -> dict:
    """
    Run all anomaly checks on a pipeline data dict.

    Input must have been through at minimum the standardiser.
    Works best after classifier (sector context available for thresholds).

    DCF-specific checks (section E) run automatically if
    data["assumptions"] and data["valuation"] are present.

    Result is stored at data["data_quality"] AND returned.
    """
    years       = data.get("canonical_by_year", {})
    stats       = data.get("stats", {})
    sector      = data.get("classification")
    company     = data.get("company", "Unknown")
    ticker      = data.get("ticker", "")
    currency    = stats.get("currency", "")

    assumptions = data.get("assumptions")
    valuation   = data.get("valuation")
    forecast    = data.get("forecast")

    # Financials sector — specialist handling flag
    sector_str = (sector or "").lower()
    specialist_flag = sector_str in ("financials", "insurance", "banking", "reits")

    t      = _thresholds(sector)
    issues: list[dict] = []

    if specialist_flag:
        issues.append(_iss(
            "SPEC_001", "sector", None, "warning",
            f"Sector '{sector}' requires specialist DCF treatment. "
            "Standard industrial DCF logic may not apply — "
            "consider sum-of-parts or dividend discount model instead.",
            action="verify", cause="classif",
        ))

    _check_accounting_identities(years, issues, t)
    _check_scaling_signs(years, stats, issues, t)
    _check_historical_movements(years, issues, t)
    _check_timeseries_patterns(years, issues, t)

    if assumptions:
        _check_dcf_assumptions(assumptions, valuation, forecast, years, issues, t)

    score  = _calc_score(issues, valuation)
    status = _pipeline_status(score, issues)

    # Fields flagged for re-check (critical or warning + verify/rescrape action)
    must_recheck = sorted({
        i["field"]
        for i in issues
        if i["severity"] in ("critical", "warning")
        and i["recommended_action"] in (
            "re-scrape from different source",
            "verify against second source",
        )
    })

    result = {
        "status":          status,
        "company":         company,
        "ticker":          ticker,
        "currency":        currency,
        "sector":          sector,
        "quality_score":   score,
        "score_band":      _score_band(score),
        "overall_status":  "critical" if any(i["severity"] == "critical" for i in issues)
                           else "warn" if any(i["severity"] == "warning" for i in issues)
                           else "pass",
        "years_checked":   sorted(years.keys()),
        "issues":          issues,
        "summary": {
            "critical_count":      sum(1 for i in issues if i["severity"] == "critical"),
            "warning_count":       sum(1 for i in issues if i["severity"] == "warning"),
            "info_count":          sum(1 for i in issues if i["severity"] == "info"),
            "must_recheck_fields": must_recheck,
        },
    }

    data["data_quality"] = result
    return result


def print_anomaly_report(result: dict) -> None:
    score   = result["quality_score"]
    band    = result["score_band"]
    summary = result["summary"]
    crits   = summary["critical_count"]
    warns   = summary["warning_count"]
    infos   = summary["info_count"]
    status  = result["overall_status"].upper()

    print("\n" + "=" * 60)
    print(f"  Anomaly Detector  —  {result['company']} ({result['ticker']})")
    print(f"  Quality Score : {score}/100  ({band})")
    print(f"  Status        : [{status}]  |  "
          f"{crits} critical · {warns} warning · {infos} info")
    print("=" * 60)

    if not result["issues"]:
        print("  No anomalies detected.\n")
        return

    for severity, tag in [
        ("critical", "[CRITICAL]"),
        ("warning",  "[WARN]    "),
        ("info",     "[INFO]    "),
    ]:
        items = [i for i in result["issues"] if i["severity"] == severity]
        for iss in items:
            yr_str = f"  [{iss['year']}]" if iss.get("year") else ""
            print(f"\n  {tag}  {iss['field']}{yr_str}  ({iss['rule_id']})")
            print(f"           {iss['message']}")
            if iss.get("observed_value") is not None:
                print(f"           Observed: {iss['observed_value']}"
                      f"  |  Action: {iss['recommended_action']}")

    recheck = result["summary"]["must_recheck_fields"]
    if recheck:
        print(f"\n  Fields to re-check: {', '.join(recheck)}")
    print()
