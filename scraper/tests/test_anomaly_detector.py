"""
Unit tests for scraper/anomaly_detector.py

Run with:
    cd "DCF tool"
    python3 -m pytest scraper/tests/test_anomaly_detector.py -v

Each test class covers one check-family (A–E).
Tests are named after the rule_id they exercise.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
from scraper.anomaly_detector import (
    run_anomaly_detector,
    _check_accounting_identities,
    _check_scaling_signs,
    _check_historical_movements,
    _check_timeseries_patterns,
    _check_dcf_assumptions,
    _calc_score,
    _thresholds,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

M = 1_000_000     # million shorthand
B = 1_000_000_000 # billion shorthand

def _base_year(overrides: dict | None = None) -> dict:
    """Clean one-year dataset that passes all checks."""
    d = {
        "revenue":             50_000 * M,
        "gross_profit":        25_000 * M,
        "ebit":                 7_500 * M,
        "ebitda":               9_000 * M,
        "da":                   1_500 * M,
        "net_income":           5_000 * M,
        "operating_cash_flow":  8_000 * M,
        "capex":               -2_000 * M,
        "free_cash_flow":       6_000 * M,
        "debt":                10_000 * M,
        "long_term_debt":       8_000 * M,
        "cash":                 4_000 * M,
        "total_assets":        60_000 * M,
        "shares_outstanding":   3_000 * M,
        "tax_provision":        2_000 * M,
        "accounts_receivable":  5_000 * M,
        "accounts_payable":     4_000 * M,
        "inventory":            3_000 * M,
    }
    if overrides:
        d.update(overrides)
    return d


def _two_years(y1_overrides=None, y2_overrides=None) -> dict:
    """Two clean consecutive years."""
    y1 = _base_year(y1_overrides)
    y2 = _base_year(y2_overrides or {
        "revenue":             52_000 * M,   # +4%
        "gross_profit":        26_000 * M,
        "ebit":                 7_800 * M,
        "ebitda":               9_360 * M,
        "da":                   1_560 * M,
        "net_income":           5_200 * M,
        "operating_cash_flow":  8_300 * M,
        "capex":               -2_100 * M,
        "free_cash_flow":       6_200 * M,
        "debt":                 9_500 * M,
        "long_term_debt":       7_600 * M,
        "cash":                 4_500 * M,
        "total_assets":        62_000 * M,
        "shares_outstanding":   3_000 * M,
        "tax_provision":        2_080 * M,
        "accounts_receivable":  5_200 * M,
        "accounts_payable":     4_150 * M,
        "inventory":            3_100 * M,
    })
    return {"2023": y1, "2024": y2}


def _issues_by_rule(issues: list, prefix: str) -> list:
    return [i for i in issues if i["rule_id"].startswith(prefix)]


def _has_rule(issues: list, rule_id: str) -> bool:
    return any(i["rule_id"] == rule_id for i in issues)


def _run(years: dict, stats: dict | None = None,
         assumptions: dict | None = None,
         valuation: dict | None = None,
         forecast: dict | None = None,
         sector: str | None = None) -> dict:
    """Convenience: build minimal data dict and run detector."""
    data = {
        "canonical_by_year": years,
        "stats": stats or {
            "currency": "GBP",
            "current_price": 10.0,
            "market_cap":    30_000 * M,
            "shares_outstanding": 3_000 * M,
        },
        "classification": sector or "consumer",
        "company": "TestCo",
        "ticker":  "TST",
    }
    if assumptions:
        data["assumptions"] = assumptions
    if valuation:
        data["valuation"] = valuation
    if forecast:
        data["forecast"] = forecast
    return run_anomaly_detector(data)


# ─────────────────────────────────────────────────────────────────────────────
# A.  Accounting identity checks
# ─────────────────────────────────────────────────────────────────────────────

class TestAccountingIdentities:

    def test_clean_data_no_acct_issues(self):
        result = _run({"2024": _base_year()})
        acct = _issues_by_rule(result["issues"], "ACCT")
        assert acct == [], f"Expected no ACCT issues, got: {acct}"

    def test_ACCT_001_ebitda_ebit_da_mismatch(self):
        """EBITDA diverges materially from EBIT + D&A."""
        d = _base_year({"ebitda": 20_000 * M})   # should be ~9000
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_001"), "Should flag EBITDA identity mismatch"

    def test_ACCT_002_fcf_vs_ocf_capex(self):
        """FCF diverges materially from OCF + Capex."""
        d = _base_year({"free_cash_flow": 15_000 * M})  # OCF-Capex ≈ 6000
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_002")

    def test_ACCT_003_gross_profit_exceeds_revenue(self):
        d = _base_year({"gross_profit": 55_000 * M})  # > revenue 50k
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_003")
        sev = next(i for i in result["issues"] if i["rule_id"] == "ACCT_003")
        assert sev["severity"] == "critical"

    def test_ACCT_004_gross_profit_equals_revenue(self):
        """Classic field-mapping collision — GP = Revenue exactly."""
        d = _base_year({"gross_profit": 50_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_004")

    def test_ACCT_005_ebit_equals_revenue(self):
        d = _base_year({"ebit": 50_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_005")

    def test_ACCT_007_ebitda_below_ebit(self):
        """EBITDA below EBIT is mathematically impossible."""
        d = _base_year({"ebitda": 5_000 * M, "ebit": 7_500 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_007")

    def test_ACCT_008_cash_exceeds_assets(self):
        d = _base_year({"cash": 80_000 * M, "total_assets": 60_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_008")

    def test_ACCT_009_total_debt_less_than_lt_debt(self):
        d = _base_year({"debt": 5_000 * M, "long_term_debt": 8_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_009")

    def test_ACCT_010_fcf_exceeds_revenue(self):
        d = _base_year({"free_cash_flow": 60_000 * M})  # > revenue
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_010")

    def test_ACCT_011_negative_revenue(self):
        d = _base_year({"revenue": -1_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_011")

    def test_ACCT_012_negative_cash(self):
        d = _base_year({"cash": -500 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_012")

    def test_ACCT_013_negative_debt(self):
        d = _base_year({"debt": -1_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "ACCT_013")


# ─────────────────────────────────────────────────────────────────────────────
# B.  Scaling and sign checks
# ─────────────────────────────────────────────────────────────────────────────

class TestScalingSigns:

    def test_SIGN_001_capex_positive(self):
        """Capex positive = sign inversion."""
        d = _base_year({"capex": 2_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "SIGN_001")

    def test_SIGN_002_da_negative(self):
        d = _base_year({"da": -1_500 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "SIGN_002")

    def test_SCAL_001_gross_margin_over_100(self):
        d = _base_year({"gross_profit": 55_000 * M, "revenue": 50_000 * M})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "SCAL_001")

    def test_SCAL_003_ebit_margin_over_sector_max(self):
        """Consumer sector max is 30% — flag 70% margin."""
        d = _base_year({"ebit": 35_000 * M})  # 70% of 50k revenue
        result = _run({"2024": d}, sector="consumer")
        assert _has_rule(result["issues"], "SCAL_003")

    def test_SCAL_010_implied_price_mismatch(self):
        """Market cap / shares implies price far from scraped price."""
        stats = {
            "currency": "GBP",
            "current_price": 10.0,
            "market_cap":    300 * B,   # implies ~100 per share
            "shares_outstanding": 3_000 * M,
        }
        result = _run({"2024": _base_year()}, stats=stats)
        assert _has_rule(result["issues"], "SCAL_010")

    def test_SCAL_010_clean_no_flag(self):
        """Market cap consistent with price and shares — no flag."""
        stats = {
            "currency": "GBP",
            "current_price": 10.0,
            "market_cap":    30_000 * M,   # 10 × 3000M shares
            "shares_outstanding": 3_000 * M,
        }
        result = _run({"2024": _base_year()}, stats=stats)
        assert not _has_rule(result["issues"], "SCAL_010")

    def test_SCAL_005_da_zero_for_large_company(self):
        d = _base_year({"da": 0})
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "SCAL_005")


# ─────────────────────────────────────────────────────────────────────────────
# C.  Historical movement checks
# ─────────────────────────────────────────────────────────────────────────────

class TestHistoricalMovements:

    def test_clean_yoy_no_hist_issues(self):
        result = _run(_two_years())
        hist = _issues_by_rule(result["issues"], "HIST")
        assert hist == [], f"Clean YoY should produce no HIST issues: {hist}"

    def test_HIST_001_revenue_spike_over_150pct(self):
        years = _two_years(y2_overrides={"revenue": 130_000 * M})  # +160%
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_001")

    def test_HIST_002_revenue_drop_over_60pct(self):
        years = _two_years(y2_overrides={"revenue": 15_000 * M})   # -70%
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_002")

    def test_HIST_003_revenue_crit_40pct(self):
        """Revenue up 45% — critical for consumer sector (threshold 30%)."""
        years = _two_years(y2_overrides={"revenue": 73_000 * M})  # ~+46%
        result = _run(years, sector="consumer")
        assert _has_rule(result["issues"], "HIST_003")

    def test_HIST_010_ebitda_crit_jump(self):
        years = _two_years(y2_overrides={"ebitda": 20_000 * M})  # +122%
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_010")

    def test_HIST_012_ebitda_doubles_revenue_flat(self):
        """Classic suspicious pattern: EBITDA 2× while revenue flat."""
        years = _two_years(y2_overrides={
            "revenue": 50_500 * M,   # ~flat
            "ebitda":  18_000 * M,   # was 9000 → doubled
        })
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_012")

    def test_HIST_013_ebitda_margin_move(self):
        """EBITDA margin jumps 15pp in one year — over consumer critical (12pp)."""
        years = _two_years(y2_overrides={
            "revenue": 52_000 * M,
            "ebitda":  17_160 * M,  # 33% margin vs 18% — 15pp move
        })
        result = _run(years, sector="consumer")
        assert _has_rule(result["issues"], "HIST_013")

    def test_HIST_022_ebit_margin_move_crit(self):
        """EBIT margin jumps 10pp — over consumer critical (6pp)."""
        years = _two_years(y2_overrides={
            "revenue": 52_000 * M,
            "ebit":    18_200 * M,  # 35% vs 15% = 20pp
        })
        result = _run(years, sector="consumer")
        assert _has_rule(result["issues"], "HIST_022")

    def test_HIST_061_capex_sign_flip(self):
        """Capex flips from negative to positive."""
        years = _two_years(
            y1_overrides={"capex": -2_000 * M},
            y2_overrides={"capex":  2_000 * M},
        )
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_061")

    def test_HIST_062_capex_pct_rev_too_high(self):
        """Capex at 25% revenue — over consumer sector critical (12%)."""
        years = _two_years(y2_overrides={"capex": -13_000 * M})  # 25% of 52k
        result = _run(years, sector="consumer")
        assert _has_rule(result["issues"], "HIST_062")

    def test_HIST_070_da_crit_jump(self):
        """D&A more than doubles in one year."""
        years = _two_years(y2_overrides={"da": 4_000 * M})  # +157%
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_070")

    def test_HIST_092_debt_disappears(self):
        """Debt drops from material level to near-zero."""
        years = _two_years(
            y1_overrides={"debt": 10_000 * M},
            y2_overrides={"debt": 100_000},   # near zero
        )
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_092")

    def test_HIST_100_shares_large_change(self):
        """Shares outstanding change by 30% in one year."""
        years = _two_years(y2_overrides={"shares_outstanding": 4_000 * M})  # +33%
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_100")

    def test_HIST_110_zero_tax_multiple_years(self):
        """Tax = 0 in 3 profitable years."""
        years = {
            "2022": _base_year({"tax_provision": 0}),
            "2023": _base_year({"tax_provision": 0}),
            "2024": _base_year({"tax_provision": 0}),
        }
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_110")

    def test_HIST_111_capex_zero_multiple_years(self):
        """Capex = 0 for 3 years in a large company."""
        years = {
            "2022": _base_year({"capex": 0}),
            "2023": _base_year({"capex": 0}),
            "2024": _base_year({"capex": 0}),
        }
        result = _run(years)
        assert _has_rule(result["issues"], "HIST_111")


# ─────────────────────────────────────────────────────────────────────────────
# D.  Time-series pattern checks
# ─────────────────────────────────────────────────────────────────────────────

class TestTimeSeriesPatterns:

    def test_PATT_001_identical_values_all_years(self):
        """Revenue identical for 4 years — backfill."""
        years = {str(y): _base_year() for y in range(2021, 2025)}
        result = _run(years)
        assert _has_rule(result["issues"], "PATT_001")

    def test_PATT_001_varying_values_no_flag(self):
        """Revenue varies across years — PATT_001 must not fire on revenue specifically."""
        years = {
            "2021": _base_year({"revenue": 40_000 * M}),
            "2022": _base_year({"revenue": 43_000 * M}),
            "2023": _base_year({"revenue": 46_000 * M}),
            "2024": _base_year({"revenue": 50_000 * M}),
        }
        result = _run(years)
        # Revenue itself should not trigger PATT_001 (it varies)
        revenue_patt001 = [
            i for i in result["issues"]
            if i["rule_id"] == "PATT_001" and i["field"] == "revenue"
        ]
        assert revenue_patt001 == [], "Revenue varies — PATT_001 must not fire on revenue"

    def test_PATT_003_spike_10x(self):
        """Single year value 10× neighbours."""
        years = {
            "2021": _base_year({"revenue": 50_000 * M}),
            "2022": _base_year({"revenue": 600_000 * M}),  # 12× spike
            "2023": _base_year({"revenue": 52_000 * M}),
            "2024": _base_year({"revenue": 54_000 * M}),
        }
        result = _run(years)
        assert _has_rule(result["issues"], "PATT_003")

    def test_PATT_003_no_spike_clean(self):
        """Gradual growth — no spike."""
        years = {
            "2021": _base_year({"revenue": 40_000 * M}),
            "2022": _base_year({"revenue": 44_000 * M}),
            "2023": _base_year({"revenue": 48_000 * M}),
            "2024": _base_year({"revenue": 52_000 * M}),
        }
        result = _run(years)
        assert not _has_rule(result["issues"], "PATT_003")

    def test_PATT_010_gap_in_years(self):
        """Year 2022 missing from sequence 2021-2024."""
        years = {
            "2021": _base_year(),
            "2023": _base_year(),
            "2024": _base_year(),
        }
        result = _run(years)
        assert _has_rule(result["issues"], "PATT_010")

    def test_PATT_011_shares_identical_5_years(self):
        years = {str(y): _base_year() for y in range(2020, 2025)}
        result = _run(years)
        assert _has_rule(result["issues"], "PATT_011")

    def test_PATT_012_all_income_fields_equal(self):
        """Revenue = GP = EBIT = EBITDA — mapping collision."""
        d = _base_year({
            "revenue":      50_000 * M,
            "gross_profit": 50_000 * M,
            "ebit":         50_000 * M,
            "ebitda":       50_000 * M,
        })
        result = _run({"2024": d})
        assert _has_rule(result["issues"], "PATT_012")


# ─────────────────────────────────────────────────────────────────────────────
# E.  DCF assumption checks
# ─────────────────────────────────────────────────────────────────────────────

class TestDCFAssumptions:

    def _clean_assumptions(self) -> dict:
        return {
            "wacc":                 0.10,
            "terminal_growth_rate": 0.02,
            "wacc_components": {
                "beta_adjusted":        1.0,
                "cost_of_debt_pretax":  0.05,
            },
        }

    def test_DCF_001_wacc_too_low(self):
        asmps = self._clean_assumptions()
        asmps["wacc"] = 0.03  # below 5% floor
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_001")

    def test_DCF_002_wacc_too_high(self):
        asmps = self._clean_assumptions()
        asmps["wacc"] = 0.22
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_002")

    def test_DCF_003_wacc_lte_tgr(self):
        """WACC ≤ TGR — Gordon Growth undefined."""
        asmps = self._clean_assumptions()
        asmps["wacc"] = 0.02
        asmps["terminal_growth_rate"] = 0.03
        result = _run({"2024": _base_year()}, assumptions=asmps)
        iss = next((i for i in result["issues"] if i["rule_id"] == "DCF_003"), None)
        assert iss is not None
        assert iss["severity"] == "critical"

    def test_DCF_003_equal_wacc_tgr(self):
        asmps = self._clean_assumptions()
        asmps["wacc"] = 0.025
        asmps["terminal_growth_rate"] = 0.025
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_003")

    def test_DCF_004_tgr_above_critical(self):
        asmps = self._clean_assumptions()
        asmps["terminal_growth_rate"] = 0.05
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_004")

    def test_DCF_005_tgr_above_warn(self):
        asmps = self._clean_assumptions()
        asmps["terminal_growth_rate"] = 0.038
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_005")

    def test_DCF_006_beta_too_low(self):
        asmps = self._clean_assumptions()
        asmps["wacc_components"]["beta_adjusted"] = 0.1
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_006")

    def test_DCF_007_beta_too_high(self):
        asmps = self._clean_assumptions()
        asmps["wacc_components"]["beta_adjusted"] = 3.0
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_007")

    def test_DCF_008_negative_cost_of_debt(self):
        asmps = self._clean_assumptions()
        asmps["wacc_components"]["cost_of_debt_pretax"] = -0.01
        result = _run({"2024": _base_year()}, assumptions=asmps)
        assert _has_rule(result["issues"], "DCF_008")

    def test_DCF_009_tv_concentration_critical(self):
        asmps = self._clean_assumptions()
        valuation = {"base": {"terminal_value_pct_ev": 0.92, "implied_return": -0.3}}
        result = _run({"2024": _base_year()}, assumptions=asmps, valuation=valuation)
        assert _has_rule(result["issues"], "DCF_009")

    def test_DCF_010_tv_concentration_warn(self):
        asmps = self._clean_assumptions()
        valuation = {"base": {"terminal_value_pct_ev": 0.78, "implied_return": -0.3}}
        result = _run({"2024": _base_year()}, assumptions=asmps, valuation=valuation)
        assert _has_rule(result["issues"], "DCF_010")

    def test_clean_assumptions_no_dcf_issues(self):
        asmps = self._clean_assumptions()
        result = _run({"2024": _base_year()}, assumptions=asmps)
        dcf = _issues_by_rule(result["issues"], "DCF")
        assert dcf == [], f"Clean assumptions should produce no DCF issues: {dcf}"


# ─────────────────────────────────────────────────────────────────────────────
# F.  Sector-specific threshold tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSectorThresholds:

    def test_consumer_tighter_revenue_growth(self):
        """25% revenue growth is consumer-critical (30% threshold) — should warn."""
        years = _two_years(y2_overrides={"revenue": 65_000 * M})  # +30%
        result_consumer  = _run(years, sector="consumer")
        result_resources = _run(years, sector="resources")
        # Consumer should flag it (threshold 30%), resources should not
        consumer_flagged  = any(i["field"] == "revenue" and
                                 i["severity"] in ("critical", "warning")
                                 for i in result_consumer["issues"])
        resources_flagged = any(i["field"] == "revenue" and
                                 i["rule_id"].startswith("HIST")
                                 for i in result_resources["issues"])
        assert consumer_flagged
        assert not resources_flagged

    def test_utilities_higher_capex_allowed(self):
        """30% capex/rev is fine for utilities, critical for consumer."""
        years = _two_years(y2_overrides={"capex": -15_600 * M})  # 30% of 52k
        result_util     = _run(years, sector="utilities")
        result_consumer = _run(years, sector="consumer")

        util_crit     = any(i["rule_id"] == "HIST_062" and i["severity"] == "critical"
                             for i in result_util["issues"])
        consumer_crit = any(i["rule_id"] == "HIST_062" and i["severity"] == "critical"
                             for i in result_consumer["issues"])

        assert not util_crit,     "Utilities: 30% capex should not be critical"
        assert consumer_crit,     "Consumer: 30% capex should be critical"

    def test_specialist_sector_flagged(self):
        """Financials sector should trigger SPEC_001."""
        result = _run({"2024": _base_year()}, sector="financials")
        assert _has_rule(result["issues"], "SPEC_001")


# ─────────────────────────────────────────────────────────────────────────────
# G.  Scoring and pipeline status
# ─────────────────────────────────────────────────────────────────────────────

class TestScoring:

    def test_perfect_score_clean_data(self):
        result = _run({"2024": _base_year()})
        assert result["quality_score"] == 100

    def test_critical_subtracts_10(self):
        """One critical issue should reduce score by 10."""
        d = _base_year({"gross_profit": 55_000 * M})   # triggers ACCT_003 critical
        result = _run({"2024": d})
        crits = sum(1 for i in result["issues"] if i["severity"] == "critical")
        capped = min(crits, 6)
        expected = max(0, 100 - capped * 10)
        assert result["quality_score"] == expected

    def test_warning_subtracts_4(self):
        """Warning-only issue should subtract 4."""
        years = _two_years(y2_overrides={"revenue": 60_000 * M})  # ~+15%, warns for consumer
        result = _run(years, sector="consumer")
        warns = sum(1 for i in result["issues"] if i["severity"] == "warning")
        crits = sum(1 for i in result["issues"] if i["severity"] == "critical")
        capped = min(crits, 6)
        expected = max(0, 100 - capped * 10 - warns * 4)
        assert result["quality_score"] == expected

    def test_score_band_high_confidence(self):
        result = _run({"2024": _base_year()})
        assert result["score_band"] == "high confidence"

    def test_score_band_fail_on_many_criticals(self):
        """Multiple criticals should push score below 50 → fail status."""
        d = _base_year({
            "gross_profit":   55_000 * M,   # ACCT_003 + ACCT_004 or similar
            "ebit":           50_000 * M,   # ACCT_005
            "ebitda":         50_000 * M,   # ACCT_006
            "cash":           80_000 * M,   # ACCT_008
            "free_cash_flow": 60_000 * M,   # ACCT_010
        })
        result = _run({"2024": d})
        assert result["quality_score"] < 50
        assert result["status"] == "fail"

    def test_pipeline_status_pass_clean(self):
        result = _run({"2024": _base_year()})
        assert result["status"] == "pass"

    def test_pipeline_status_warn_with_warnings(self):
        """Warns but score ≥ 50 → status 'warn'."""
        years = _two_years(y2_overrides={"revenue": 57_500 * M})  # +15%, consumer warn
        result = _run(years, sector="consumer")
        if result["summary"]["warning_count"] > 0 and result["quality_score"] >= 50:
            assert result["status"] == "warn"

    def test_must_recheck_fields_populated(self):
        """Critical issues with verify/rescrape action populate must_recheck_fields."""
        d = _base_year({"gross_profit": 55_000 * M})  # ACCT_003
        result = _run({"2024": d})
        assert "gross_profit" in result["summary"]["must_recheck_fields"]

    def test_tv_penalty_applied(self):
        """TV between 75-85% triggers DCF_010 (warning -4) + TV extra penalty (-5) = -9."""
        asmps = {
            "wacc": 0.10,
            "terminal_growth_rate": 0.02,
            "wacc_components": {"beta_adjusted": 1.0, "cost_of_debt_pretax": 0.05},
        }
        # 0.82 → DCF_010 warning (-4) + TV extra penalty (-5) → score = 91
        valuation = {"base": {"terminal_value_pct_ev": 0.82, "implied_return": -0.3}}
        result = _run({"2024": _base_year()}, assumptions=asmps, valuation=valuation)
        assert _has_rule(result["issues"], "DCF_010")
        assert result["quality_score"] == 91  # 100 - 4 (warn) - 5 (TV penalty)

    def test_tv_penalty_critical_applied(self):
        """TV > 85% triggers DCF_009 (critical -10) + TV extra penalty (-5) = -15."""
        asmps = {
            "wacc": 0.10,
            "terminal_growth_rate": 0.02,
            "wacc_components": {"beta_adjusted": 1.0, "cost_of_debt_pretax": 0.05},
        }
        valuation = {"base": {"terminal_value_pct_ev": 0.92, "implied_return": -0.3}}
        result = _run({"2024": _base_year()}, assumptions=asmps, valuation=valuation)
        assert _has_rule(result["issues"], "DCF_009")
        assert result["quality_score"] <= 85  # 100 - 10 (crit) - 5 (TV penalty)

    def test_summary_counts_accurate(self):
        d = _base_year({
            "gross_profit": 55_000 * M,   # critical
            "ebit":          50_000 * M,   # critical
        })
        result = _run({"2024": d})
        crits  = result["summary"]["critical_count"]
        actual = sum(1 for i in result["issues"] if i["severity"] == "critical")
        assert crits == actual


# ─────────────────────────────────────────────────────────────────────────────
# H.  Integration — full pipeline data dicts (realistic examples)
# ─────────────────────────────────────────────────────────────────────────────

class TestIntegration:

    def test_unilever_style_clean(self):
        """
        Stable consumer staples — 5 years of Unilever-like numbers with all key
        fields varied year-on-year so PATT_001 does not fire on any field.
        Should produce high quality score with no criticals.
        """
        def _ul(rev, gp, ebit, ebitda, da, ni, ocf, capex, debt, cash, tax, ar, ap, inv):
            return {
                "revenue":             rev,
                "gross_profit":        gp,
                "ebit":                ebit,
                "ebitda":              ebitda,
                "da":                  da,
                "net_income":          ni,
                "operating_cash_flow": ocf,
                "capex":               capex,
                "free_cash_flow":      ocf + capex,
                "debt":                debt,
                "long_term_debt":      int(debt * 0.8),
                "cash":                cash,
                "total_assets":        rev * 2,
                "shares_outstanding":  3_000 * M,
                "tax_provision":       tax,
                "accounts_receivable": ar,
                "accounts_payable":    ap,
                "inventory":           inv,
            }

        years = {
            "2020": _ul(50_700*M, 25_100*M, 8_000*M, 10_000*M, 2_000*M,
                        5_500*M, 9_200*M, -2_000*M, 11_000*M, 3_800*M,
                        2_100*M, 5_000*M, 4_000*M, 3_000*M),
            "2021": _ul(52_400*M, 25_900*M, 8_300*M, 10_400*M, 2_100*M,
                        5_700*M, 9_500*M, -2_050*M, 10_500*M, 4_100*M,
                        2_200*M, 5_150*M, 4_100*M, 3_080*M),
            "2022": _ul(54_000*M, 26_600*M, 8_100*M, 10_200*M, 2_100*M,
                        5_500*M, 9_300*M, -2_100*M, 10_200*M, 4_300*M,
                        2_100*M, 5_300*M, 4_200*M, 3_150*M),
            "2023": _ul(56_000*M, 27_500*M, 8_700*M, 10_900*M, 2_200*M,
                        6_000*M, 9_800*M, -2_150*M, 9_900*M, 4_600*M,
                        2_300*M, 5_450*M, 4_350*M, 3_230*M),
            "2024": _ul(59_000*M, 29_000*M, 9_200*M, 11_500*M, 2_300*M,
                        6_300*M, 10_200*M, -2_250*M, 9_600*M, 4_900*M,
                        2_450*M, 5_600*M, 4_500*M, 3_320*M),
        }
        result = _run(years, sector="consumer")
        assert result["summary"]["critical_count"] == 0, \
            f"Expected no criticals, got: {[i for i in result['issues'] if i['severity']=='critical']}"
        assert result["quality_score"] >= 75

    def test_suspicious_scrape_low_score(self):
        """
        Dataset with classic fishy data signals.
        Should produce a low quality score and multiple criticals.
        """
        years = {
            "2022": _base_year(),
            "2023": _base_year({
                "revenue":      50_000 * M,   # flat
                "gross_profit": 50_000 * M,   # equals revenue (ACCT_004)
                "ebitda":       18_000 * M,   # doubled (HIST_012)
                "cash":         80_000 * M,   # exceeds assets (ACCT_008)
            }),
        }
        result = _run(years, sector="consumer")
        assert result["summary"]["critical_count"] >= 3
        assert result["quality_score"] < 50
        assert result["status"] == "fail"

    def test_output_structure(self):
        """Verify all required output keys are present."""
        result = _run({"2024": _base_year()})
        required = {
            "status", "company", "ticker", "currency", "sector",
            "quality_score", "score_band", "overall_status",
            "years_checked", "issues", "summary",
        }
        assert required.issubset(result.keys())
        sum_keys = {"critical_count", "warning_count", "info_count", "must_recheck_fields"}
        assert sum_keys.issubset(result["summary"].keys())

    def test_issue_structure(self):
        """Every issue must have all required fields."""
        d = _base_year({"gross_profit": 55_000 * M})
        result = _run({"2024": d})
        required = {
            "rule_id", "field", "year", "severity", "message",
            "recommended_action", "likely_cause",
        }
        for iss in result["issues"]:
            missing = required - iss.keys()
            assert not missing, f"Issue missing fields: {missing}  — {iss}"

    def test_detector_stores_result_on_data_dict(self):
        """run_anomaly_detector should write result back to data['data_quality']."""
        data = {
            "canonical_by_year": {"2024": _base_year()},
            "stats": {
                "currency": "GBP",
                "current_price": 10.0,
                "market_cap": 30_000 * M,
                "shares_outstanding": 3_000 * M,
            },
            "classification": "consumer",
            "company": "TestCo",
            "ticker": "TST",
        }
        result = run_anomaly_detector(data)
        assert "data_quality" in data
        assert data["data_quality"] is result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
