"""
Unit tests for high_risk_fields.py  —  Stage 2.6

Run from the DCF tool root:
    python -m pytest canonical_fundamentals/tests/test_high_risk_fields.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from canonical_fundamentals.high_risk_fields import (
    _validate_gross_profit,
    _rebuild_net_debt,
    _validate_shares,
    _check_shares_staleness,
    _validate_tax_rate,
    _check_tax_rate_yoy,
    _check_zero_tax,
    _smooth_tax_rate,
    _derive_and_validate_nwc,
    _check_ocf_supplemental,
    _check_capex_supplemental,
    _rebuild_ev_multiples,
    run_high_risk_validation,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _make_year(overrides: dict | None = None) -> dict:
    """
    Realistic year dict: large-cap industrial.
    Revenue ~50B, GP ~20B (40% margin), EBIT ~8B, EBITDA ~12B.
    Debt 15B, LT debt 12B, Leases 2B, Cash 5B → Net Debt ~12B.
    Shares ~1B (diluted), EPS 6.0, NI ~6B.
    OCF ~11B, Capex 3B, FCF ~8B.
    Tax ~1.5B on pre-tax ~7.5B → 20% effective rate.
    CA 15B, CL 10B → NWC 5B.
    """
    base = {
        "revenue":               50_000,
        "gross_profit":          20_000,
        "ebit":                   8_000,
        "ebitda":                12_000,
        "da":                     4_000,
        "net_income":             6_000,
        "pre_tax_income":         7_500,
        "tax_provision":          1_500,
        "interest_expense":         500,
        "operating_cash_flow":   11_000,
        "capex":                  3_000,
        "free_cash_flow":         8_000,
        "change_in_working_cap":   -500,   # OCF statement: WC build absorbs cash
        "debt":                  15_000,
        "long_term_debt":        12_000,
        "lease_liabilities":      2_000,
        "cash":                   5_000,
        "total_assets":          80_000,
        "current_assets":        15_000,
        "current_liabilities":   10_000,
        "shares_outstanding":     1_000,   # millions
        "eps_diluted":             6.00,   # NI 6000 / shares 1000 = 6.0
        "_sources": {
            "ebit":           "yahoo",
            "free_cash_flow": "yahoo",
        },
        "_mapping_confidence": {
            "ebit": "medium",
        },
    }
    if overrides:
        base.update(overrides)
    return base


def _make_standardised(years: dict | None = None, stats: dict | None = None) -> dict:
    if years is None:
        years = {
            "2023": _make_year(),
            "2022": _make_year({"revenue": 47_000, "gross_profit": 18_000}),
            "2021": _make_year({"revenue": 44_000, "gross_profit": 17_000}),
        }
    if stats is None:
        stats = {
            "sector":        "Industrials",
            "market_cap":    60_000,   # millions — MC slightly above equity value
            "current_price": 60.0,     # shares ~1000M → implied MC ~60B ✓
        }
    return {
        "ticker":             "TEST",
        "company_name":       "Test Industrial Co",
        "status":             "pass",
        "blockers":           [],
        "warnings":           [],
        "stats":              stats,
        "canonical_by_year":  years,
        "years_available":    sorted(years.keys(), reverse=True),
        "field_mapping_table": [],
        "unmapped_fields":    [],
        "metadata":           {},
    }


# ─────────────────────────────────────────────────────────────────────────────
# A.  Gross Profit validation
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateGrossProfit:
    def test_normal_case_no_issues(self):
        yr = _make_year()
        issues = []
        rpt = _validate_gross_profit("2023", yr, "Industrials", issues)
        assert rpt["status"] == "ok"
        assert rpt["gross_margin"] == pytest_approx(0.40, abs=0.01)
        assert issues == []

    def test_gp_exceeds_revenue_critical(self):
        yr = _make_year({"gross_profit": 55_000, "revenue": 50_000})
        issues = []
        rpt = _validate_gross_profit("2023", yr, "Industrials", issues)
        assert rpt["status"] == "critical"
        assert any(i["rule_id"] == "HRF-GP-001" for i in issues)

    def test_gp_equals_revenue_exactly(self):
        yr = _make_year({"gross_profit": 50_000, "revenue": 50_000})
        issues = []
        rpt = _validate_gross_profit("2023", yr, "Industrials", issues)
        assert rpt["status"] == "critical"
        assert any(i["rule_id"] == "HRF-GP-002" for i in issues)

    def test_gp_equals_ebit_warns(self):
        yr = _make_year({"gross_profit": 8_000, "ebit": 8_000})
        issues = []
        rpt = _validate_gross_profit("2023", yr, "Industrials", issues)
        assert rpt["status"] == "warn"
        assert any(i["rule_id"] == "HRF-GP-003" for i in issues)

    def test_gp_margin_above_sector_ceiling(self):
        # 95% margin for Industrials is implausible (ceiling ~50%)
        yr = _make_year({"gross_profit": 47_500, "revenue": 50_000})
        issues = []
        rpt = _validate_gross_profit("2023", yr, "Industrials", issues)
        assert rpt["status"] == "warn"
        assert any(i["rule_id"] == "HRF-GP-004" for i in issues)

    def test_high_margin_ok_for_software(self):
        # 80% margin is fine for software
        yr = _make_year({"gross_profit": 40_000, "revenue": 50_000})
        issues = []
        rpt = _validate_gross_profit("2023", yr, "software", issues)
        # Should not flag HRF-GP-004 for software
        gp_004 = [i for i in issues if i["rule_id"] == "HRF-GP-004"]
        assert gp_004 == []

    def test_missing_fields_no_crash(self):
        yr = _make_year()
        yr.pop("gross_profit")
        issues = []
        rpt = _validate_gross_profit("2023", yr, "Industrials", issues)
        assert rpt["status"] == "ok"
        assert "missing" in rpt["flags"][0]


# ─────────────────────────────────────────────────────────────────────────────
# B.  Net Debt reconstruction
# ─────────────────────────────────────────────────────────────────────────────

class TestRebuildNetDebt:
    def test_standard_rebuild(self):
        yr = _make_year()
        issues = []
        rpt = _rebuild_net_debt("2023", yr, "Industrials", None, issues)
        # Net debt = 15000 (debt) + 2000 (leases) - 5000 (cash) = 12000
        assert rpt["derived_net_debt"] == 12_000
        assert rpt["status"] == "ok"
        assert issues == []

    def test_lt_debt_exceeds_total_debt_critical(self):
        # LT debt > total debt → impossible
        yr = _make_year({"debt": 10_000, "long_term_debt": 15_000})
        issues = []
        rpt = _rebuild_net_debt("2023", yr, "Industrials", None, issues)
        assert rpt["status"] == "critical"
        assert any(i["rule_id"] == "HRF-ND-001" for i in issues)
        # Falls back to LT debt as the total floor
        assert rpt["total_debt"] == 15_000

    def test_missing_debt_uses_lt_debt_with_warning(self):
        yr = _make_year()
        yr.pop("debt")
        issues = []
        rpt = _rebuild_net_debt("2023", yr, "Industrials", None, issues)
        assert rpt["total_debt"] == 12_000   # long_term_debt
        assert rpt["status"] == "warn"
        assert any(i["rule_id"] == "HRF-ND-001" for i in issues)

    def test_no_leases_info_flag_for_asset_heavy(self):
        yr = _make_year()
        yr.pop("lease_liabilities")
        issues = []
        rpt = _rebuild_net_debt("2023", yr, "Industrials", None, issues)
        # Should still compute net debt, and flag missing leases for non-asset-light
        assert rpt["derived_net_debt"] is not None
        assert any(i["rule_id"] == "HRF-ND-003" for i in issues)

    def test_no_lease_flag_for_asset_light(self):
        yr = _make_year()
        yr.pop("lease_liabilities")
        issues = []
        rpt = _rebuild_net_debt("2023", yr, "Industrials", "asset_light", issues)
        # Should NOT flag missing leases for asset-light company
        nd003 = [i for i in issues if i["rule_id"] == "HRF-ND-003"]
        assert nd003 == []

    def test_net_debt_stored_in_canonical(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        yr  = out["canonical_by_year"]["2023"]
        assert yr.get("derived_net_debt") == 12_000

    def test_both_debt_and_cash_missing(self):
        yr = _make_year()
        yr.pop("debt")
        yr.pop("long_term_debt")
        issues = []
        rpt = _rebuild_net_debt("2023", yr, "Industrials", None, issues)
        assert rpt["derived_net_debt"] is None


# ─────────────────────────────────────────────────────────────────────────────
# C.  Shares validation
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateShares:
    def test_consistent_shares_no_issues(self):
        yr = _make_year()
        # NI=6000, EPS=6.0 → implied = 1000 = shares_outstanding ✓
        issues = []
        rpt = _validate_shares("2023", yr, None, {}, issues)
        assert rpt["status"] == "ok"
        assert abs(rpt["implied_from_eps"] - 1_000) < 1

    def test_basic_vs_diluted_confusion_flagged(self):
        # shares_outstanding is 1200 (basic) but NI/EPS implies 1000 (diluted)
        yr = _make_year({"shares_outstanding": 1_200})
        issues = []
        rpt = _validate_shares("2023", yr, None, {}, issues)
        assert rpt["status"] in ("warning", "warn", "critical")
        assert any(i["rule_id"] == "HRF-SH-001" for i in issues)

    def test_large_yoy_change_flagged(self):
        yr      = _make_year()
        prior   = _make_year({"shares_outstanding": 700})  # 43% increase → flag
        issues  = []
        rpt     = _validate_shares("2023", yr, prior, {}, issues)
        assert any(i["rule_id"] == "HRF-SH-003" for i in issues)

    def test_small_yoy_change_ok(self):
        yr    = _make_year()
        prior = _make_year({"shares_outstanding": 990})   # 1% change → fine
        issues = []
        rpt = _validate_shares("2023", yr, prior, {}, issues)
        sh_003 = [i for i in issues if i["rule_id"] == "HRF-SH-003"]
        assert sh_003 == []

    def test_market_cap_implied_shares_check(self):
        # market_cap=60000, price=60 → implied shares=1000 ✓
        yr = _make_year()
        issues = []
        stats = {"market_cap": 60_000, "current_price": 60.0}
        rpt = _validate_shares("2023", yr, None, stats, issues)
        sh_004 = [i for i in issues if i["rule_id"] == "HRF-SH-004"]
        assert sh_004 == []   # should agree

    def test_market_cap_implied_shares_diverge(self):
        # market_cap=90000, price=60 → implied shares=1500, stored=1000 → 50% divergence
        yr = _make_year()
        issues = []
        stats = {"market_cap": 90_000, "current_price": 60.0}
        rpt = _validate_shares("2023", yr, None, stats, issues)
        assert any(i["rule_id"] == "HRF-SH-004" for i in issues)

    def test_missing_shares_no_crash(self):
        yr = _make_year()
        yr.pop("shares_outstanding")
        issues = []
        rpt = _validate_shares("2023", yr, None, {}, issues)
        assert rpt["shares_outstanding"] is None


class TestSharesStaleness:
    def test_identical_shares_3_years_flagged(self):
        years = {
            "2021": _make_year({"shares_outstanding": 1_000}),
            "2022": _make_year({"shares_outstanding": 1_000}),
            "2023": _make_year({"shares_outstanding": 1_000}),
        }
        issues = []
        _check_shares_staleness(years, issues)
        assert any(i["rule_id"] == "HRF-SH-002" for i in issues)

    def test_varying_shares_no_flag(self):
        years = {
            "2021": _make_year({"shares_outstanding": 1_050}),
            "2022": _make_year({"shares_outstanding": 1_020}),
            "2023": _make_year({"shares_outstanding": 1_000}),
        }
        issues = []
        _check_shares_staleness(years, issues)
        sh_002 = [i for i in issues if i["rule_id"] == "HRF-SH-002"]
        assert sh_002 == []

    def test_only_two_years_not_flagged(self):
        years = {
            "2022": _make_year({"shares_outstanding": 1_000}),
            "2023": _make_year({"shares_outstanding": 1_000}),
        }
        issues = []
        _check_shares_staleness(years, issues)
        sh_002 = [i for i in issues if i["rule_id"] == "HRF-SH-002"]
        assert sh_002 == []


# ─────────────────────────────────────────────────────────────────────────────
# D.  Tax Rate
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateTaxRate:
    def test_normal_rate_no_issues(self):
        yr = _make_year()   # tax 1500/7500 = 20%
        issues = []
        rpt = _validate_tax_rate("2023", yr, issues)
        assert rpt["effective_rate"] == pytest_approx(0.20, abs=0.001)
        assert rpt["status"] == "ok"
        assert issues == []

    def test_negative_rate_with_positive_pretax_flagged(self):
        yr = _make_year({"tax_provision": -500, "pre_tax_income": 7_500})
        issues = []
        rpt = _validate_tax_rate("2023", yr, issues)
        assert rpt["status"] == "warn"
        assert any(i["rule_id"] == "HRF-TR-003" for i in issues)

    def test_rate_above_55pct_flagged(self):
        yr = _make_year({"tax_provision": 4_500, "pre_tax_income": 7_500})  # 60%
        issues = []
        rpt = _validate_tax_rate("2023", yr, issues)
        assert rpt["status"] == "warn"
        assert any(i["rule_id"] == "HRF-TR-001" for i in issues)

    def test_missing_pretax_returns_na(self):
        yr = _make_year()
        yr.pop("pre_tax_income")
        issues = []
        rpt = _validate_tax_rate("2023", yr, issues)
        assert rpt["status"] == "n/a"
        assert rpt["effective_rate"] is None


class TestTaxRateYoY:
    def test_yoy_spike_flagged(self):
        issues = []
        _check_tax_rate_yoy("2023", 0.40, 0.20, issues)  # 20pp swing
        assert any(i["rule_id"] == "HRF-TR-002" for i in issues)

    def test_small_swing_ok(self):
        issues = []
        _check_tax_rate_yoy("2023", 0.22, 0.20, issues)  # 2pp swing
        assert issues == []


class TestZeroTaxCheck:
    def test_zero_tax_profitable_2_years_flagged(self):
        years = {
            "2022": _make_year({"tax_provision": 0, "pre_tax_income": 5_000}),
            "2023": _make_year({"tax_provision": 0, "pre_tax_income": 7_500}),
        }
        issues = []
        _check_zero_tax(years, issues)
        assert any(i["rule_id"] == "HRF-TR-004" for i in issues)

    def test_normal_tax_no_flag(self):
        years = {
            "2022": _make_year({"tax_provision": 1_000}),
            "2023": _make_year({"tax_provision": 1_500}),
        }
        issues = []
        _check_zero_tax(years, issues)
        tr_004 = [i for i in issues if i["rule_id"] == "HRF-TR-004"]
        assert tr_004 == []


class TestSmoothTaxRate:
    def test_median_of_plausible_rates(self):
        rates = [0.20, 0.22, 0.18, 0.85, -0.10]   # last two are extreme
        smoothed = _smooth_tax_rate(rates)
        # Plausible: [0.20, 0.22, 0.18] → median = 0.20
        assert smoothed == pytest_approx(0.20, abs=0.01)

    def test_all_extreme_returns_none(self):
        rates = [0.80, 0.90, -0.30]
        smoothed = _smooth_tax_rate(rates)
        assert smoothed is None

    def test_single_valid_rate(self):
        smoothed = _smooth_tax_rate([0.21])
        assert smoothed == pytest_approx(0.21, abs=0.001)


# ─────────────────────────────────────────────────────────────────────────────
# E.  Working Capital
# ─────────────────────────────────────────────────────────────────────────────

class TestDeriveAndValidateNwc:
    def test_standard_nwc_derivation(self):
        yr = _make_year()   # CA=15000, CL=10000 → NWC=5000
        issues = []
        rpt = _derive_and_validate_nwc("2023", yr, None, issues)
        assert rpt["derived_nwc"] == 5_000
        assert rpt["status"] == "ok"

    def test_delta_nwc_direction_mismatch(self):
        # BS: NWC went from 4000 → 5000 (increased, absorbed cash)
        # OCF statement should show ΔNWC as negative (cash absorbed)
        # If OCF shows positive ΔNWC with same BS direction → mismatch
        yr      = _make_year({"change_in_working_cap": 500})   # POSITIVE in OCF (wrong direction)
        prior   = _make_year({"current_assets": 14_000, "current_liabilities": 10_000})  # NWC was 4000
        issues  = []
        rpt     = _derive_and_validate_nwc("2023", yr, prior, issues)
        # BS delta = 5000 - 4000 = +1000 (NWC increased)
        # OCF ΔNWC = +500 (SAME sign as BS delta → mismatch flag)
        assert rpt["direction_mismatch"] is True
        assert any(i["rule_id"] == "HRF-NWC-001" for i in issues)

    def test_correct_ocf_direction_no_flag(self):
        # NWC increased (BS ΔNWC positive) → OCF ΔNWC should be negative (cash absorbed)
        yr    = _make_year({"change_in_working_cap": -500})   # negative in OCF ✓
        prior = _make_year({"current_assets": 14_000, "current_liabilities": 10_000})
        issues = []
        rpt = _derive_and_validate_nwc("2023", yr, prior, issues)
        assert rpt["direction_mismatch"] is False
        nwc_001 = [i for i in issues if i["rule_id"] == "HRF-NWC-001"]
        assert nwc_001 == []

    def test_delta_nwc_implausibly_large(self):
        # ΔNWC = 30000 on revenue of 50000 → 60% > 50% ceiling
        yr = _make_year({"change_in_working_cap": -30_000})
        issues = []
        rpt = _derive_and_validate_nwc("2023", yr, None, issues)
        assert any(i["rule_id"] == "HRF-NWC-002" for i in issues)

    def test_both_ca_cl_zero_flagged(self):
        yr = _make_year({"current_assets": 0, "current_liabilities": 0})
        issues = []
        rpt = _derive_and_validate_nwc("2023", yr, None, issues)
        assert any(i["rule_id"] == "HRF-NWC-003" for i in issues)
        assert rpt["derived_nwc"] is None

    def test_nwc_stored_in_canonical(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        yr  = out["canonical_by_year"]["2023"]
        assert yr.get("derived_nwc") == 5_000


# ─────────────────────────────────────────────────────────────────────────────
# F.  OCF supplemental
# ─────────────────────────────────────────────────────────────────────────────

class TestOcfSupplemental:
    def test_normal_ocf_no_flags(self):
        yr = _make_year()
        issues = []
        flags = _check_ocf_supplemental("2023", yr, issues)
        assert flags == []

    def test_ocf_equals_ni_flagged(self):
        yr = _make_year({"operating_cash_flow": 6_000, "net_income": 6_000})
        issues = []
        flags = _check_ocf_supplemental("2023", yr, issues)
        assert any(i["rule_id"] == "HRF-OCF-001" for i in issues)

    def test_zero_ocf_profitable_company_flagged(self):
        yr = _make_year({"operating_cash_flow": 0})
        issues = []
        flags = _check_ocf_supplemental("2023", yr, issues)
        assert any(i["rule_id"] == "HRF-OCF-002" for i in issues)

    def test_ocf_exceeds_150pct_revenue(self):
        yr = _make_year({"operating_cash_flow": 80_000, "revenue": 50_000})
        issues = []
        flags = _check_ocf_supplemental("2023", yr, issues)
        assert any(i["rule_id"] == "HRF-OCF-003" for i in issues)


# ─────────────────────────────────────────────────────────────────────────────
# G.  Capex supplemental
# ─────────────────────────────────────────────────────────────────────────────

class TestCapexSupplemental:
    def test_normal_capex_no_flags(self):
        years = {
            "2021": _make_year({"capex": 2_500}),
            "2022": _make_year({"capex": 2_800}),
            "2023": _make_year({"capex": 3_000}),
        }
        issues = []
        flags = _check_capex_supplemental(years, "industrial", issues)
        assert flags == []

    def test_zero_capex_2_consecutive_years_flagged(self):
        years = {
            "2022": _make_year({"capex": 0}),
            "2023": _make_year({"capex": 0}),
        }
        issues = []
        flags = _check_capex_supplemental(years, "industrial", issues)
        assert any(i["rule_id"] == "HRF-CPX-001" for i in issues)

    def test_zero_capex_ok_for_asset_light(self):
        years = {
            "2022": _make_year({"capex": 0}),
            "2023": _make_year({"capex": 0}),
        }
        issues = []
        _check_capex_supplemental(years, "asset_light", issues)
        cpx_001 = [i for i in issues if i["rule_id"] == "HRF-CPX-001"]
        assert cpx_001 == []

    def test_capex_exceeds_revenue_critical(self):
        years = {"2023": _make_year({"capex": 60_000, "revenue": 50_000})}
        issues = []
        flags = _check_capex_supplemental(years, "industrial", issues)
        assert any(i["rule_id"] == "HRF-CPX-002" for i in issues)

    def test_negative_capex_convention_handled(self):
        # Negative capex convention — abs() should be used for magnitude check
        years = {"2023": _make_year({"capex": -3_000})}
        issues = []
        _check_capex_supplemental(years, "industrial", issues)
        cpx_002 = [i for i in issues if i["rule_id"] == "HRF-CPX-002"]
        assert cpx_002 == []   # -3000 abs = 3000 < revenue 50000 ✓


# ─────────────────────────────────────────────────────────────────────────────
# H.  EV Multiples rebuild
# ─────────────────────────────────────────────────────────────────────────────

class TestRebuildEvMultiples:
    def test_standard_rebuild(self):
        # MC=60000, debt=15000, leases=2000, cash=5000 → EV = 60000+15000+2000-5000 = 72000
        std = _make_standardised()
        canonical_by_year = std["canonical_by_year"]
        stats = std["stats"]
        issues = []
        rpt = _rebuild_ev_multiples(canonical_by_year, stats, issues)
        assert rpt["rebuilt_ev"] == pytest_approx(72_000, abs=100)

    def test_ev_ebitda_computed(self):
        std = _make_standardised()
        issues = []
        rpt = _rebuild_ev_multiples(std["canonical_by_year"], std["stats"], issues)
        # EV=72000, EBITDA=12000 → 6.0×
        assert rpt["ev_ebitda"] == pytest_approx(6.0, abs=0.1)

    def test_pe_computed(self):
        std = _make_standardised()
        issues = []
        rpt = _rebuild_ev_multiples(std["canonical_by_year"], std["stats"], issues)
        # MC=60000, NI=6000 → P/E = 10×
        assert rpt["pe"] == pytest_approx(10.0, abs=0.1)

    def test_no_market_cap_returns_none(self):
        std = _make_standardised(stats={})
        issues = []
        rpt = _rebuild_ev_multiples(std["canonical_by_year"], {}, issues)
        assert rpt["rebuilt_ev"] is None

    def test_negative_ebitda_no_ev_ebitda(self):
        years = {"2023": _make_year({"ebitda": -1_000})}
        std = _make_standardised(years=years)
        issues = []
        rpt = _rebuild_ev_multiples(std["canonical_by_year"], std["stats"], issues)
        assert rpt["ev_ebitda"] is None

    def test_rebuilt_multiples_in_output(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        mult = out["high_risk_validation"]["rebuilt_multiples"]
        assert mult["rebuilt_ev"] is not None
        assert mult["ev_ebitda"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# Integration — full run
# ─────────────────────────────────────────────────────────────────────────────

class TestRunHighRiskValidation:
    def test_returns_high_risk_key(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        assert "high_risk_validation" in out

    def test_canonical_by_year_updated(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        for yr_key in out["canonical_by_year"]:
            yr = out["canonical_by_year"][yr_key]
            assert "derived_net_debt" in yr, f"derived_net_debt missing for {yr_key}"
            assert "derived_nwc"      in yr, f"derived_nwc missing for {yr_key}"

    def test_smoothed_tax_rate_written_to_all_years(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        for yr_key in out["canonical_by_year"]:
            yr = out["canonical_by_year"][yr_key]
            assert "smoothed_tax_rate" in yr, f"smoothed_tax_rate missing for {yr_key}"

    def test_original_dict_not_mutated(self):
        std = _make_standardised()
        original_gp = std["canonical_by_year"]["2023"]["gross_profit"]
        _ = run_high_risk_validation(std)
        assert std["canonical_by_year"]["2023"]["gross_profit"] == original_gp

    def test_clean_data_minimal_issues(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        summary = out["high_risk_validation"]["summary"]
        # Clean realistic data should produce few or zero criticals
        assert summary["critical_count"] == 0

    def test_bad_data_produces_issues(self):
        years = {
            "2023": _make_year({
                "gross_profit": 50_000,   # == revenue → GP-002 critical
                "long_term_debt": 20_000, # > debt 15000 → ND-001 critical
                "shares_outstanding": 2_000,  # far from NI/EPS implied 1000 → SH-001
            }),
        }
        std = _make_standardised(years=years)
        out = run_high_risk_validation(std)
        summary = out["high_risk_validation"]["summary"]
        assert summary["critical_count"] >= 1

    def test_summary_fields_present(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        summ = out["high_risk_validation"]["summary"]
        assert "critical_count"         in summ
        assert "warning_count"          in summ
        assert "info_count"             in summ
        assert "derived_net_debt_years" in summ
        assert "smoothed_tax_rate"      in summ
        assert "must_review_fields"     in summ

    def test_status_pass_for_clean_data(self):
        std = _make_standardised()
        out = run_high_risk_validation(std)
        assert out["high_risk_validation"]["status"] == "pass"


# ─────────────────────────────────────────────────────────────────────────────
# Approximate equality helper (mirrors pytest.approx without requiring pytest
# for the helper itself — but tests still need pytest to run)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from pytest import approx as pytest_approx
except ImportError:
    def pytest_approx(value, abs=None, rel=None):  # type: ignore
        return value
