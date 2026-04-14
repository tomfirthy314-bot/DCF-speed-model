"""
Unit tests for canonical_fundamentals.py

Run from the DCF tool root:
    python -m pytest canonical_fundamentals/tests/ -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from canonical_fundamentals.canonical_fundamentals import (
    _normalise_capex_for_derivation,
    _derive_ebit_m1,
    _derive_ebit_m2,
    _derive_ebit_m3,
    _choose_best_ebit,
    _derive_fcf_m1,
    _derive_fcf_m2,
    _derive_fcf_m3,
    _choose_best_fcf,
    _derive_ebitda_m1,
    _score_ebit_derivation,
    _score_fcf_derivation,
    _score_scraped_value,
    _compare_and_choose,
    run_canonical_fundamentals,
)


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _make_year(overrides: dict | None = None) -> dict:
    """
    Build a realistic year dict for a large-cap industrial company.
    Revenue ~50B, EBIT ~8B, EBITDA ~12B, OCF ~11B, Capex ~3B, FCF ~8B.
    All values in millions for simplicity (× 1e6).
    """
    base = {
        "revenue":              50_000,
        "gross_profit":         25_000,
        "ebit":                  8_000,   # scraped
        "ebitda":               12_000,   # scraped
        "da":                    4_000,
        "net_income":            6_000,
        "tax_provision":         1_500,
        "interest_expense":        500,
        "pre_tax_income":        7_500,
        "operating_cash_flow":  11_000,
        "capex":                 3_000,   # positive convention
        "free_cash_flow":        8_000,   # scraped
        "change_in_working_cap":  -500,
        "_sources": {
            "ebit":           "yahoo",
            "ebitda":         "yahoo",
            "free_cash_flow": "yahoo",
        },
        "_mapping_confidence": {
            "ebit": "medium",   # operating_income proxy
        },
    }
    if overrides:
        # Shallow merge — for nested dicts, caller must override the whole sub-dict
        base.update(overrides)
    return base


def _make_standardised(years: dict | None = None) -> dict:
    """Build a minimal standardised dict for integration tests."""
    if years is None:
        years = {
            "2023": _make_year(),
            "2022": _make_year({"revenue": 47_000, "ebit": 7_500, "ebitda": 11_500}),
            "2021": _make_year({"revenue": 44_000, "ebit": 7_000, "ebitda": 11_000}),
        }
    return {
        "ticker":           "TEST",
        "company_name":     "Test Co",
        "status":           "pass",
        "blockers":         [],
        "warnings":         [],
        "stats":            {"sector": "Industrials"},
        "canonical_by_year": years,
        "years_available":   sorted(years.keys(), reverse=True),
        "field_mapping_table": [],
        "unmapped_fields":  [],
        "metadata":         {},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Capex sign normalisation
# ─────────────────────────────────────────────────────────────────────────────

class TestNormaliseCapex:
    def test_negative_capex_is_flipped(self):
        pos, note = _normalise_capex_for_derivation(-3_000, None, None)
        assert pos == 3_000
        assert "flipped" in note

    def test_positive_capex_unchanged(self):
        pos, note = _normalise_capex_for_derivation(3_000, None, None)
        assert pos == 3_000
        assert "positive" in note

    def test_cross_check_confirms_positive(self):
        # OCF=11k, FCF=8k → inferred capex=3k; stored=3k → should confirm
        pos, note = _normalise_capex_for_derivation(3_000, 11_000, 8_000)
        assert pos == 3_000
        assert "confirm" in note.lower()

    def test_cross_check_diverges_warning(self):
        # Stored capex=3k but OCF-FCF implies 8k → diverges
        pos, note = _normalise_capex_for_derivation(3_000, 11_000, 3_000)
        assert pos == 3_000
        assert "diverges" in note.lower()

    def test_none_capex_returns_none(self):
        pos, note = _normalise_capex_for_derivation(None, 11_000, 8_000)
        assert pos is None


# ─────────────────────────────────────────────────────────────────────────────
# EBIT derivation methods
# ─────────────────────────────────────────────────────────────────────────────

class TestEbitM2:
    def test_basic_bottom_up(self):
        yr = _make_year()
        res = _derive_ebit_m2(yr)
        assert res["available"] is True
        # EBIT = 6000 + 1500 + 500 = 8000
        assert res["value"] == 8_000

    def test_matches_scraped_value(self):
        yr = _make_year()
        res = _derive_ebit_m2(yr)
        assert abs(res["value"] - yr["ebit"]) < 1

    def test_missing_net_income_returns_unavailable(self):
        yr = _make_year()
        yr.pop("net_income", None)
        res = _derive_ebit_m2(yr)
        assert res["available"] is False
        assert res["value"] is None

    def test_negative_interest_is_flipped(self):
        yr = _make_year({"interest_expense": -500})
        res = _derive_ebit_m2(yr)
        assert res["available"] is True
        assert res["value"] == 8_000   # same result after flip
        assert any("flipped" in w for w in res["warnings"])

    def test_missing_interest_still_partial(self):
        yr = _make_year()
        yr.pop("interest_expense", None)
        res = _derive_ebit_m2(yr)
        assert res["available"] is False  # all three inputs required


class TestEbitM3:
    def test_ebitda_minus_da(self):
        yr = _make_year()
        res = _derive_ebit_m3(yr)
        assert res["available"] is True
        assert res["value"] == 8_000   # 12000 - 4000

    def test_missing_ebitda_unavailable(self):
        yr = _make_year()
        yr.pop("ebitda", None)
        res = _derive_ebit_m3(yr)
        assert res["available"] is False

    def test_negative_da_flipped(self):
        yr = _make_year({"da": -4_000})
        res = _derive_ebit_m3(yr)
        assert res["available"] is True
        assert res["value"] == 8_000
        assert any("flipped" in w for w in res["warnings"])


class TestChooseBestEbit:
    def test_prefers_m2_over_m1(self):
        yr = _make_year()
        m1 = _derive_ebit_m1(yr)
        m2 = _derive_ebit_m2(yr)
        m3 = _derive_ebit_m3(yr)
        best = _choose_best_ebit([m1, m2, m3])
        assert best["method_id"] == "CF-EBIT-M2"

    def test_methods_agree_flag_set(self):
        yr = _make_year()
        m2 = _derive_ebit_m2(yr)
        m3 = _derive_ebit_m3(yr)
        best = _choose_best_ebit([m2, m3])
        assert best["methods_agree"] is True

    def test_methods_disagree_when_far_apart(self):
        yr = _make_year()
        # Change DA so M3 gives a different answer
        yr["da"] = 8_000   # M3 EBIT = 12000 - 8000 = 4000, M2 still 8000
        m2 = _derive_ebit_m2(yr)
        m3 = _derive_ebit_m3(yr)
        best = _choose_best_ebit([m2, m3])
        assert best["methods_agree"] is False
        assert "disagree" in best["agreement_note"]

    def test_no_methods_returns_unavailable(self):
        best = _choose_best_ebit([])
        assert best["available"] is False

    def test_only_m1_falls_back_to_m1(self):
        yr = _make_year()
        yr.pop("net_income", None)
        yr.pop("ebitda", None)
        m1 = _derive_ebit_m1(yr)
        m2 = _derive_ebit_m2(yr)
        m3 = _derive_ebit_m3(yr)
        best = _choose_best_ebit([m1, m2, m3])
        assert best["method_id"] == "CF-EBIT-M1"


# ─────────────────────────────────────────────────────────────────────────────
# FCF derivation methods
# ─────────────────────────────────────────────────────────────────────────────

class TestFcfM1:
    def test_ocf_minus_capex(self):
        yr  = _make_year()
        res = _derive_fcf_m1(yr, 3_000)
        assert res["available"] is True
        assert res["value"] == 8_000   # 11000 - 3000

    def test_missing_ocf_unavailable(self):
        yr = _make_year()
        yr.pop("operating_cash_flow", None)
        res = _derive_fcf_m1(yr, 3_000)
        assert res["available"] is False

    def test_none_capex_unavailable(self):
        yr  = _make_year()
        res = _derive_fcf_m1(yr, None)
        assert res["available"] is False


class TestFcfM2:
    def test_nopat_formula(self):
        yr  = _make_year()
        # tax_rate = 1500/7500 = 20%
        # NOPAT = 8000 × 0.80 = 6400
        # FCF = 6400 + 4000 - 3000 - (-500) = 7900
        res = _derive_fcf_m2(yr, 3_000, 8_000)
        assert res["available"] is True
        assert abs(res["value"] - 7_900) < 1

    def test_missing_capex_unavailable(self):
        yr  = _make_year()
        res = _derive_fcf_m2(yr, None, 8_000)
        assert res["available"] is False


class TestFcfM3:
    def test_ebit_times_nopat(self):
        yr  = _make_year()
        # Same formula as M2 when EBIT provided directly
        # tax_rate = 20%, NOPAT = 6400, FCF = 6400 + 4000 - 3000 - (-500) = 7900
        res = _derive_fcf_m3(yr, 3_000, 8_000)
        assert res["available"] is True
        assert abs(res["value"] - 7_900) < 1

    def test_no_ebit_unavailable(self):
        yr  = _make_year()
        res = _derive_fcf_m3(yr, 3_000, None)
        assert res["available"] is False


class TestChooseBestFcf:
    def test_prefers_m1(self):
        yr  = _make_year()
        m1  = _derive_fcf_m1(yr, 3_000)
        m2  = _derive_fcf_m2(yr, 3_000, 8_000)
        best = _choose_best_fcf([m1, m2])
        assert best["method_id"] == "CF-FCF-M1"

    def test_falls_back_to_m2_when_m1_unavailable(self):
        yr = _make_year()
        yr.pop("operating_cash_flow", None)
        m1 = _derive_fcf_m1(yr, 3_000)
        m2 = _derive_fcf_m2(yr, 3_000, 8_000)
        best = _choose_best_fcf([m1, m2])
        assert best["method_id"] == "CF-FCF-M2"


# ─────────────────────────────────────────────────────────────────────────────
# EBITDA derivation
# ─────────────────────────────────────────────────────────────────────────────

class TestEbitdaDerivation:
    def test_ebit_plus_da(self):
        yr  = _make_year()
        res = _derive_ebitda_m1(yr, 8_000)
        assert res["available"] is True
        assert res["value"] == 12_000   # 8000 + 4000

    def test_no_ebit_unavailable(self):
        yr  = _make_year()
        res = _derive_ebitda_m1(yr, None)
        assert res["available"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Confidence scoring
# ─────────────────────────────────────────────────────────────────────────────

class TestConfidenceScoring:
    def test_derived_ebit_high_when_methods_agree(self):
        yr   = _make_year()
        m2   = _derive_ebit_m2(yr)
        m3   = _derive_ebit_m3(yr)
        best = _choose_best_ebit([m2, m3])
        score = _score_ebit_derivation(best, yr, "Industrials")
        # Should be high: agree bonus + margin plausible + EBIT ≤ EBITDA
        assert score >= 85

    def test_ebit_confidence_drops_when_exceeds_ebitda(self):
        yr = _make_year()
        yr["ebitda"] = 5_000   # EBIT=8000 > EBITDA=5000 → penalty
        m2 = _derive_ebit_m2(yr)
        best = _choose_best_ebit([m2])
        score = _score_ebit_derivation(best, yr, "Industrials")
        # Should lose ebit_exceeds_ebitda penalty
        assert score < 70

    def test_scraped_fcf_penalised(self):
        score = _score_scraped_value(8_000, "yahoo", "free_cash_flow")
        # Base 50 + yahoo 0 - fcf_penalty 15 = 35
        assert score == 35

    def test_scraped_edgar_boosted(self):
        score = _score_scraped_value(8_000, "edgar", "ebit")
        # Base 50 + edgar 15 - ebit 5 = 60
        assert score == 60

    def test_unavailable_derivation_scores_zero(self):
        yr = _make_year()
        yr.pop("net_income", None)
        m2 = _derive_ebit_m2(yr)
        best = _choose_best_ebit([m2])
        score = _score_ebit_derivation(best, yr, None)
        assert score == 0


# ─────────────────────────────────────────────────────────────────────────────
# Comparison and selection
# ─────────────────────────────────────────────────────────────────────────────

class TestCompareAndChoose:
    def test_chooses_higher_confidence(self):
        comp = _compare_and_choose(
            derived_val=8_100, derived_conf=85,
            scraped_val=8_000, scraped_conf=50,
            field="ebit", year="2023", method_id="CF-EBIT-M2",
        )
        assert comp["chosen_source"] == "derived"
        assert comp["chosen_value"]  == 8_100

    def test_falls_back_to_scraped_when_derivation_fails(self):
        comp = _compare_and_choose(
            derived_val=None, derived_conf=0,
            scraped_val=8_000, scraped_conf=50,
            field="ebit", year="2023", method_id="none",
        )
        assert comp["chosen_source"] == "scraped"
        assert comp["chosen_value"]  == 8_000

    def test_uses_derived_when_scraped_absent(self):
        comp = _compare_and_choose(
            derived_val=8_000, derived_conf=80,
            scraped_val=None, scraped_conf=0,
            field="ebit", year="2023", method_id="CF-EBIT-M2",
        )
        assert comp["chosen_source"] == "derived"

    def test_difference_pct_computed(self):
        comp = _compare_and_choose(
            derived_val=8_400, derived_conf=80,
            scraped_val=8_000, scraped_conf=50,
            field="ebit", year="2023", method_id="CF-EBIT-M2",
        )
        assert comp["difference_pct"] == 5.0

    def test_both_none_returns_none_chosen(self):
        comp = _compare_and_choose(
            derived_val=None, derived_conf=0,
            scraped_val=None, scraped_conf=0,
            field="ebit", year="2023", method_id="none",
        )
        assert comp["chosen_value"] is None
        assert comp["low_confidence_warning"] is True

    def test_low_confidence_flag_when_both_below_50(self):
        comp = _compare_and_choose(
            derived_val=8_000, derived_conf=40,
            scraped_val=8_000, scraped_conf=30,
            field="ebit", year="2023", method_id="CF-EBIT-M2",
        )
        assert comp["low_confidence_warning"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Integration test — full run
# ─────────────────────────────────────────────────────────────────────────────

class TestRunCanonicalFundamentals:
    def test_returns_augmented_dict(self):
        std = _make_standardised()
        out = run_canonical_fundamentals(std)
        assert "canonical_fundamentals" in out
        assert "canonical_by_year" in out

    def test_ebit_updated_in_canonical(self):
        std = _make_standardised()
        out = run_canonical_fundamentals(std)
        # EBIT should be updated (derived value chosen or scraped, but not absent)
        for year in out["canonical_by_year"]:
            yr = out["canonical_by_year"][year]
            assert yr.get("ebit") is not None, f"EBIT missing for {year}"

    def test_fcf_updated_in_canonical(self):
        std = _make_standardised()
        out = run_canonical_fundamentals(std)
        for year in out["canonical_by_year"]:
            yr = out["canonical_by_year"][year]
            assert yr.get("free_cash_flow") is not None, f"FCF missing for {year}"

    def test_derivation_metadata_stored(self):
        std = _make_standardised()
        out = run_canonical_fundamentals(std)
        for year in out["canonical_by_year"]:
            yr = out["canonical_by_year"][year]
            assert "_ebit_derivation" in yr
            assert "_free_cash_flow_derivation" in yr
            assert "_ebitda_derivation" in yr

    def test_original_dict_not_mutated(self):
        std = _make_standardised()
        original_ebit = std["canonical_by_year"]["2023"]["ebit"]
        _ = run_canonical_fundamentals(std)
        # Original should be unchanged (deep copy of canonical_by_year)
        assert std["canonical_by_year"]["2023"]["ebit"] == original_ebit

    def test_summary_counts_correct(self):
        std = _make_standardised()
        out = run_canonical_fundamentals(std)
        summ = out["canonical_fundamentals"]["summary"]
        assert summ["years_processed"] == 3
        # With consistent data, derivation should win most years
        assert summ["ebit_used_derived"] + summ["ebit_used_scraped"] == 3

    def test_negative_capex_handled(self):
        # Capex reported as negative (cash outflow convention) — should not break FCF
        year = _make_year({"capex": -3_000})
        std  = _make_standardised({"2023": year})
        out  = run_canonical_fundamentals(std)
        yr   = out["canonical_by_year"]["2023"]
        # FCF should be positive ~8000 (OCF=11000 - |capex|=3000)
        assert yr.get("free_cash_flow") is not None
        fcf = yr["free_cash_flow"]
        assert 7_000 < fcf < 9_000, f"Unexpected FCF with negative capex: {fcf}"

    def test_missing_ocf_falls_back_to_m2_or_m3(self):
        year = _make_year()
        year.pop("operating_cash_flow", None)
        std  = _make_standardised({"2023": year})
        out  = run_canonical_fundamentals(std)
        yr   = out["canonical_by_year"]["2023"]
        # FCF M1 unavailable — should fall back to M2 or M3
        deriv = yr.get("_free_cash_flow_derivation", {})
        assert deriv.get("method_id") in ("CF-FCF-M2", "CF-FCF-M3", "none", None)

    def test_ebit_exceeds_ebitda_flagged(self):
        # Make EBITDA very low so EBIT derivation looks suspicious
        year = _make_year({"ebitda": 4_000})   # EBIT derived ≈ 8000 > EBITDA 4000
        std  = _make_standardised({"2023": year})
        out  = run_canonical_fundamentals(std)
        comp = out["canonical_fundamentals"]["year_reports"]["2023"]["ebit_comparison"]
        # Derived confidence should be lower
        assert comp["derived_confidence"] < 70
