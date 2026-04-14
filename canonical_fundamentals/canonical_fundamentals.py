"""
Canonical Fundamentals Engine  —  Stage 2.5

Derives canonical EBIT, EBITDA, and Free Cash Flow from primitive validated
financial statement fields rather than trusting pre-calculated website values.

Problem
-------
Free financial websites (Yahoo Finance, Macrotrends, etc.) frequently publish
incorrect or inconsistently defined values for derived metrics:
  - EBIT may be adjusted EBIT, operating profit excluding interest, or plain wrong
  - FCF often has sign errors on Capex, or mixes in M&A / lease payments
  - EBITDA may be "adjusted" (stock comp, restructuring excluded) vs reported
  - TTM and annual periods can be silently mixed

This engine:
  1. Derives EBIT, EBITDA, and FCF from primitive income statement / cash flow fields
  2. Scores confidence for both derived and scraped versions
  3. Chooses the higher-confidence value for each year
  4. Updates canonical_by_year so all downstream stages use clean values
  5. Preserves a full audit trail (raw scraped, derived, chosen, reason)

Pipeline position
-----------------
  Stage 2   → run_standardiser        → standardised dict
  Stage 2.5 → run_canonical_fundamentals → augmented standardised dict  ← HERE
  Stage 3   → run_validator

Input
-----
  standardised dict (output of run_standardiser)

Output
------
  Same structure as standardised, with:
    - canonical_by_year updated with chosen EBIT / EBITDA / FCF values
    - Per-field derivation metadata stored in _<field>_derivation keys
    - Top-level "canonical_fundamentals" key containing the full audit report

Rule IDs
--------
  CF-EBIT-M1   Direct operating income (scraped, high-confidence mapping)
  CF-EBIT-M2   Bottom-up: Net Income + Tax Expense + Interest Expense
  CF-EBIT-M3   Top-down:  EBITDA - D&A
  CF-EBITDA-M1 Derived:   Chosen EBIT + D&A
  CF-FCF-M1    OCF minus Capex  (preferred)
  CF-FCF-M2    NOPAT + D&A - Capex - ΔNWC
  CF-FCF-M3    EBIT × (1-t) + D&A - Capex - ΔNWC  (fallback)
  CF-VAL-001   EBIT ≤ EBITDA accounting relationship
  CF-VAL-002   EBIT margin within sector-plausible range
  CF-VAL-003   Multiple derivation methods agree within tolerance
  CF-VAL-004   FCF / EBITDA conversion ratio plausible
  CF-VAL-005   FCF does not exceed revenue × 1.5
  CF-VAL-006   Capex sign convention detection
  CF-VAL-007   Interest expense sign correction
"""

from __future__ import annotations

import copy
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Base confidence scores (0–100)
_DERIVED_BASE = 75   # starting score for a value derived from primitives
_SCRAPED_BASE = 50   # starting score for a pre-calculated website value

# Two methods "agree" if they differ by less than this
_METHOD_AGREE_TOL = 0.10   # 10%

# EBIT may exceed EBITDA by this small tolerance before being flagged
# (rounding / different D&A sources can cause tiny inversions)
_EBIT_EBITDA_CEIL_TOL = 0.03

# FCF / EBITDA plausible range — outside this triggers a confidence deduction
_FCF_EBITDA_MIN = -0.60   # heavily capex-intensive
_FCF_EBITDA_MAX =  1.30   # very asset-light (royalty co. etc.)

# EBIT margin plausible bounds by sector  (fraction of revenue: min, max)
_EBIT_MARGIN_BOUNDS: dict[str, tuple[float, float]] = {
    "technology":             (-0.15, 0.55),
    "software":               (-0.15, 0.60),
    "healthcare":             (-0.10, 0.40),
    "pharma":                 (-0.10, 0.45),
    "biotech":                (-0.30, 0.50),
    "consumer defensive":     (-0.05, 0.28),
    "consumer staples":       (-0.05, 0.28),
    "consumer discretionary": (-0.15, 0.25),
    "consumer cyclical":      (-0.15, 0.25),
    "industrials":            (-0.05, 0.28),
    "energy":                 (-0.20, 0.40),
    "basic materials":        (-0.10, 0.35),
    "materials":              (-0.10, 0.35),
    "financials":             (-0.10, 0.50),
    "utilities":              (-0.05, 0.35),
    "communication services": (-0.05, 0.45),
    "real estate":            (-0.05, 0.50),
    "default":                (-0.30, 0.60),
}

# Confidence adjustment amounts (additive deltas)
_ADJ = {
    "methods_agree":           +15,
    "methods_disagree":        -20,
    "ebit_ebitda_ok":          +10,
    "ebit_exceeds_ebitda":     -30,
    "margin_plausible":        +8,
    "margin_implausible":      -25,
    "source_edgar":            +15,
    "source_yahoo":             +0,
    "source_macrotrends":       -5,
    "source_scraped_generic":  -10,
    "source_primitive_high":   +15,
    "source_primitive_medium":  +5,
    "fcf_conversion_ok":        +8,
    "fcf_conversion_bad":      -15,
    "fcf_exceeds_revenue":     -20,
    "missing_input":           -15,
    "sign_ambiguous":          -10,
    "per_warning":              -4,
    "m1_fcf_bonus":             +5,
    "m3_fcf_penalty":          -10,
}


# ─────────────────────────────────────────────────────────────────────────────
# Micro helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f   # NaN → None
    except (TypeError, ValueError):
        return None


def _pct_diff(a: float, b: float) -> float | None:
    """Percentage difference of a relative to b. Returns None if b ≈ 0."""
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


def _clamp(val: int, lo: int = 0, hi: int = 100) -> int:
    return max(lo, min(hi, val))


def _sector_ebit_bounds(sector: str | None) -> tuple[float, float]:
    if not sector:
        return _EBIT_MARGIN_BOUNDS["default"]
    sl = sector.lower()
    for key, bounds in _EBIT_MARGIN_BOUNDS.items():
        if key in sl:
            return bounds
    return _EBIT_MARGIN_BOUNDS["default"]


def _tax_rate_from_year(year_data: dict) -> tuple[float, str]:
    """
    Derive effective tax rate for a year.
    Returns (rate, note). Falls back to 25% default.
    """
    pre_tax = _safe(year_data.get("pre_tax_income"))
    tax     = _safe(year_data.get("tax_provision"))

    if pre_tax is not None and tax is not None and abs(pre_tax) > 1:
        rate = tax / pre_tax
        if 0.0 <= rate <= 0.55:
            return rate, f"derived from statements ({rate:.1%})"
        # Clamp and warn
        rate = max(0.0, min(rate, 0.55))
        return rate, f"clamped to {rate:.1%} (raw was {tax/pre_tax:.1%})"

    return 0.25, "default 25% (insufficient data)"


# ─────────────────────────────────────────────────────────────────────────────
# CF-VAL-006  Capex sign normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_capex_for_derivation(
    capex_raw: float | None,
    ocf:       float | None,
    fcf_sc:    float | None,
) -> tuple[float | None, str]:
    """
    Return (positive_capex_amount, evidence_note) for use in FCF arithmetic.

    Capex is a cash *outflow* and should be subtracted in:
        FCF = OCF - Capex

    Some sources store it as a positive number (purchase amount).
    Others store it as a negative number (cash flow statement convention).

    This function returns a positive number regardless of input convention.
    It does NOT modify the stored canonical_by_year capex field.

    Detection logic
    ---------------
    1. Negative input → almost certainly cash-outflow convention → flip to positive.
    2. Positive input → assumed to already be purchase-amount convention.
    3. Cross-check: if OCF and scraped FCF are available, infer expected capex
       and verify sign consistency.
    """
    if capex_raw is None:
        return None, "capex missing"

    if capex_raw < 0:
        pos = abs(capex_raw)
        note = "CF-VAL-006: negative raw capex flipped to positive (cash-outflow convention detected)"
        # Cross-check
        if ocf is not None and fcf_sc is not None:
            inferred = ocf - fcf_sc
            if abs(inferred) > 1:
                pd = _pct_diff(pos, inferred)
                if pd is not None and pd < 20:
                    note += f" — cross-check confirms within {pd:.0f}%"
                else:
                    note += f" — cross-check diverges (OCF-FCF implies {_fmt(inferred)})"
        return pos, note

    # Positive capex — standard Yahoo convention
    note = "CF-VAL-006: positive capex convention (assumed purchase-amount)"
    if ocf is not None and fcf_sc is not None:
        inferred = ocf - fcf_sc
        if abs(inferred) > 1:
            pd = _pct_diff(capex_raw, inferred)
            if pd is not None and pd < 20:
                note += f" — cross-check confirms within {pd:.0f}%"
            elif pd is not None:
                note += (
                    f" — cross-check diverges: stored={_fmt(capex_raw)} "
                    f"OCF-FCF implies={_fmt(inferred)} ({pd:.0f}% diff)"
                )
    return capex_raw, note


# ─────────────────────────────────────────────────────────────────────────────
# EBIT derivation
# ─────────────────────────────────────────────────────────────────────────────

def _derive_ebit_m1(year_data: dict) -> dict:
    """
    CF-EBIT-M1: Accept the scraped/mapped value only if it arrived via a
    high-confidence direct mapping.  If mapping confidence is 'medium' (i.e.
    it came from 'operating_income' rather than a labelled 'ebit' line), we
    still capture the value but mark the result degraded so the scorer
    discounts it appropriately.
    """
    val  = _safe(year_data.get("ebit"))
    conf = (year_data.get("_mapping_confidence") or {}).get("ebit", "")
    src  = (year_data.get("_sources") or {}).get("ebit", "unknown")

    if val is None:
        return {
            "method_id": "CF-EBIT-M1", "value": None, "available": False,
            "inputs": {}, "warnings": ["No ebit present in canonical data"],
        }

    warnings = []
    degraded = False
    if conf != "high":
        warnings.append(
            f"CF-EBIT-M1: Mapping confidence is '{conf}' (source: {src}) — "
            "not treating as a tier-1 direct report; derivation methods preferred"
        )
        degraded = True

    return {
        "method_id": "CF-EBIT-M1",
        "value":     val,
        "available": True,
        "degraded":  degraded,
        "inputs":    {"ebit_raw": val, "source": src, "mapping_confidence": conf},
        "warnings":  warnings,
    }


def _derive_ebit_m2(year_data: dict) -> dict:
    """
    CF-EBIT-M2: Bottom-up derivation.
    EBIT = Net Income + Tax Provision + Interest Expense
    """
    ni    = _safe(year_data.get("net_income"))
    tax   = _safe(year_data.get("tax_provision"))
    int_e = _safe(year_data.get("interest_expense"))

    warnings = []
    missing  = [f for f, v in [("net_income", ni), ("tax_provision", tax),
                                ("interest_expense", int_e)] if v is None]
    if missing:
        warnings.append(f"CF-EBIT-M2: Missing inputs: {', '.join(missing)}")

    if ni is None or tax is None or int_e is None:
        return {
            "method_id": "CF-EBIT-M2", "value": None, "available": False,
            "inputs": {"net_income": ni, "tax_provision": tax, "interest_expense": int_e},
            "warnings": warnings,
        }

    # Interest expense should be a positive cost.
    # Some sources report it as a negative number (cash flow statement sign).
    if int_e < 0:
        int_e = abs(int_e)
        warnings.append("CF-VAL-007: Interest expense was negative — sign flipped for bottom-up EBIT")

    ebit = ni + tax + int_e
    return {
        "method_id": "CF-EBIT-M2",
        "value":     ebit,
        "available": True,
        "inputs":    {"net_income": ni, "tax_provision": tax, "interest_expense": int_e},
        "warnings":  warnings,
    }


def _derive_ebit_m3(year_data: dict) -> dict:
    """
    CF-EBIT-M3: Top-down derivation.
    EBIT = EBITDA - D&A
    """
    ebitda = _safe(year_data.get("ebitda"))
    da     = _safe(year_data.get("da"))

    warnings = []
    if ebitda is None:
        warnings.append("CF-EBIT-M3: ebitda missing")
    if da is None:
        warnings.append("CF-EBIT-M3: da (depreciation & amortisation) missing")

    if ebitda is None or da is None:
        return {
            "method_id": "CF-EBIT-M3", "value": None, "available": False,
            "inputs": {"ebitda": ebitda, "da": da}, "warnings": warnings,
        }

    if da < 0:
        da = abs(da)
        warnings.append("CF-EBIT-M3: D&A was negative — sign flipped")

    ebit = ebitda - da
    return {
        "method_id": "CF-EBIT-M3",
        "value":     ebit,
        "available": True,
        "inputs":    {"ebitda": ebitda, "da": da},
        "warnings":  warnings,
    }


def _choose_best_ebit(methods: list[dict]) -> dict:
    """
    Given results from M1/M2/M3, choose the best single derivation.

    Priority logic
    --------------
    - If M2 and M3 are both available, prefer M2 (uses more primitives)
      but report their agreement/disagreement.
    - If only one method is available, use it.
    - An undegraded M1 (high-confidence direct report) wins only over a
      disagreeing M2/M3 pair.
    """
    avail = [m for m in methods if m.get("available") and m.get("value") is not None]
    if not avail:
        return {
            "method_id": "none", "value": None, "available": False,
            "inputs": {}, "warnings": ["No EBIT derivation method available"],
            "agreement_note": "no methods available", "methods_agree": False, "all_methods": [],
        }

    m1 = next((m for m in avail if m["method_id"] == "CF-EBIT-M1"), None)
    m2 = next((m for m in avail if m["method_id"] == "CF-EBIT-M2"), None)
    m3 = next((m for m in avail if m["method_id"] == "CF-EBIT-M3"), None)

    agreement_note = "single method available"
    methods_agree  = False

    # Check M2 vs M3 agreement
    if m2 and m3:
        pd = _pct_diff(m2["value"], m3["value"])
        if pd is not None:
            if pd < _METHOD_AGREE_TOL * 100:
                agreement_note = f"CF-VAL-003: M2 and M3 agree within {pd:.1f}%"
                methods_agree  = True
            else:
                agreement_note = (
                    f"CF-VAL-003: M2 ({_fmt(m2['value'])}) and M3 ({_fmt(m3['value'])}) "
                    f"disagree by {pd:.1f}%"
                )
    elif m1 and m2:
        pd = _pct_diff(m1["value"], m2["value"])
        if pd is not None:
            if pd < _METHOD_AGREE_TOL * 100:
                agreement_note = f"CF-VAL-003: M1 and M2 agree within {pd:.1f}%"
                methods_agree  = True
            else:
                agreement_note = (
                    f"CF-VAL-003: M1 ({_fmt(m1['value'])}) and M2 ({_fmt(m2['value'])}) "
                    f"disagree by {pd:.1f}%"
                )

    # Choose: M2 preferred; M3 if M2 unavailable; M1 if neither.
    # Exception: pristine M1 (undegraded) beats a single M2 or M3 if they agree.
    m1_pristine = m1 and not m1.get("degraded")
    if m2:
        chosen = m2
    elif m3:
        chosen = m3
    elif m1_pristine:
        chosen = m1
    else:
        chosen = avail[0]

    return {
        **chosen,
        "agreement_note": agreement_note,
        "methods_agree":  methods_agree,
        "all_methods":    avail,
    }


# ─────────────────────────────────────────────────────────────────────────────
# EBITDA derivation
# ─────────────────────────────────────────────────────────────────────────────

def _derive_ebitda_m1(year_data: dict, chosen_ebit: float | None) -> dict:
    """
    CF-EBITDA-M1: EBITDA = Chosen EBIT + D&A
    """
    da = _safe(year_data.get("da"))
    warnings = []

    if chosen_ebit is None:
        warnings.append("CF-EBITDA-M1: chosen EBIT not available")
    if da is None:
        warnings.append("CF-EBITDA-M1: da missing")

    if chosen_ebit is None or da is None:
        return {
            "method_id": "CF-EBITDA-M1", "value": None, "available": False,
            "inputs": {"chosen_ebit": chosen_ebit, "da": da}, "warnings": warnings,
        }

    if da < 0:
        da = abs(da)
        warnings.append("CF-EBITDA-M1: D&A was negative — sign flipped")

    return {
        "method_id": "CF-EBITDA-M1",
        "value":     chosen_ebit + da,
        "available": True,
        "inputs":    {"chosen_ebit": chosen_ebit, "da": da},
        "warnings":  warnings,
    }


# ─────────────────────────────────────────────────────────────────────────────
# FCF derivation
# ─────────────────────────────────────────────────────────────────────────────

def _derive_fcf_m1(year_data: dict, capex_pos: float | None) -> dict:
    """
    CF-FCF-M1: FCF = Operating Cash Flow - Capex  (preferred method)
    """
    ocf = _safe(year_data.get("operating_cash_flow"))
    warnings = []

    if ocf is None:
        warnings.append("CF-FCF-M1: operating_cash_flow missing")
    if capex_pos is None:
        warnings.append("CF-FCF-M1: capex missing or not normalisable")

    if ocf is None or capex_pos is None:
        return {
            "method_id": "CF-FCF-M1", "value": None, "available": False,
            "inputs": {"operating_cash_flow": ocf, "capex": capex_pos}, "warnings": warnings,
        }

    return {
        "method_id": "CF-FCF-M1",
        "value":     ocf - capex_pos,
        "available": True,
        "inputs":    {"operating_cash_flow": ocf, "capex": capex_pos},
        "warnings":  warnings,
    }


def _derive_fcf_m2(year_data: dict, capex_pos: float | None, ebit_val: float | None) -> dict:
    """
    CF-FCF-M2: FCF = NOPAT + D&A - Capex - ΔNWC
    Uses chosen EBIT to derive NOPAT.
    """
    da  = _safe(year_data.get("da"))
    nwc = _safe(year_data.get("change_in_working_cap"))
    warnings = []

    if capex_pos is None:
        return {
            "method_id": "CF-FCF-M2", "value": None, "available": False,
            "inputs": {}, "warnings": ["CF-FCF-M2: capex missing"],
        }

    tax_rate, tax_note = _tax_rate_from_year(year_data)
    warnings.append(f"CF-FCF-M2: tax rate {tax_note}")

    # Derive NOPAT
    if ebit_val is not None:
        nopat = ebit_val * (1 - tax_rate)
    else:
        # Fallback: use net income + after-tax interest as NOPAT proxy
        ni    = _safe(year_data.get("net_income"))
        int_e = _safe(year_data.get("interest_expense"))
        if ni is None:
            return {
                "method_id": "CF-FCF-M2", "value": None, "available": False,
                "inputs": {}, "warnings": ["CF-FCF-M2: EBIT and net_income both missing"],
            }
        int_e = abs(int_e) if int_e is not None else 0.0
        nopat = ni + int_e * (1 - tax_rate)
        warnings.append("CF-FCF-M2: EBIT unavailable — using ni + after-tax interest as NOPAT proxy")

    if da is None:
        warnings.append("CF-FCF-M2: D&A missing — omitted from formula")
        da = 0.0
    elif da < 0:
        da = abs(da)
        warnings.append("CF-FCF-M2: D&A was negative — sign flipped")

    # NWC change: positive ΔNWC = working capital build = cash absorbed = subtract
    nwc_adj = 0.0
    if nwc is not None:
        nwc_adj = nwc
        warnings.append(f"CF-FCF-M2: NWC change applied ({_fmt(nwc)})")
    else:
        warnings.append("CF-FCF-M2: change_in_working_cap missing — omitted")

    fcf = nopat + da - capex_pos - nwc_adj
    return {
        "method_id": "CF-FCF-M2",
        "value":     fcf,
        "available": True,
        "inputs":    {
            "nopat": nopat, "da": da, "capex": capex_pos,
            "nwc_change": nwc_adj, "tax_rate": tax_rate,
        },
        "warnings":  warnings,
    }


def _derive_fcf_m3(year_data: dict, capex_pos: float | None, ebit_val: float | None) -> dict:
    """
    CF-FCF-M3: FCF = EBIT × (1-t) + D&A - Capex - ΔNWC  (fallback when OCF missing)
    Requires EBIT — use only when OCF is unavailable.
    """
    if ebit_val is None:
        return {
            "method_id": "CF-FCF-M3", "value": None, "available": False,
            "inputs": {}, "warnings": ["CF-FCF-M3: EBIT required — not available"],
        }
    if capex_pos is None:
        return {
            "method_id": "CF-FCF-M3", "value": None, "available": False,
            "inputs": {}, "warnings": ["CF-FCF-M3: capex missing"],
        }

    da  = _safe(year_data.get("da"))
    nwc = _safe(year_data.get("change_in_working_cap"))
    warnings = []

    tax_rate, tax_note = _tax_rate_from_year(year_data)
    warnings.append(f"CF-FCF-M3: tax rate {tax_note}")

    nopat = ebit_val * (1 - tax_rate)

    if da is None:
        warnings.append("CF-FCF-M3: D&A missing — omitted")
        da = 0.0
    elif da < 0:
        da = abs(da)
        warnings.append("CF-FCF-M3: D&A negative — sign flipped")

    nwc_adj = nwc if nwc is not None else 0.0
    if nwc is None:
        warnings.append("CF-FCF-M3: NWC change missing — omitted")

    fcf = nopat + da - capex_pos - nwc_adj
    return {
        "method_id": "CF-FCF-M3",
        "value":     fcf,
        "available": True,
        "inputs":    {
            "ebit": ebit_val, "tax_rate": tax_rate, "nopat": nopat,
            "da": da, "capex": capex_pos, "nwc_change": nwc_adj,
        },
        "warnings":  warnings,
    }


def _choose_best_fcf(methods: list[dict]) -> dict:
    """
    Prefer CF-FCF-M1 (OCF-Capex) — it uses the most direct cash measurement.
    Fall back to M2, then M3.
    Check agreement between available methods.
    """
    avail = [m for m in methods if m.get("available") and m.get("value") is not None]
    if not avail:
        return {
            "method_id": "none", "value": None, "available": False,
            "inputs": {}, "warnings": ["No FCF derivation method available"],
            "agreement_note": "no methods available", "methods_agree": False, "all_methods": [],
        }

    m1 = next((m for m in avail if m["method_id"] == "CF-FCF-M1"), None)
    m2 = next((m for m in avail if m["method_id"] == "CF-FCF-M2"), None)
    m3 = next((m for m in avail if m["method_id"] == "CF-FCF-M3"), None)

    agreement_note = "single method available"
    methods_agree  = False

    if m1 and m2:
        pd = _pct_diff(m1["value"], m2["value"])
        if pd is not None:
            if pd < _METHOD_AGREE_TOL * 100:
                agreement_note = f"CF-VAL-003: M1 and M2 agree within {pd:.1f}%"
                methods_agree  = True
            else:
                agreement_note = (
                    f"CF-VAL-003: M1 ({_fmt(m1['value'])}) and M2 ({_fmt(m2['value'])}) "
                    f"disagree by {pd:.1f}%"
                )
    elif m2 and m3:
        pd = _pct_diff(m2["value"], m3["value"])
        if pd is not None:
            if pd < _METHOD_AGREE_TOL * 100:
                agreement_note = f"CF-VAL-003: M2 and M3 agree within {pd:.1f}%"
                methods_agree  = True

    chosen = m1 or m2 or m3 or avail[0]
    return {
        **chosen,
        "agreement_note": agreement_note,
        "methods_agree":  methods_agree,
        "all_methods":    avail,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Confidence scoring
# ─────────────────────────────────────────────────────────────────────────────

def _score_ebit_derivation(derivation: dict, year_data: dict, sector: str | None) -> int:
    """Score a derived EBIT result 0–100."""
    if not derivation.get("available") or derivation.get("value") is None:
        return 0

    val   = derivation["value"]
    score = _DERIVED_BASE

    method_id = derivation.get("method_id", "")

    # M1 direct report — add source quality adjustment
    if method_id == "CF-EBIT-M1":
        conf = (year_data.get("_mapping_confidence") or {}).get("ebit", "")
        if conf == "high":
            score += _ADJ["source_primitive_high"]
        elif conf == "medium":
            score += _ADJ["source_primitive_medium"]
        if derivation.get("degraded"):
            score -= 10

    # Agreement between methods
    if derivation.get("methods_agree"):
        score += _ADJ["methods_agree"]
    elif "disagree" in derivation.get("agreement_note", ""):
        score += _ADJ["methods_disagree"]

    # CF-VAL-001: EBIT ≤ EBITDA
    ebitda = _safe(year_data.get("ebitda"))
    if ebitda is not None and abs(ebitda) > 1:
        if val <= ebitda * (1 + _EBIT_EBITDA_CEIL_TOL):
            score += _ADJ["ebit_ebitda_ok"]
        else:
            score += _ADJ["ebit_exceeds_ebitda"]

    # CF-VAL-002: EBIT margin plausibility
    rev = _safe(year_data.get("revenue"))
    if rev and abs(rev) > 1:
        margin = val / rev
        lo, hi = _sector_ebit_bounds(sector)
        if lo <= margin <= hi:
            score += _ADJ["margin_plausible"]
        else:
            score += _ADJ["margin_implausible"]

    # Warning count penalty
    score += len(derivation.get("warnings", [])) * _ADJ["per_warning"]

    return _clamp(score)


def _score_fcf_derivation(derivation: dict, year_data: dict, sector: str | None) -> int:
    """Score a derived FCF result 0–100."""
    if not derivation.get("available") or derivation.get("value") is None:
        return 0

    val   = derivation["value"]
    score = _DERIVED_BASE

    method_id = derivation.get("method_id", "")
    if method_id == "CF-FCF-M1":
        score += _ADJ["m1_fcf_bonus"]
    elif method_id == "CF-FCF-M3":
        score += _ADJ["m3_fcf_penalty"]

    # Method agreement
    if derivation.get("methods_agree"):
        score += _ADJ["methods_agree"]
    elif "disagree" in derivation.get("agreement_note", ""):
        score += _ADJ["methods_disagree"]

    # CF-VAL-004: FCF / EBITDA conversion plausibility
    ebitda = _safe(year_data.get("ebitda"))
    if ebitda is not None and ebitda > 1:
        ratio = val / ebitda
        if _FCF_EBITDA_MIN <= ratio <= _FCF_EBITDA_MAX:
            score += _ADJ["fcf_conversion_ok"]
        else:
            score += _ADJ["fcf_conversion_bad"]

    # CF-VAL-005: FCF should not exceed 1.5× revenue
    rev = _safe(year_data.get("revenue"))
    if rev and abs(rev) > 1 and val > rev * 1.5:
        score += _ADJ["fcf_exceeds_revenue"]

    # Warning count penalty
    score += len(derivation.get("warnings", [])) * _ADJ["per_warning"]

    return _clamp(score)


def _score_scraped_value(scraped_val: float | None, source: str, field: str) -> int:
    """Score a scraped/pre-calculated value 0–100."""
    if scraped_val is None:
        return 0

    score = _SCRAPED_BASE
    sl    = source.lower()

    if "edgar" in sl:
        score += _ADJ["source_edgar"]
    elif "yahoo" in sl:
        score += _ADJ["source_yahoo"]
    elif "macrotrends" in sl:
        score += _ADJ["source_macrotrends"]
    else:
        score += _ADJ["source_scraped_generic"]

    # Additional per-field trust penalties for known high-risk fields
    if field == "free_cash_flow":
        score -= 15   # FCF is the most frequently wrong field on free sites
    elif field == "ebit":
        score -= 5    # EBIT definitions vary (adjusted vs reported)
    elif field == "ebitda":
        score -= 8    # adjusted EBITDA confusion

    return _clamp(score)


# ─────────────────────────────────────────────────────────────────────────────
# Comparison and selection
# ─────────────────────────────────────────────────────────────────────────────

def _compare_and_choose(
    derived_val:   float | None,
    derived_conf:  int,
    scraped_val:   float | None,
    scraped_conf:  int,
    field:         str,
    year:          str,
    method_id:     str,
) -> dict:
    """
    Compare derived vs scraped and select the higher-confidence version.
    Returns a structured comparison record.
    """
    pct_diff = None
    if derived_val is not None and scraped_val is not None and abs(scraped_val) > 1:
        pct_diff = round(abs(derived_val - scraped_val) / abs(scraped_val) * 100, 1)

    # Both unavailable
    if derived_val is None and scraped_val is None:
        return {
            "field": field, "year": year,
            "derived_value": None, "derived_confidence": 0, "derived_method": method_id,
            "scraped_value": None, "scraped_confidence": 0,
            "difference_pct": None,
            "chosen_value": None, "chosen_source": "none",
            "reason": "Both derived and scraped values unavailable",
            "low_confidence_warning": True,
        }

    # Only derived
    if derived_val is not None and scraped_val is None:
        return {
            "field": field, "year": year,
            "derived_value": derived_val, "derived_confidence": derived_conf,
            "derived_method": method_id,
            "scraped_value": None, "scraped_confidence": 0,
            "difference_pct": None,
            "chosen_value": derived_val, "chosen_source": "derived",
            "reason": "Scraped value absent — using derived value",
            "low_confidence_warning": derived_conf < 50,
        }

    # Only scraped
    if derived_val is None and scraped_val is not None:
        return {
            "field": field, "year": year,
            "derived_value": None, "derived_confidence": 0,
            "derived_method": method_id,
            "scraped_value": scraped_val, "scraped_confidence": scraped_conf,
            "difference_pct": None,
            "chosen_value": scraped_val, "chosen_source": "scraped",
            "reason": "Derivation failed — falling back to scraped value",
            "low_confidence_warning": scraped_conf < 50,
        }

    # Both available — choose by confidence
    if derived_conf >= scraped_conf:
        chosen_val    = derived_val
        chosen_source = "derived"
        diff_str = f"  Difference: {pct_diff:.1f}%." if pct_diff is not None else ""
        reason = (
            f"Derived confidence ({derived_conf}) ≥ scraped ({scraped_conf}).{diff_str}"
        )
    else:
        chosen_val    = scraped_val
        chosen_source = "scraped"
        diff_str = f"  Difference: {pct_diff:.1f}%." if pct_diff is not None else ""
        reason = (
            f"Scraped confidence ({scraped_conf}) > derived ({derived_conf}).{diff_str}"
        )

    # Flag both-low-confidence case
    low_conf = derived_conf < 50 and scraped_conf < 50
    if low_conf:
        reason += "  WARNING: both versions are low-confidence — field unreliable."

    # Flag large disagreement even when a choice is made
    if pct_diff is not None and pct_diff > 20 and chosen_source == "derived":
        reason += f"  Large gap vs scraped ({pct_diff:.1f}%) — verify derivation inputs."

    return {
        "field":              field,
        "year":               year,
        "derived_value":      derived_val,
        "derived_confidence": derived_conf,
        "derived_method":     method_id,
        "scraped_value":      scraped_val,
        "scraped_confidence": scraped_conf,
        "difference_pct":     pct_diff,
        "chosen_value":       chosen_val,
        "chosen_source":      chosen_source,
        "reason":             reason.strip(),
        "low_confidence_warning": low_conf,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Canonical field update helper
# ─────────────────────────────────────────────────────────────────────────────

def _update_canonical_field(yr: dict, field: str, comp: dict) -> None:
    """
    Apply the chosen value from a comparison record back to canonical_by_year.
    Preserves raw scraped value and full derivation audit trail as metadata.
    """
    chosen = comp.get("chosen_value")

    # Always store the derivation audit trail, even if no value changed
    yr[f"_{field}_derivation"] = {
        "derived_value":      comp.get("derived_value"),
        "scraped_value":      comp.get("scraped_value"),
        "chosen_source":      comp.get("chosen_source"),
        "chosen_value":       chosen,
        "derived_confidence": comp.get("derived_confidence"),
        "scraped_confidence": comp.get("scraped_confidence"),
        "difference_pct":     comp.get("difference_pct"),
        "method_id":          comp.get("derived_method"),
        "reason":             comp.get("reason"),
    }

    if chosen is None:
        return   # nothing to update in the data fields

    # Update the canonical value and its provenance
    yr[field] = chosen

    if "_sources" not in yr:
        yr["_sources"] = {}
    if "_mapping_confidence" not in yr:
        yr["_mapping_confidence"] = {}

    src_tag = comp["chosen_source"]
    if src_tag == "derived":
        method = comp.get("derived_method", "?")
        yr["_sources"][field]            = f"derived:{method}"
        yr["_mapping_confidence"][field] = "high"
    else:
        # Keep existing source tag for scraped values
        existing_src = yr["_sources"].get(field, "scraped")
        yr["_sources"][field] = existing_src
        # Confidence stays as-is (set by standardiser)


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_canonical_fundamentals(standardised: dict) -> dict:
    """
    Derive canonical EBIT, EBITDA, and FCF for every year in canonical_by_year.
    Updates the canonical_by_year dict with chosen values and attaches a full
    audit report under the 'canonical_fundamentals' key.

    Parameters
    ----------
    standardised : dict  — output of run_standardiser()

    Returns
    -------
    Augmented standardised dict (deep copy of canonical_by_year; other keys
    are shallow-copied to avoid mutating the caller's dict).
    """
    raw_cby   = standardised.get("canonical_by_year", {})
    stats     = standardised.get("stats", {})
    sector    = stats.get("sector")

    canonical_by_year = copy.deepcopy(raw_cby)
    comparisons: list[dict] = []
    year_reports: dict[str, dict] = {}
    issues: list[str] = []

    for year in sorted(canonical_by_year.keys(), reverse=True):
        yr = canonical_by_year[year]

        # ── Capex sign normalisation ──────────────────────────────────────────
        capex_raw = _safe(yr.get("capex"))
        ocf       = _safe(yr.get("operating_cash_flow"))
        fcf_sc    = _safe(yr.get("free_cash_flow"))
        capex_pos, capex_note = _normalise_capex_for_derivation(capex_raw, ocf, fcf_sc)

        # ── Derive EBIT ───────────────────────────────────────────────────────
        m1_ebit = _derive_ebit_m1(yr)
        m2_ebit = _derive_ebit_m2(yr)
        m3_ebit = _derive_ebit_m3(yr)
        best_ebit = _choose_best_ebit([m1_ebit, m2_ebit, m3_ebit])

        derived_ebit      = best_ebit.get("value")
        derived_ebit_conf = _score_ebit_derivation(best_ebit, yr, sector)
        scraped_ebit      = _safe(yr.get("ebit"))
        scraped_ebit_src  = (yr.get("_sources") or {}).get("ebit", "unknown")
        scraped_ebit_conf = _score_scraped_value(scraped_ebit, scraped_ebit_src, "ebit")

        ebit_comp = _compare_and_choose(
            derived_ebit, derived_ebit_conf,
            scraped_ebit, scraped_ebit_conf,
            "ebit", year, best_ebit.get("method_id", "none"),
        )
        comparisons.append(ebit_comp)
        _update_canonical_field(yr, "ebit", ebit_comp)

        if ebit_comp.get("low_confidence_warning"):
            issues.append(f"Low confidence on EBIT for {year}")

        # ── Derive EBITDA ─────────────────────────────────────────────────────
        ebit_chosen   = ebit_comp.get("chosen_value")
        ebitda_der    = _derive_ebitda_m1(yr, ebit_chosen)
        derived_ebitda      = ebitda_der.get("value")
        # EBITDA derivation is simple — give it a moderate fixed confidence
        derived_ebitda_conf = 68 if ebitda_der.get("available") else 0
        scraped_ebitda      = _safe(yr.get("ebitda"))
        scraped_ebitda_src  = (yr.get("_sources") or {}).get("ebitda", "unknown")
        scraped_ebitda_conf = _score_scraped_value(scraped_ebitda, scraped_ebitda_src, "ebitda")

        ebitda_comp = _compare_and_choose(
            derived_ebitda, derived_ebitda_conf,
            scraped_ebitda, scraped_ebitda_conf,
            "ebitda", year, "CF-EBITDA-M1",
        )
        comparisons.append(ebitda_comp)
        _update_canonical_field(yr, "ebitda", ebitda_comp)

        # ── Derive FCF ────────────────────────────────────────────────────────
        fcf_m1 = _derive_fcf_m1(yr, capex_pos)
        fcf_m2 = _derive_fcf_m2(yr, capex_pos, ebit_chosen)
        fcf_m3 = _derive_fcf_m3(yr, capex_pos, ebit_chosen)
        best_fcf = _choose_best_fcf([fcf_m1, fcf_m2, fcf_m3])

        derived_fcf      = best_fcf.get("value")
        derived_fcf_conf = _score_fcf_derivation(best_fcf, yr, sector)
        scraped_fcf      = _safe(yr.get("free_cash_flow"))
        scraped_fcf_src  = (yr.get("_sources") or {}).get("free_cash_flow", "unknown")
        scraped_fcf_conf = _score_scraped_value(scraped_fcf, scraped_fcf_src, "free_cash_flow")

        fcf_comp = _compare_and_choose(
            derived_fcf, derived_fcf_conf,
            scraped_fcf, scraped_fcf_conf,
            "free_cash_flow", year, best_fcf.get("method_id", "none"),
        )
        comparisons.append(fcf_comp)
        _update_canonical_field(yr, "free_cash_flow", fcf_comp)

        if fcf_comp.get("low_confidence_warning"):
            issues.append(f"Low confidence on FCF for {year}")

        # ── Store capex note ──────────────────────────────────────────────────
        yr["_capex_sign_note"] = capex_note

        # ── Year report ───────────────────────────────────────────────────────
        year_reports[year] = {
            "ebit_comparison":    ebit_comp,
            "ebitda_comparison":  ebitda_comp,
            "fcf_comparison":     fcf_comp,
            "capex_note":         capex_note,
            "ebit_methods": {
                m["method_id"]: {"value": m.get("value"), "warnings": m.get("warnings", [])}
                for m in [m1_ebit, m2_ebit, m3_ebit] if m.get("available")
            },
            "fcf_methods": {
                m["method_id"]: {"value": m.get("value"), "warnings": m.get("warnings", [])}
                for m in [fcf_m1, fcf_m2, fcf_m3] if m.get("available")
            },
        }

    # ── Build summary stats ───────────────────────────────────────────────────
    n_years = len(canonical_by_year)
    ebit_rows = [c for c in comparisons if c["field"] == "ebit"]
    fcf_rows  = [c for c in comparisons if c["field"] == "free_cash_flow"]

    n_ebit_derived   = sum(1 for c in ebit_rows  if c.get("chosen_source") == "derived")
    n_fcf_derived    = sum(1 for c in fcf_rows   if c.get("chosen_source") == "derived")
    ebit_large_gaps  = [c for c in ebit_rows if (c.get("difference_pct") or 0) > 20]
    fcf_large_gaps   = [c for c in fcf_rows  if (c.get("difference_pct") or 0) > 20]

    # ── Assemble output ───────────────────────────────────────────────────────
    out = dict(standardised)   # shallow copy — preserves all other keys
    out["canonical_by_year"] = canonical_by_year
    out["canonical_fundamentals"] = {
        "status":       "pass" if not issues else "pass_with_warnings",
        "year_reports": year_reports,
        "comparisons":  comparisons,
        "issues":       issues,
        "summary": {
            "years_processed":        n_years,
            "ebit_used_derived":      n_ebit_derived,
            "ebit_used_scraped":      len(ebit_rows) - n_ebit_derived,
            "fcf_used_derived":       n_fcf_derived,
            "fcf_used_scraped":       len(fcf_rows) - n_fcf_derived,
            "ebit_large_gaps":        len(ebit_large_gaps),
            "fcf_large_gaps":         len(fcf_large_gaps),
            "low_confidence_years":   len(issues),
        },
    }
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Terminal report
# ─────────────────────────────────────────────────────────────────────────────

def print_canonical_fundamentals_report(result: dict) -> None:
    """Print a structured summary to stdout."""
    cf    = result.get("canonical_fundamentals", {})
    summ  = cf.get("summary", {})
    years = sorted(cf.get("year_reports", {}).keys(), reverse=True)

    print()
    print("=" * 70)
    print("  Canonical Fundamentals  —  Stage 2.5")
    print("=" * 70)

    if not years:
        print("  No years processed.\n")
        return

    print(
        f"  Years processed : {len(years)}  "
        f"({years[-1]} – {years[0]})\n"
    )

    # Per-field summary
    for field, label in [("ebit", "EBIT"), ("free_cash_flow", "FCF"), ("ebitda", "EBITDA")]:
        rows = [c for c in cf.get("comparisons", []) if c["field"] == field]
        if not rows:
            continue
        n_derived = sum(1 for r in rows if r.get("chosen_source") == "derived")
        n_scraped = len(rows) - n_derived
        n_gaps    = sum(1 for r in rows if (r.get("difference_pct") or 0) > 20)
        print(
            f"  {label:<12} "
            f"derived chosen: {n_derived}/{len(rows)} yr(s)  "
            f"scraped chosen: {n_scraped}/{len(rows)} yr(s)  "
            f"large gaps (>20%): {n_gaps}"
        )

    # Per-year detail
    print()
    hdr = f"  {'Year':<6} {'Field':<14} {'Derived':>12} {'Scraped':>12} {'Diff%':>7} {'Chosen':>12}  Source / Method"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for year in years:
        rpt = cf.get("year_reports", {}).get(year, {})
        for field_key, label in [
            ("ebit_comparison",   "EBIT"),
            ("ebitda_comparison", "EBITDA"),
            ("fcf_comparison",    "FCF"),
        ]:
            comp = rpt.get(field_key, {})
            if not comp:
                continue
            dv   = _fmt(comp.get("derived_value"))
            sv   = _fmt(comp.get("scraped_value"))
            pd   = f"{comp['difference_pct']:.1f}%" if comp.get("difference_pct") is not None else "—"
            cv   = _fmt(comp.get("chosen_value"))
            src  = comp.get("chosen_source", "—")
            meth = comp.get("derived_method", "—")
            flag = " ⚠" if comp.get("low_confidence_warning") else ""
            print(
                f"  {year:<6} {label:<14} {dv:>12} {sv:>12} {pd:>7} {cv:>12}"
                f"  {src} / {meth}{flag}"
            )
        # Capex note (only print once per year, abbreviated)
        cap_note = rpt.get("capex_note", "")
        if "flipped" in cap_note or "diverges" in cap_note:
            print(f"  {'':6} {'Capex note':<14} {cap_note[:60]}")

    # Issues
    issues = cf.get("issues", [])
    if issues:
        print(f"\n  Issues ({len(issues)}):")
        for iss in issues:
            print(f"    ⚠  {iss}")

    print(f"\n  Status: {cf.get('status', '—').upper()}")
    print("=" * 70 + "\n")
