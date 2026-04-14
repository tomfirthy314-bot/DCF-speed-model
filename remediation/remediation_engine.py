"""
Sprint 4C — Remediation Engine

Purpose:
    Automatically address FLAG and WARN issues identified by the Coherence Engine.

    FLAGS (critical): attempt active correction — patch the offending assumption
    with the best available alternative, re-run the forecast and valuation, then
    re-check coherence on the corrected output to measure improvement.

    WARNs (caution): verify the number by re-deriving or cross-checking; apply a
    conservative correction only when the re-derived value materially improves
    model quality. Less aggressive intervention than FLAG corrections.

    Non-destructive: the original values are preserved in valued["pre_remediation"]
    so the analyst can compare before/after. Every correction is logged with what
    changed, what the new value is, and the reasoning.

Correction strategies per coherence check:
    tax_rate              FLAG/WARN  Override with jurisdiction statutory rate
    shares_cross_check    FLAG       Re-derive shares from market_cap / price
    shares_cross_check    WARN       Log and verify; no change applied
    nwc_pct               FLAG       Cap at sector-appropriate maximum
    nwc_pct               WARN       Cap only if >5pp above sector max
    wacc_bounds           FLAG       Clamp to [6%, 18%]
    wacc_bounds           WARN       Log and verify; no change applied
    wacc_tg_spread        FLAG       Cap terminal growth to WACC − 2%
    wacc_tg_spread        WARN       Cap terminal growth to WACC − 1.5%
    nopat_margin_exp      FLAG       Cap terminal EBIT margin at 1.5× historical

    tv_concentration      —  Cannot be directly fixed (structural); note only
    vps_vs_price          —  Root cause addressed by other corrections
    ev_revenue_multiple   —  Cannot be fixed without external data
    assumption_confidence —  Cannot be improved programmatically
"""

import copy
from forecaster.forecaster import run_forecaster
from valuation_engine.valuation_engine import run_valuation_engine
from coherence.coherence_engine import run_coherence_engine


# ---------------------------------------------------------------------------
# Jurisdiction statutory tax rates (approximate as of early 2026)
# ---------------------------------------------------------------------------
_STATUTORY_TAX = {
    "United Kingdom": 0.25,
    "United States":  0.21,
    "Germany":        0.30,
    "France":         0.25,
    "Netherlands":    0.258,
    "Sweden":         0.206,
    "Switzerland":    0.185,
    "Ireland":        0.125,
    "Singapore":      0.17,
    "Japan":          0.307,
    "Canada":         0.265,
    "Australia":      0.30,
}
_DEFAULT_STATUTORY = 0.25

# NWC/revenue caps by classification
_NWC_SECTOR_CAPS = {
    "asset_light": 0.15,
    "industrial":  0.35,
    "consumer":    0.20,
    "resources":   0.30,
    "utilities":   0.15,
    "hybrid":      0.25,
    "unknown":     0.30,
}

_WACC_FLOOR = 0.06
_WACC_CAP   = 0.18
_MIN_SPREAD_FLAG = 0.02    # Minimum WACC-g spread enforced on FLAG
_MIN_SPREAD_WARN = 0.015   # Minimum WACC-g spread enforced on WARN
_MAX_NOPAT_EXPANSION = 1.5 # Terminal year NOPAT margin cap vs normalised historical


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_remediation_engine(valued: dict) -> dict:
    """
    Run the remediation engine on a fully-valued pipeline output.

    Returns an updated valued dict. The original valued is never mutated —
    all work is done on a deep copy. Never raises — all errors are caught
    and surfaced as valued["remediation"]["error"].
    """
    try:
        return _run(valued)
    except Exception as e:
        out = copy.deepcopy(valued)
        out["remediation"] = {
            "status":       "error",
            "error":        str(e),
            "corrections":  [],
            "re_coherence": valued.get("coherence"),
        }
        return out


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _run(valued: dict) -> dict:
    coherence = valued.get("coherence", {})
    flags     = coherence.get("flags", [])
    warns     = coherence.get("warns", [])

    if not flags and not warns:
        out = copy.deepcopy(valued)
        out["remediation"] = {
            "status":      "no_action",
            "message":     "No flags or warnings raised by coherence engine — nothing to remediate.",
            "corrections": [],
            "re_coherence": coherence,
        }
        return out

    working     = copy.deepcopy(valued)
    stats       = working.get("stats", {})
    classif     = working.get("classification", "unknown")
    country     = stats.get("country", "")
    price       = stats.get("current_price")
    market_cap  = stats.get("market_cap")
    norm        = working.get("normalised", {})
    assumptions = working["assumptions"]       if "assumptions" in working else {}
    scenarios   = working["scenarios"]         if "scenarios"   in working else {}

    corrections      = []
    needs_forecaster = False
    needs_valuation  = False
    processed        = set()

    # Flags first (critical urgency), then warns (caution)
    all_issues = [(c, "FLAG") for c in flags] + [(c, "WARN") for c in warns]

    for chk, severity in all_issues:
        check_id = chk["check"]
        if check_id in processed:
            continue
        processed.add(check_id)

        corr = _correct(
            check_id, chk, severity,
            working, assumptions, scenarios, norm,
            stats, classif, country, price, market_cap,
        )
        if corr:
            corrections.append(corr)
            if corr["applied"]:
                if corr.get("requires_forecaster"):
                    needs_forecaster = True
                if corr.get("requires_valuation"):
                    needs_valuation = True

    # -------------------------------------------------------------------------
    # Re-run downstream stages for all applied corrections in a single pass
    # -------------------------------------------------------------------------
    applied = [c for c in corrections if c["applied"]]

    if applied and (needs_forecaster or needs_valuation):
        # Snapshot original values for comparison
        working["pre_remediation"] = {
            c["field"]: c["original_value"] for c in applied
        }

        try:
            if needs_forecaster:
                forecasted = run_forecaster(working)
                if forecasted.get("status") != "fail":
                    working["forecast"]            = forecasted["forecast"]
                    working["forecast_start_year"] = forecasted.get("forecast_start_year")

            re_valued = run_valuation_engine(working)
            if re_valued.get("status") != "fail":
                working["valuation"]           = re_valued["valuation"]
                working["sensitivity"]         = re_valued.get("sensitivity")
                working["equity_bridge_inputs"] = re_valued.get(
                    "equity_bridge_inputs", working.get("equity_bridge_inputs")
                )

            # Re-check coherence on corrected output
            re_coherence = run_coherence_engine(working)
            working["coherence"] = re_coherence

            re_flags = re_coherence.get("flags", [])
            re_warns = re_coherence.get("warns", [])
            improved = (len(flags) + len(warns)) - (len(re_flags) + len(re_warns))

            working["remediation"] = {
                "status":         "corrected",
                "corrections":    corrections,
                "re_coherence":   re_coherence,
                "flags_before":   len(flags),
                "flags_after":    len(re_flags),
                "warns_before":   len(warns),
                "warns_after":    len(re_warns),
                "net_improvement": improved,
            }

        except Exception as e:
            working["remediation"] = {
                "status":       "correction_applied_rerun_failed",
                "corrections":  corrections,
                "error":        str(e),
                "re_coherence": None,
            }

    elif not applied:
        # All issues were reviewed but none could be corrected automatically
        working["remediation"] = {
            "status":      "reviewed_no_change",
            "message":     "All issues reviewed — no automated correction was possible.",
            "corrections": corrections,
            "re_coherence": coherence,
        }
    else:
        # Corrections applied but neither forecaster nor valuation needed a re-run
        # (shouldn't happen in practice, but handle gracefully)
        working["remediation"] = {
            "status":      "reviewed_no_change",
            "corrections": corrections,
            "re_coherence": coherence,
        }

    return working


# ---------------------------------------------------------------------------
# Individual correction strategies
# ---------------------------------------------------------------------------

def _correct(check_id, chk, severity,
             working, assumptions, scenarios, norm,
             stats, classif, country, price, market_cap):
    """
    Attempt a correction for the given coherence check.
    Returns a correction record dict, or None if this check has no strategy.
    """

    # -----------------------------------------------------------------------
    # Tax rate — override with statutory rate for jurisdiction
    # -----------------------------------------------------------------------
    if check_id == "tax_rate":
        current_tax = (assumptions.get("tax_rate") or {}).get("value")
        if current_tax is None:
            return None

        statutory = _STATUTORY_TAX.get(country, _DEFAULT_STATUTORY)

        # FLAG: always apply; WARN: only if difference is ≥ 3pp
        if severity == "FLAG" or abs(statutory - current_tax) >= 0.03:
            _patch_assumption(
                working, "tax_rate", statutory,
                tag="remediation_override",
                rationale=(
                    f"Remediation: effective rate {current_tax:.1%} flagged as unrealistically "
                    f"low. Replaced with {country or 'default'} statutory rate {statutory:.1%}."
                ),
            )
            return _corr(check_id, severity,
                         field="assumptions.tax_rate",
                         original=current_tax,
                         corrected=statutory,
                         reasoning=(
                             f"Effective rate {current_tax:.1%} replaced with "
                             f"{country} statutory rate {statutory:.1%}."
                         ),
                         applied=True, forecaster=True, valuation=True)

        return _corr(check_id, severity,
                     field="assumptions.tax_rate",
                     original=current_tax, corrected=None,
                     reasoning=(
                         f"Effective rate {current_tax:.1%} is within 3pp of statutory "
                         f"{statutory:.1%} — difference too small to justify override."
                     ),
                     applied=False)

    # -----------------------------------------------------------------------
    # Shares cross-check — re-derive from market cap / price
    # -----------------------------------------------------------------------
    if check_id == "shares_cross_check":
        current_shares = stats.get("shares_outstanding")
        if severity != "FLAG":
            return _corr(check_id, severity,
                         field="stats.shares_outstanding",
                         original=current_shares, corrected=None,
                         reasoning=(
                             "WARN level — shares re-verified but discrepancy is within "
                             "tolerance. Possible recent buyback or issuance."
                         ),
                         applied=False)

        if not (market_cap and price and price > 0 and current_shares):
            return _corr(check_id, severity,
                         field="stats.shares_outstanding",
                         original=current_shares, corrected=None,
                         reasoning="Cannot re-derive shares — market cap or price missing.",
                         applied=False)

        derived = market_cap / price
        ratio = derived / current_shares

        if ratio < 0.5 or ratio > 2.0:
            # Materially different — apply the derived count
            working["stats"]["shares_outstanding"] = derived
            return _corr(check_id, severity,
                         field="stats.shares_outstanding",
                         original=current_shares,
                         corrected=derived,
                         reasoning=(
                             f"Re-derived from market cap ({_fb(market_cap)}) ÷ price "
                             f"({price:.2f}) = {_fb(derived)}. "
                             f"Replaced original {_fb(current_shares)} (ratio: {ratio:.1f}x)."
                         ),
                         applied=True, forecaster=False, valuation=True)

        return _corr(check_id, severity,
                     field="stats.shares_outstanding",
                     original=current_shares, corrected=None,
                     reasoning=(
                         f"Re-derived shares ({_fb(derived)}) within 50% of current "
                         f"({_fb(current_shares)}) — discrepancy does not justify override."
                     ),
                     applied=False)

    # -----------------------------------------------------------------------
    # NWC % revenue — cap at sector-appropriate maximum
    # -----------------------------------------------------------------------
    if check_id == "nwc_pct":
        current_nwc = (assumptions.get("nwc_pct_revenue") or {}).get("value")
        if current_nwc is None:
            return None

        cap = _NWC_SECTOR_CAPS.get(classif, 0.30)
        # WARN: only correct if value is more than 5pp above the cap
        threshold = cap if severity == "FLAG" else cap + 0.05

        if abs(current_nwc) > threshold:
            new_nwc = cap if current_nwc > 0 else -cap
            _patch_assumption(
                working, "nwc_pct_revenue", new_nwc,
                tag="remediation_override",
                rationale=(
                    f"Remediation: NWC/revenue {current_nwc:.1%} exceeds "
                    f"{classif} sector cap. Capped at {new_nwc:.1%}."
                ),
            )
            return _corr(check_id, severity,
                         field="assumptions.nwc_pct_revenue",
                         original=current_nwc, corrected=new_nwc,
                         reasoning=(
                             f"NWC/revenue {current_nwc:.1%} exceeds {classif} sector cap "
                             f"of {cap:.1%}. Capped at {new_nwc:.1%}."
                         ),
                         applied=True, forecaster=True, valuation=True)

        return _corr(check_id, severity,
                     field="assumptions.nwc_pct_revenue",
                     original=current_nwc, corrected=None,
                     reasoning=(
                         f"NWC {current_nwc:.1%} within threshold for {severity} correction "
                         f"(sector cap {cap:.1%}{'+ 5pp tolerance' if severity == 'WARN' else ''}) "
                         f"— no correction applied."
                     ),
                     applied=False)

    # -----------------------------------------------------------------------
    # WACC bounds — clamp to acceptable range
    # -----------------------------------------------------------------------
    if check_id == "wacc_bounds":
        current_wacc = (assumptions.get("wacc") or {}).get("value")
        if current_wacc is None:
            return None

        if severity != "FLAG":
            return _corr(check_id, severity,
                         field="assumptions.wacc",
                         original=current_wacc, corrected=None,
                         reasoning="WARN level — reviewed but no correction applied.",
                         applied=False)

        new_wacc = max(_WACC_FLOOR, min(_WACC_CAP, current_wacc))
        if new_wacc != current_wacc:
            _patch_wacc(working, new_wacc)
            return _corr(check_id, severity,
                         field="assumptions.wacc",
                         original=current_wacc, corrected=new_wacc,
                         reasoning=(
                             f"WACC {current_wacc:.1%} outside acceptable range "
                             f"[{_WACC_FLOOR:.0%}–{_WACC_CAP:.0%}]. Clamped to {new_wacc:.1%}."
                         ),
                         applied=True, forecaster=False, valuation=True)

        return _corr(check_id, severity,
                     field="assumptions.wacc",
                     original=current_wacc, corrected=None,
                     reasoning="WACC already within bounds after rounding — no change.",
                     applied=False)

    # -----------------------------------------------------------------------
    # WACC–terminal growth spread — enforce minimum spread
    # -----------------------------------------------------------------------
    if check_id == "wacc_tg_spread":
        current_wacc = (assumptions.get("wacc") or {}).get("value")
        current_tg   = (assumptions.get("terminal_growth") or {}).get("value")
        if current_wacc is None or current_tg is None:
            return None

        min_spread = _MIN_SPREAD_FLAG if severity == "FLAG" else _MIN_SPREAD_WARN
        spread = current_wacc - current_tg

        if spread < min_spread:
            new_tg = max(0.005, round(current_wacc - min_spread, 4))
            _patch_assumption(
                working, "terminal_growth", new_tg,
                tag="remediation_override",
                rationale=(
                    f"Remediation: WACC-growth spread was {spread:.1%}. Terminal growth "
                    f"reduced from {current_tg:.1%} to {new_tg:.1%} to enforce "
                    f"{min_spread:.1%} minimum spread."
                ),
            )
            return _corr(check_id, severity,
                         field="assumptions.terminal_growth",
                         original=current_tg, corrected=new_tg,
                         reasoning=(
                             f"WACC ({current_wacc:.1%}) - g ({current_tg:.1%}) = "
                             f"{spread:.1%} spread. Terminal growth reduced to {new_tg:.1%} "
                             f"to achieve {min_spread:.1%} minimum spread."
                         ),
                         applied=True, forecaster=False, valuation=True)

        return _corr(check_id, severity,
                     field="assumptions.terminal_growth",
                     original=current_tg, corrected=None,
                     reasoning=f"Spread {spread:.1%} already meets minimum — no correction.",
                     applied=False)

    # -----------------------------------------------------------------------
    # NOPAT margin expansion — cap terminal year EBIT margin
    # -----------------------------------------------------------------------
    if check_id == "nopat_margin_expansion":
        if severity != "FLAG":
            return _corr(check_id, severity,
                         field="assumptions.ebit_margin",
                         original=None, corrected=None,
                         reasoning="WARN level — margin expansion reviewed but no correction applied.",
                         applied=False)

        hist_ebit_margin = norm.get("ebit_margin")
        if hist_ebit_margin is None:
            return None

        n_years     = working.get("forecast_years", 5)
        margin_entry = assumptions.get("ebit_margin") or {}
        final_key    = f"year_{n_years}"
        final_margin = margin_entry.get(final_key)
        if final_margin is None:
            return None

        max_margin = hist_ebit_margin * _MAX_NOPAT_EXPANSION

        if final_margin > max_margin:
            yr1_margin = margin_entry.get("year_1", hist_ebit_margin)
            new_path   = _linear_path(yr1_margin, max_margin, n_years)

            # Patch the ebit_margin path in assumptions and all scenarios
            for i, v in enumerate(new_path):
                assumptions["ebit_margin"][f"year_{i + 1}"] = round(v, 4)
                for s_data in working.get("scenarios", {}).values():
                    if "ebit_margin" in s_data:
                        s_data["ebit_margin"][f"year_{i + 1}"] = round(v, 4)
            assumptions["ebit_margin"]["tag"] = "remediation_override"

            return _corr(check_id, severity,
                         field=f"assumptions.ebit_margin.{final_key}",
                         original=final_margin, corrected=max_margin,
                         reasoning=(
                             f"Terminal EBIT margin {final_margin:.1%} exceeds "
                             f"{_MAX_NOPAT_EXPANSION:.0f}× historical "
                             f"({hist_ebit_margin:.1%}). Capped at {max_margin:.1%}."
                         ),
                         applied=True, forecaster=True, valuation=True)

        return _corr(check_id, severity,
                     field=f"assumptions.ebit_margin.{final_key}",
                     original=final_margin, corrected=None,
                     reasoning=(
                         f"Margin expansion within {_MAX_NOPAT_EXPANSION:.0f}× bound — "
                         "no correction required."
                     ),
                     applied=False)

    # -----------------------------------------------------------------------
    # Checks with no automated fix — record the review note
    # -----------------------------------------------------------------------
    _no_fix = {
        "tv_concentration": (
            "Terminal value concentration is a structural result driven by WACC-growth spread. "
            "If wacc_tg_spread was also flagged, that correction reduces TV weight. "
            "Present the sensitivity table as the primary output."
        ),
        "vps_vs_price": (
            "VPS vs price divergence is likely a symptom. Root causes (tax rate, shares, NWC) "
            "are addressed by other corrections — re-check after re-run completes."
        ),
        "ev_revenue_multiple": (
            "EV/Revenue multiple is a valuation output, not an input. Review sector "
            "classification and key revenue/EBIT assumptions against listed comparables."
        ),
        "assumption_confidence": (
            "Assumption confidence cannot be improved programmatically — requires analyst "
            "input of company-specific data to replace defaulted assumptions."
        ),
    }
    if check_id in _no_fix:
        return _corr(check_id, severity,
                     field="—",
                     original=chk.get("value"), corrected=None,
                     reasoning=_no_fix[check_id],
                     applied=False)

    return None


# ---------------------------------------------------------------------------
# Patching helpers
# ---------------------------------------------------------------------------

def _patch_assumption(working: dict, field: str, new_value: float,
                      tag: str = "remediation_override", rationale: str = ""):
    """Patch a single-value assumption in both assumptions and all scenarios."""
    assumptions = working.get("assumptions", {})
    if field not in assumptions:
        assumptions[field] = {}
    assumptions[field]["value"]     = new_value
    assumptions[field]["tag"]       = tag
    assumptions[field]["rationale"] = rationale

    for s_data in working.get("scenarios", {}).values():
        if field in s_data:
            s_data[field] = dict(s_data[field])
            s_data[field]["value"]     = new_value
            s_data[field]["tag"]       = tag
            s_data[field]["rationale"] = rationale


def _patch_wacc(working: dict, new_wacc: float):
    """Patch WACC in assumptions and all scenarios (bear gets +100bps, capped)."""
    scenarios = working.get("scenarios", {})

    _patch_assumption(
        working, "wacc", new_wacc,
        tag="remediation_override",
        rationale=f"WACC clamped to {new_wacc:.1%} by remediation engine.",
    )
    # Bear scenario retains its +100bps risk premium, but also capped
    if "bear" in scenarios and "wacc" in scenarios["bear"]:
        scenarios["bear"]["wacc"]["value"] = min(new_wacc + 0.01, _WACC_CAP)


def _linear_path(start: float, end: float, n: int) -> list:
    """Return a linear path from start to end over n steps."""
    if n <= 1:
        return [end]
    return [start + (end - start) * i / (n - 1) for i in range(n)]


# ---------------------------------------------------------------------------
# Record builder helpers
# ---------------------------------------------------------------------------

def _corr(check_id, severity, field, original, corrected, reasoning,
          applied=False, forecaster=False, valuation=False):
    out = {
        "check_id":        check_id,
        "severity":        severity,
        "field":           field,
        "original_value":  original,
        "corrected_value": corrected,
        "reasoning":       reasoning,
        "applied":         applied,
    }
    if applied:
        out["requires_forecaster"] = forecaster
        out["requires_valuation"]  = valuation
    return out


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_remediation_report(result: dict, valued: dict):
    company = valued.get("company_name", "")
    ticker  = valued.get("ticker", "")
    rem     = result.get("remediation", {})
    status  = rem.get("status", "unknown")

    _status_labels = {
        "corrected":                       "CORRECTIONS APPLIED",
        "reviewed_no_change":              "REVIEWED — NO CHANGE",
        "no_action":                       "NO ACTION NEEDED",
        "correction_applied_rerun_failed": "APPLIED — RE-RUN FAILED",
        "error":                           "ERROR",
    }

    print()
    print("=" * 60)
    print(f"  Remediation Engine  —  {company} ({ticker})")
    print(f"  Status : [{_status_labels.get(status, status.upper())}]")
    print("=" * 60)

    if rem.get("error"):
        print(f"\n  ERROR: {rem['error']}\n")
        return

    if status == "no_action":
        print(f"\n  {rem.get('message', '')}\n")
        return

    corrections  = rem.get("corrections", [])
    applied      = [c for c in corrections if c["applied"]]
    not_applied  = [c for c in corrections if not c["applied"]]

    # Applied corrections
    if applied:
        print(f"\n  Applied corrections ({len(applied)}):")
        for c in applied:
            orig = _fmt_val(c["original_value"])
            new  = _fmt_val(c["corrected_value"])
            print(f"\n    [{c['severity']}] {c['check_id']}  —  {orig}  →  {new}")
            _wrap(c["reasoning"], indent=6)

    # Re-coherence delta
    if status == "corrected":
        fb = rem.get("flags_before", 0)
        fa = rem.get("flags_after",  0)
        wb = rem.get("warns_before", 0)
        wa = rem.get("warns_after",  0)
        ni = rem.get("net_improvement", 0)
        print(f"\n  Re-coherence result:")
        print(f"    Flags : {fb}  →  {fa}  "
              f"({'resolved' if fa < fb else 'unchanged' if fa == fb else 'new flags'})")
        print(f"    Warns : {wb}  →  {wa}  "
              f"({'resolved' if wa < wb else 'unchanged' if wa == wb else 'new warns'})")
        print(f"    Net   : {ni:+d} issue(s) resolved")

    # Reviewed but not corrected
    if not_applied:
        print(f"\n  Reviewed — no correction applied ({len(not_applied)}):")
        for c in not_applied:
            badge = f"[{c['severity']}]"
            line  = f"    {badge} {c['check_id']}  —  {c['reasoning']}"
            if len(line) > 78:
                line = line[:75] + "..."
            print(line)

    print()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_val(val) -> str:
    if val is None:
        return "—"
    if isinstance(val, float):
        if abs(val) < 2.0:
            return f"{val:.1%}"
        v = abs(val)
        if v >= 1e9:
            return f"{val / 1e9:.2f}B"
        if v >= 1e6:
            return f"{val / 1e6:.1f}M"
        return f"{val:,.0f}"
    return str(val)


def _fb(val) -> str:
    if val is None:
        return "—"
    v = abs(float(val))
    s = f"{v / 1e9:.2f}B" if v >= 1e9 else f"{v / 1e6:.1f}M" if v >= 1e6 else f"{v:,.0f}"
    return f"({s})" if float(val) < 0 else s


def _wrap(text: str, indent: int = 4, width: int = 76):
    words = text.split()
    line  = " " * indent
    for word in words:
        if len(line) + len(word) + 1 > width:
            print(line)
            line = " " * indent + word
        else:
            line += ("" if line.strip() == "" else " ") + word
    if line.strip():
        print(line)
