"""
DCF Tool — Streamlit front end

Run with:
    streamlit run app.py
"""

import io
import os
import sys
import queue
import threading
import traceback

import streamlit as st

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DCF Valuation Tool",
    page_icon="📊",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Main container width */
    .block-container { max-width: 760px; padding-top: 2rem; }

    /* Stage log box */
    .log-box {
        background: #0e1117;
        border: 1px solid #2e3440;
        border-radius: 6px;
        padding: 14px 18px;
        font-family: 'Courier New', monospace;
        font-size: 0.82rem;
        color: #d8dee9;
        line-height: 1.6;
        max-height: 360px;
        overflow-y: auto;
    }
    .log-pass  { color: #a3be8c; }
    .log-warn  { color: #ebcb8b; }
    .log-flag  { color: #bf616a; }
    .log-info  { color: #88c0d0; }
    .log-dim   { color: #636e7b; }

    /* Result card */
    .result-card {
        background: #1c2333;
        border: 1px solid #2e3440;
        border-radius: 8px;
        padding: 20px 24px;
        margin-top: 1rem;
    }
    .vps-big { font-size: 2.4rem; font-weight: 700; color: #eceff4; }
    .vps-sub { font-size: 0.9rem; color: #7b8799; margin-top: 2px; }
    .metric-row { display: flex; gap: 32px; margin-top: 16px; }
    .metric-block { flex: 1; }
    .metric-label { font-size: 0.75rem; color: #7b8799; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-value { font-size: 1.1rem; font-weight: 600; color: #eceff4; margin-top: 2px; }
    .up   { color: #a3be8c; }
    .down { color: #bf616a; }
    .badge-flag { background:#3b1e1e; color:#bf616a; border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }
    .badge-warn { background:#2e2a14; color:#ebcb8b; border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }
    .badge-pass { background:#1a2e1a; color:#a3be8c; border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }
    .scenario-row { display:flex; gap:16px; margin-top:16px; }
    .scenario-cell { flex:1; background:#151c2b; border-radius:6px; padding:12px 14px; }
    .scenario-name { font-size:0.7rem; color:#7b8799; text-transform:uppercase; letter-spacing:0.06em; }
    .scenario-vps  { font-size:1.2rem; font-weight:600; color:#eceff4; margin-top:4px; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("## DCF Valuation Tool")
st.markdown(
    "<span style='color:#7b8799;font-size:0.9rem;'>"
    "Enter a company name or ticker to run a full discounted cash flow valuation."
    "</span>",
    unsafe_allow_html=True,
)
st.markdown("---")


# ---------------------------------------------------------------------------
# Input row
# ---------------------------------------------------------------------------
col_input, col_btn = st.columns([4, 1])
with col_input:
    company_input = st.text_input(
        label="Company",
        placeholder="e.g. Apple, AAPL, RR.L, Shell",
        label_visibility="collapsed",
    )
with col_btn:
    run_clicked = st.button("Run DCF", type="primary", use_container_width=True)


# ---------------------------------------------------------------------------
# Fields that directly feed the DCF model (forecaster + valuation engine)
# Anomaly issues for any other field are suppressed — they don't affect the output.
# ---------------------------------------------------------------------------
_DCF_FIELDS = frozenset({
    "revenue", "ebit", "ebitda", "free_cash_flow", "operating_cash_flow",
    "capex", "da", "net_income", "interest_expense", "pre_tax_income",
    "tax_provision", "debt", "long_term_debt", "lease_liabilities", "cash",
    "shares_outstanding", "eps_diluted", "change_in_working_cap",
    "net_debt", "derived_net_debt", "smoothed_tax_rate",
    # NWC components used for ΔNWC derivation
    "current_assets", "current_liabilities", "accounts_receivable",
    "inventory", "accounts_payable",
})


# ---------------------------------------------------------------------------
# Anomaly provenance tagger + model-input filter
# ---------------------------------------------------------------------------

def _tag_anomaly_provenance(anomaly: dict, canonical_by_year: dict) -> dict:
    """
    1. Tag each anomaly issue with whether the flagged value was derived or scraped.
    2. Filter issues to only those on fields that actually feed the DCF model.
       Issues on any other field (e.g. gross_profit, total_assets) are silently dropped
       — they don't affect the output and only create noise.
    3. Rebuild the summary counts and must_recheck_fields from the filtered set.
    """
    # Step 1: tag derived vs scraped
    for issue in anomaly.get("issues", []):
        field = issue.get("field")
        year  = issue.get("year")
        src   = ""
        if year and field:
            yr_data = canonical_by_year.get(year) or canonical_by_year.get(str(year)) or {}
            src = yr_data.get("_sources", {}).get(field, "")
        issue["data_source"] = "derived" if str(src).startswith("derived:") else (
            "scraped" if src else "unknown"
        )

    # Step 2: keep only issues on DCF-relevant fields (or structural issues with no field)
    all_issues = anomaly.get("issues", [])
    dcf_issues = [
        i for i in all_issues
        if not i.get("field") or i.get("field") in _DCF_FIELDS
    ]
    n_dropped = len(all_issues) - len(dcf_issues)
    anomaly["issues"] = dcf_issues

    # Step 3: rebuild summary from filtered issues
    summary = anomaly.setdefault("summary", {})
    crit_fields = sorted({i["field"] for i in dcf_issues if i.get("severity") == "critical" and i.get("field")})
    warn_fields = sorted({i["field"] for i in dcf_issues if i.get("severity") == "warning"  and i.get("field")})
    summary["must_recheck_fields"] = crit_fields
    summary["flagged_fields"]      = sorted(set(crit_fields) | set(warn_fields))
    summary["critical_count"]      = sum(1 for i in dcf_issues if i.get("severity") == "critical")
    summary["warning_count"]       = sum(1 for i in dcf_issues if i.get("severity") == "warning")

    # Provenance summary
    derived_issues = [i for i in dcf_issues if i.get("data_source") == "derived"]
    scraped_issues = [i for i in dcf_issues if i.get("data_source") == "scraped"]
    anomaly["provenance"] = {
        "derived_flagged":  len(derived_issues),
        "scraped_flagged":  len(scraped_issues),
        "derived_critical": sum(1 for i in derived_issues if i.get("severity") == "critical"),
        "scraped_critical": sum(1 for i in scraped_issues if i.get("severity") == "critical"),
        "non_dcf_dropped":  n_dropped,
    }
    return anomaly


# ---------------------------------------------------------------------------
# Pipeline runner (in a thread so we can stream log lines)
# ---------------------------------------------------------------------------

def _run_pipeline(company: str, log_q: queue.Queue, result_q: queue.Queue):
    """
    Execute the full pipeline and push log lines to log_q.
    When done, push the final 'valued' dict (or an exception) to result_q.
    """
    tool_dir = os.path.dirname(__file__)
    if tool_dir not in sys.path:
        sys.path.insert(0, tool_dir)

    def log(msg, kind="info"):
        log_q.put({"msg": msg, "kind": kind})

    try:
        from scraper.data_engine import run_data_engine
        from scraper.data_qa import run_data_qa
        from scraper.anomaly_detector import run_anomaly_detector
        from scraper.fetchers.fmp import fetch_fmp_crosscheck, is_available as fmp_available
        from scraper.fetchers.comparables import fetch_comparables
        from standardiser.standardiser import run_standardiser
        from canonical_fundamentals.canonical_fundamentals import run_canonical_fundamentals
        from canonical_fundamentals.high_risk_fields import run_high_risk_validation
        from validator.validator import run_validator
        from normaliser.normaliser import run_normaliser
        from classifier.classifier import run_classifier
        from assumption_engine.assumption_engine import run_assumption_engine
        from forecaster.forecaster import run_forecaster
        from valuation_engine.valuation_engine import run_valuation_engine
        from coherence.coherence_engine import run_coherence_engine
        from remediation.remediation_engine import run_remediation_engine
        from explainer.explainer import run_explainer
        from excel_model import build_model

        # Stage 1 — Scrape
        # Cache the data engine result by company name for 1 hour to avoid
        # hammering Yahoo Finance's rate limits on Streamlit Cloud's shared IP.
        @st.cache_data(ttl=3600, show_spinner=False)
        def _cached_data_engine(co):
            return run_data_engine(co)

        log("Resolving ticker and fetching data…", "info")
        data = _cached_data_engine(company)
        if data is None:
            result_q.put(RuntimeError(
                f"No financial data found for '{company}'. "
                "This usually means the company is privately held and not exchange-listed. "
                "The DCF tool only works with publicly traded companies — try a listed peer instead."
            ))
            return
        ticker   = data.get("stats", {}).get("ticker", company)
        co_name  = data.get("stats", {}).get("company_name", "")
        log(f"  ✓  {co_name} ({ticker})", "pass")

        # Stage 1.5 — Data QA
        log("Cross-checking data sources…", "info")
        data = run_data_qa(data)
        n_corrections = len(data.get("data_qa_log", []))
        if n_corrections:
            log(f"  ✓  Data QA  {n_corrections} correction(s) applied", "pass")
        else:
            log("  ✓  Data QA  all sources agree", "pass")

        # Stage 2 — Standardise
        log("Standardising financial data…", "info")
        standardised = run_standardiser(data)
        if standardised["status"] == "fail":
            result_q.put(RuntimeError("Standardiser failed — " + "; ".join(standardised.get("blockers", []))))
            return
        log(f"  ✓  Standardiser  [{standardised['status'].upper()}]", "pass")

        # Stage 2.5 — Canonical Fundamentals
        log("Deriving canonical EBIT and FCF from primitives…", "info")
        standardised = run_canonical_fundamentals(standardised)
        cf_summ = standardised.get("canonical_fundamentals", {}).get("summary", {})
        log(
            f"  ✓  Canonical fundamentals  "
            f"EBIT derived: {cf_summ.get('ebit_used_derived', 0)}/{cf_summ.get('years_processed', 0)} yr(s)  "
            f"FCF derived: {cf_summ.get('fcf_used_derived', 0)}/{cf_summ.get('years_processed', 0)} yr(s)",
            "pass",
        )

        # Stage 2.6 — High-Risk Field Validation
        log("Validating high-risk fields…", "info")
        standardised = run_high_risk_validation(standardised)
        hrv_summ = standardised.get("high_risk_validation", {}).get("summary", {})
        n_crit = hrv_summ.get("critical_count", 0)
        n_warn = hrv_summ.get("warning_count", 0)
        str_rate = hrv_summ.get("smoothed_tax_rate")
        tax_str = f"  smoothed tax {str_rate:.1%}" if str_rate is not None else ""
        kind = "warn" if n_crit else "pass"
        log(
            f"  {'⚠' if n_crit else '✓'}  High-risk fields  "
            f"{n_crit} critical  {n_warn} warning(s){tax_str}",
            kind,
        )

        # Stage 3 — Validate
        log("Validating data quality…", "info")
        validated = run_validator(standardised)
        if validated["status"] == "fail":
            result_q.put(RuntimeError("Validator failed — " + "; ".join(validated.get("blockers", []))))
            return
        qs = validated.get("quality_score", 0)
        log(f"  ✓  Validator  [{validated['status'].upper()}]  data quality {qs:.0f}/100", "pass")

        # Stage 4 — Normalise
        log("Normalising historical metrics…", "info")
        normalised = run_normaliser(standardised, validated)
        if normalised["status"] == "fail":
            result_q.put(RuntimeError("Normaliser failed — " + "; ".join(normalised.get("blockers", []))))
            return
        log(f"  ✓  Normaliser  [{normalised['status'].upper()}]", "pass")

        # Stage 5 — Classify
        log("Classifying business model…", "info")
        classified = run_classifier(normalised)
        if classified["status"] == "fail":
            result_q.put(RuntimeError("Classifier failed — " + "; ".join(classified.get("blockers", []))))
            return
        log(f"  ✓  Classifier  →  {classified.get('classification', '').upper()}", "pass")

        # Stage 5.5 — Anomaly Detection
        log("Checking for data anomalies…", "info")
        anomaly = run_anomaly_detector(classified)
        # Tag each issue with whether the flagged value was derived or scraped
        anomaly = _tag_anomaly_provenance(anomaly, standardised.get("canonical_by_year", {}))
        flagged_fields = anomaly.get("summary", {}).get("must_recheck_fields", [])
        n_anomalies = len(flagged_fields)
        prov = anomaly.get("provenance", {})
        d_flagged  = prov.get("derived_flagged", 0)
        s_flagged  = prov.get("scraped_flagged", 0)
        n_dropped  = prov.get("non_dcf_dropped", 0)
        kind = "warn" if n_anomalies else "pass"
        parts = []
        if d_flagged: parts.append(f"derived: {d_flagged}")
        if s_flagged: parts.append(f"scraped: {s_flagged}")
        if n_dropped: parts.append(f"{n_dropped} non-model issue(s) suppressed")
        detail = ("  " + "  ·  ".join(parts)) if parts else ""
        log(
            f"  {'⚠' if n_anomalies else '✓'}  Anomaly detector  "
            f"{n_anomalies} model field(s) flagged{detail}",
            kind,
        )

        # Stage 5.6 — FMP Cross-Check (conditional)
        if flagged_fields and fmp_available():
            log("FMP cross-check on flagged fields…", "info")
            fmp_result = fetch_fmp_crosscheck(classified["ticker"], flagged_fields)
            classified["fmp_crosscheck"] = fmp_result
            if fmp_result["available"]:
                log(f"  ✓  FMP  {len(fmp_result['by_year'])} year(s) verified", "pass")
            else:
                log(f"  ✓  FMP  not available — {fmp_result['reason']}", "dim")
        else:
            classified["fmp_crosscheck"] = {}

        # Stage 6 — Assume
        log("Building assumption set…", "info")
        assumed = run_assumption_engine(classified)
        if assumed["status"] == "fail":
            result_q.put(RuntimeError("Assumption engine failed — " + "; ".join(assumed.get("blockers", []))))
            return
        log(f"  ✓  Assumptions  [{assumed['status'].upper()}]", "pass")

        # Stage 7 — Forecast
        log("Projecting free cash flows…", "info")
        forecasted = run_forecaster(assumed)
        if forecasted["status"] == "fail":
            result_q.put(RuntimeError("Forecaster failed — " + "; ".join(forecasted.get("blockers", []))))
            return
        log(f"  ✓  Forecast  [{forecasted['status'].upper()}]", "pass")

        # Stage 8 — Value
        log("Calculating enterprise value and VPS…", "info")
        valued = run_valuation_engine(forecasted)
        if valued["status"] == "fail":
            result_q.put(RuntimeError("Valuation engine failed — " + "; ".join(valued.get("blockers", []))))
            return
        vps = valued.get("valuation", {}).get("base", {}).get("value_per_share")
        log(f"  ✓  Valuation  [{valued['status'].upper()}]" +
            (f"  base VPS {valued.get('stats',{}).get('currency','')} {vps:.2f}" if vps else ""), "pass")

        # Stage 9 — Coherence
        log("Running coherence checks…", "info")
        coherence = run_coherence_engine(valued)
        valued["coherence"] = coherence
        n_flags = len(coherence.get("flags", []))
        n_warns = len(coherence.get("warns", []))
        kind = "flag" if n_flags else ("warn" if n_warns else "pass")
        log(f"  {'⚑' if n_flags else '⚠' if n_warns else '✓'}  Coherence  "
            f"{n_flags} flag(s)  {n_warns} warning(s)", kind)

        # Stage 9.5 — Remediation
        log("Applying automatic corrections…", "info")
        valued = run_remediation_engine(valued)
        rem = valued.get("remediation", {})
        applied = [c for c in rem.get("corrections", []) if c.get("applied")]
        ni = rem.get("net_improvement", 0)
        if applied:
            log(f"  ✓  Remediation  {len(applied)} correction(s) applied  "
                f"net {ni:+d} issue(s) resolved", "pass")
        else:
            log("  ✓  Remediation  reviewed — no corrections needed", "pass")

        # Stage 10 — Explain
        log("Generating explanation…", "info")
        explanation = run_explainer(valued)
        valued["explanation"] = explanation
        log(f"  ✓  Explainer  confidence: {explanation.get('overall_confidence','').upper()}", "pass")

        # Stage 10.5 — Comparables
        log("Fetching comparable companies…", "info")
        stats    = valued.get("stats", {})
        sector   = stats.get("sector")
        industry = stats.get("industry")
        comps    = fetch_comparables(valued["ticker"], sector, industry,
                                     stats=stats, valued=valued)
        valued["comparables"] = comps
        if comps["available"]:
            n_peers = len(comps["peers"])
            med = comps["median"]
            ev_ebitda = med.get("ev_ebitda")
            mult_str = f"  EV/EBITDA {ev_ebitda:.1f}x" if ev_ebitda else ""
            log(f"  ✓  Comparables  {n_peers} peers fetched{mult_str}", "pass")
        else:
            log(f"  ✓  Comparables  not available — {comps.get('reason','unknown')}", "dim")

        # Carry forward scraper-level data
        valued["reconciled"]     = data.get("reconciled", {})
        valued["raw"]            = data.get("raw", {})
        valued["fmp_crosscheck"] = classified.get("fmp_crosscheck", {})
        valued["data_qa_log"]    = data.get("data_qa_log", [])

        # Carry forward derived-fundamentals audit data (dropped by normaliser)
        valued["canonical_fundamentals"] = standardised.get("canonical_fundamentals", {})
        valued["high_risk_validation"]   = standardised.get("high_risk_validation", {})

        # Re-run anomaly detector on the final post-remediation state so the quality
        # score reflects the derived EBIT/FCF/net_debt values actually used in the model.
        log("Re-evaluating data quality on final model inputs…", "info")
        final_anomaly = run_anomaly_detector(valued)
        final_anomaly = _tag_anomaly_provenance(final_anomaly, valued.get("canonical_by_year", {}))
        valued["data_quality"] = final_anomaly
        prov = final_anomaly.get("provenance", {})
        n_dropped = prov.get("non_dcf_dropped", 0)
        fqs = final_anomaly.get("quality_score", "—")
        fc  = final_anomaly.get("summary", {}).get("critical_count", 0)
        fw  = final_anomaly.get("summary", {}).get("warning_count", 0)
        fkind = "warn" if fc else "pass"
        log(
            f"  {'⚠' if fc else '✓'}  Final quality score  {fqs}/100  "
            f"·  {fc} critical  {fw} warning(s)"
            + (f"  ·  {n_dropped} non-model issue(s) suppressed" if n_dropped else ""),
            fkind,
        )

        # Excel output
        log("Building Excel model…", "info")
        xl_dir = os.path.join(tool_dir, "xl outputs")
        os.makedirs(xl_dir, exist_ok=True)
        output_path = build_model(valued, output_dir=xl_dir)
        log(f"  ✓  Excel saved:  {os.path.basename(output_path)}", "pass")

        valued["_excel_path"] = output_path
        result_q.put(valued)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log(f"  ✗  Error: {e}", "flag")
        log(f"  Traceback:\n{tb}", "flag")
        result_q.put(RuntimeError(f"{e}\n\nTraceback:\n{tb}"))


# ---------------------------------------------------------------------------
# Render result card
# ---------------------------------------------------------------------------

def _render_result(valued: dict):
    stats       = valued.get("stats", {})
    valuation   = valued.get("valuation", {})
    coherence   = valued.get("coherence", {})
    remediation = valued.get("remediation", {})
    explanation = valued.get("explanation", {})
    assumptions = valued.get("assumptions", {})
    base_val    = valuation.get("base", {})
    bear_val    = valuation.get("bear", {})
    bull_val    = valuation.get("bull", {})

    ccy         = stats.get("currency", "")
    price       = stats.get("current_price")
    vps         = base_val.get("value_per_share")
    impl_ret    = base_val.get("implied_return")
    tv_pct      = base_val.get("terminal_value_pct_ev")
    n_flags     = len(coherence.get("flags", []))
    n_warns     = len(coherence.get("warns", []))
    n_applied   = len([c for c in remediation.get("corrections", []) if c.get("applied")])
    confidence  = explanation.get("overall_confidence", "—").upper()
    conf_colour = {"HIGH": "#a3be8c", "MEDIUM": "#ebcb8b", "LOW": "#bf616a"}.get(confidence, "#eceff4")

    wacc_val = assumptions.get("wacc", {}).get("value")
    tgr_val  = assumptions.get("terminal_growth", {}).get("value")
    wacc_str = f"{wacc_val:.1%}" if wacc_val is not None else "—"
    tgr_str  = f"{tgr_val:.1%}" if tgr_val  is not None else "—"

    if vps is None:
        st.error("Valuation did not produce a per-share value — check the Pipeline & Flags tab in the Excel output.")
        return

    direction_word = "above" if vps > (price or 0) else "below"
    ret_pct = f"{abs(impl_ret):.0%}" if impl_ret is not None else "—"
    arrow   = "▲" if vps > (price or 0) else "▼"
    dir_cls = "up" if vps > (price or 0) else "down"

    badge_html = ""
    if n_flags:
        badge_html += f'<span class="badge-flag">⚑ {n_flags} Flag{"s" if n_flags>1 else ""}</span> '
    if n_warns:
        badge_html += f'<span class="badge-warn">⚠ {n_warns} Warning{"s" if n_warns>1 else ""}</span> '
    if not n_flags and not n_warns:
        badge_html = '<span class="badge-pass">✓ All checks passed</span>'
    if n_applied:
        badge_html += f'<span class="badge-pass" style="background:#1a1e2e;color:#7b8799;">⚙ {n_applied} auto-corrected</span>'

    bear_vps = bear_val.get("value_per_share")
    bull_vps = bull_val.get("value_per_share")

    # Build one-liner
    one_liner = explanation.get("one_liner", "")

    st.markdown(f"""
<div class="result-card">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;">
    <div>
      <div style="font-size:0.8rem;color:#7b8799;margin-bottom:4px;">
        {stats.get('company_name','')} &nbsp;·&nbsp; {stats.get('ticker','')} &nbsp;·&nbsp; {ccy}
      </div>
      <div class="vps-big">{ccy} {vps:.2f}</div>
      <div class="vps-sub">Base case intrinsic value per share</div>
    </div>
    <div style="text-align:right;">
      <div style="font-size:0.75rem;color:#7b8799;">Confidence</div>
      <div style="font-size:1.1rem;font-weight:700;color:{conf_colour};">{confidence}</div>
    </div>
  </div>

  <div class="metric-row">
    <div class="metric-block">
      <div class="metric-label">Market Price</div>
      <div class="metric-value">{f"{ccy} {price:.2f}" if price is not None else "—"}</div>
    </div>
    <div class="metric-block">
      <div class="metric-label">Implied Return</div>
      <div class="metric-value {dir_cls}">{arrow} {ret_pct} {direction_word} market</div>
    </div>
    <div class="metric-block">
      <div class="metric-label">Terminal Value %</div>
      <div class="metric-value">{f"{tv_pct:.0%}" if tv_pct is not None else "—"}</div>
    </div>
    <div class="metric-block">
      <div class="metric-label">Checks</div>
      <div class="metric-value" style="margin-top:4px;">{badge_html}</div>
    </div>
  </div>

  <div class="scenario-row">
    <div class="scenario-cell">
      <div class="scenario-name">Bear case</div>
      <div class="scenario-vps">{f"{ccy} {bear_vps:.2f}" if bear_vps is not None else "—"}</div>
    </div>
    <div class="scenario-cell" style="border:1px solid #3b4a6b;">
      <div class="scenario-name" style="color:#88c0d0;">Base case</div>
      <div class="scenario-vps" style="color:#88c0d0;">{ccy} {vps:.2f}</div>
    </div>
    <div class="scenario-cell">
      <div class="scenario-name">Bull case</div>
      <div class="scenario-vps">{f"{ccy} {bull_vps:.2f}" if bull_vps is not None else "—"}</div>
    </div>
  </div>

  <div style="margin-top:14px;padding-top:14px;border-top:1px solid #2e3440;display:flex;gap:32px;">
    <div class="metric-block">
      <div class="metric-label">WACC</div>
      <div class="metric-value">{wacc_str}</div>
    </div>
    <div class="metric-block">
      <div class="metric-label">Terminal Growth</div>
      <div class="metric-value">{tgr_str}</div>
    </div>
  </div>

  <div style="margin-top:14px;padding-top:14px;border-top:1px solid #2e3440;
              font-size:0.82rem;color:#7b8799;line-height:1.5;">
    {one_liner}
  </div>
</div>
""", unsafe_allow_html=True)

    # Download button — prominent call to action
    excel_path = valued.get("_excel_path")
    if excel_path and os.path.exists(excel_path):
        st.markdown(
            "<div style='background:#1a2e1a;border:1px solid #a3be8c;border-radius:8px;"
            "padding:14px 18px;margin:16px 0 4px;display:flex;align-items:center;gap:12px;'>"
            "<div style='font-size:1.4rem;'>📊</div>"
            "<div><div style='font-size:0.9rem;font-weight:700;color:#a3be8c;'>"
            "Full model available to download</div>"
            "<div style='font-size:0.78rem;color:#7b8799;margin-top:2px;'>"
            "10-year forecast · Bear / Base / Bull scenarios · Source comparison · "
            "Data audit · Assumptions · Comparables — all in one spreadsheet</div></div>"
            "</div>",
            unsafe_allow_html=True,
        )
        with open(excel_path, "rb") as fh:
            st.download_button(
                label="⬇  Download Excel Model",
                data=fh.read(),
                file_name=os.path.basename(excel_path),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
            )

    # Derived Fundamentals section
    cf   = valued.get("canonical_fundamentals", {})
    hrv  = valued.get("high_risk_validation", {})
    cf_s = cf.get("summary", {})
    hrv_s = hrv.get("summary", {})
    cf_years = cf_s.get("years_processed", 0)

    if cf_years:
        ebit_d  = cf_s.get("ebit_used_derived", 0)
        fcf_d   = cf_s.get("fcf_used_derived", 0)
        ebitda_comps = [c for c in cf.get("comparisons", []) if c["field"] == "ebitda"]
        ebitda_d = sum(1 for c in ebitda_comps if c.get("chosen_source") == "derived")
        nd_yrs  = hrv_s.get("derived_net_debt_years", 0)

        def _cov(n, total):
            colour = "#a3be8c" if n == total else "#ebcb8b" if n > 0 else "#bf616a"
            return f"<span style='color:{colour};font-weight:600;'>{n}/{total}</span>"

        st.markdown(
            f"<div style='font-size:0.78rem;color:#7b8799;margin-top:4px;padding:8px 0;'>"
            f"Derived fundamentals used &nbsp;·&nbsp; "
            f"EBIT {_cov(ebit_d, cf_years)} yrs &nbsp;·&nbsp; "
            f"FCF {_cov(fcf_d, cf_years)} yrs &nbsp;·&nbsp; "
            f"EBITDA {_cov(ebitda_d, cf_years)} yrs &nbsp;·&nbsp; "
            f"Net Debt rebuilt {_cov(nd_yrs, cf_years)} yrs"
            f"</div>",
            unsafe_allow_html=True,
        )

        year_reports = cf.get("year_reports", {})
        if year_reports:
            with st.expander("Derived Fundamentals — year-by-year source", expanded=False):
                # Build anomaly lookup: (year_as_str, canonical_field) -> worst severity
                _FIELD_MAP = {
                    "ebit_comparison":   "ebit",
                    "fcf_comparison":    "free_cash_flow",
                    "ebitda_comparison": "ebitda",
                }
                _SEV_RANK = {"critical": 2, "warning": 1}
                _anomaly_issues = valued.get("data_quality", {}).get("issues", [])
                _anomaly_lookup: dict = {}
                for _iss in _anomaly_issues:
                    _key = (str(_iss.get("year", "")), _iss.get("field", ""))
                    if _key not in _anomaly_lookup:
                        _anomaly_lookup[_key] = []
                    _anomaly_lookup[_key].append(_iss)

                def _anomaly_cell(year, field_key):
                    canon = _FIELD_MAP.get(field_key, "")
                    issues_for = _anomaly_lookup.get((str(year), canon), [])
                    if not issues_for:
                        return "✓", "#a3be8c"
                    worst = max(issues_for, key=lambda x: _SEV_RANK.get(x.get("severity", ""), 0))
                    sev  = worst.get("severity", "flag")
                    dsrc = worst.get("data_source", "")
                    tag  = f" ({dsrc})" if dsrc and dsrc != "unknown" else ""
                    if sev == "critical":
                        return f"⚑ critical{tag}", "#bf616a"
                    return f"⚠ warning{tag}", "#ebcb8b"

                def _fv(v):
                    if v is None: return "—"
                    try:
                        v = float(v)
                        if abs(v) >= 1e9:  return f"{v/1e9:.1f}B"
                        if abs(v) >= 1e6:  return f"{v/1e6:.0f}M"
                        return f"{v:,.0f}"
                    except: return "—"

                rows_html = ""
                for year in sorted(year_reports.keys(), reverse=True):
                    rpt = year_reports[year]
                    for field_key, label in [
                        ("ebit_comparison",   "EBIT"),
                        ("fcf_comparison",    "FCF"),
                        ("ebitda_comparison", "EBITDA"),
                    ]:
                        comp = rpt.get(field_key, {})
                        if not comp:
                            continue
                        src    = comp.get("chosen_source", "—")
                        dv     = comp.get("derived_value")
                        sv     = comp.get("scraped_value")
                        cv     = comp.get("chosen_value")
                        pct    = comp.get("difference_pct")
                        method = comp.get("derived_method", "—")
                        d_conf = comp.get("derived_confidence", 0)
                        s_conf = comp.get("scraped_confidence", 0)
                        lowc   = comp.get("low_confidence_warning", False)

                        src_colour = "#a3be8c" if src == "derived" else "#ebcb8b"
                        warn_icon  = " ⚠" if lowc else ""
                        anom_text, anom_colour = _anomaly_cell(year, field_key)

                        pct_str = f"{pct:.0f}%" if pct is not None else "—"
                        rows_html += (
                            f"<tr style='border-bottom:1px solid #1e2535;'>"
                            f"<td style='padding:5px 8px;color:#7b8799;'>{year}</td>"
                            f"<td style='padding:5px 8px;color:#eceff4;'>{label}</td>"
                            f"<td style='padding:5px 8px;color:{src_colour};font-weight:600;'>{src.upper()}{warn_icon}</td>"
                            f"<td style='padding:5px 8px;text-align:right;'>{_fv(cv)}</td>"
                            f"<td style='padding:5px 8px;text-align:right;color:#636e7b;'>{_fv(dv)}</td>"
                            f"<td style='padding:5px 8px;text-align:right;color:#636e7b;'>{_fv(sv)}</td>"
                            f"<td style='padding:5px 8px;text-align:right;color:#7b8799;'>{pct_str}</td>"
                            f"<td style='padding:5px 8px;text-align:right;color:#636e7b;font-size:0.75rem;'>{d_conf} / {s_conf}</td>"
                            f"<td style='padding:5px 8px;color:#636e7b;font-size:0.75rem;'>{method}</td>"
                            f"<td style='padding:5px 8px;color:{anom_colour};font-size:0.75rem;'>{anom_text}</td>"
                            f"</tr>"
                        )

                st.markdown(
                    f"<table style='width:100%;border-collapse:collapse;font-size:0.8rem;'>"
                    f"<thead><tr style='border-bottom:1px solid #2e3440;'>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Year</th>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Field</th>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Chosen</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Value used</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Derived</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Scraped</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Gap</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Conf D/S</th>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Method</th>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Anomaly</th>"
                    f"</tr></thead>"
                    f"<tbody>{rows_html}</tbody>"
                    f"</table>",
                    unsafe_allow_html=True,
                )

    # ── Expander 1: Comparable Companies ─────────────────────────────────────
    comps = valued.get("comparables", {})
    if comps.get("available"):
        med   = comps.get("median", {})
        impl  = comps.get("implied_from_peers", {})
        rng   = impl.get("vps_range", [])
        peers = comps.get("peers", [])

        def _mx(v): return f"{v:.1f}×" if v is not None else "—"

        vps_range_str = f"{ccy} {rng[0]:.2f} – {rng[2]:.2f}" if rng and len(rng) == 3 else ""
        comp_label = (
            f"Comparable Companies — {len(peers)} peers · peer-implied {vps_range_str}"
            if vps_range_str else f"Comparable Companies — {len(peers)} peers"
        )
        with st.expander(comp_label, expanded=True):

            # VPS comparison banner
            if rng and len(rng) == 3:
                dcf_vs_peer = vps - rng[1] if vps else None
                vs_str = ""
                if dcf_vs_peer is not None:
                    vs_str = (
                        f"<div style='margin-left:auto;text-align:right;'>"
                        f"<div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;letter-spacing:0.05em;'>DCF vs peer mid</div>"
                        f"<div style='font-size:1.0rem;font-weight:600;color:{'#a3be8c' if dcf_vs_peer >= 0 else '#bf616a'};'>"
                        f"{'▲' if dcf_vs_peer >= 0 else '▼'} {ccy} {abs(dcf_vs_peer):.2f} {'premium' if dcf_vs_peer >= 0 else 'discount'}"
                        f"</div></div>"
                    )
                st.markdown(
                    f"<div style='display:flex;gap:24px;align-items:flex-end;padding:10px 0 14px;border-bottom:1px solid #2e3440;'>"
                    f"<div><div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;letter-spacing:0.05em;'>Peer-implied low</div>"
                    f"<div style='font-size:1.1rem;font-weight:600;color:#eceff4;'>{ccy} {rng[0]:.2f}</div></div>"
                    f"<div><div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;letter-spacing:0.05em;'>Peer-implied mid</div>"
                    f"<div style='font-size:1.1rem;font-weight:600;color:#88c0d0;'>{ccy} {rng[1]:.2f}</div></div>"
                    f"<div><div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;letter-spacing:0.05em;'>Peer-implied high</div>"
                    f"<div style='font-size:1.1rem;font-weight:600;color:#eceff4;'>{ccy} {rng[2]:.2f}</div></div>"
                    f"<div><div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;letter-spacing:0.05em;'>Our DCF</div>"
                    f"<div style='font-size:1.1rem;font-weight:600;color:#a3be8c;'>{ccy} {vps:.2f}</div></div>"
                    f"{vs_str}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            if peers:
                rows_html = ""
                for p in peers:
                    rows_html += (
                        f"<tr style='border-bottom:1px solid #1e2535;'>"
                        f"<td style='padding:6px 8px;color:#eceff4;font-weight:600;'>{p.get('ticker','—')}</td>"
                        f"<td style='padding:6px 8px;color:#7b8799;font-size:0.8rem;'>{p.get('name','')[:30]}</td>"
                        f"<td style='padding:6px 8px;text-align:right;'>{_mx(p.get('ev_ebitda'))}</td>"
                        f"<td style='padding:6px 8px;text-align:right;'>{_mx(p.get('ev_ebit'))}</td>"
                        f"<td style='padding:6px 8px;text-align:right;'>{_mx(p.get('pe'))}</td>"
                        f"</tr>"
                    )
                rows_html += (
                    f"<tr style='border-top:2px solid #2e3440;font-weight:700;'>"
                    f"<td style='padding:6px 8px;color:#88c0d0;' colspan='2'>Peer median</td>"
                    f"<td style='padding:6px 8px;text-align:right;color:#88c0d0;'>{_mx(med.get('ev_ebitda'))}</td>"
                    f"<td style='padding:6px 8px;text-align:right;color:#88c0d0;'>{_mx(med.get('ev_ebit'))}</td>"
                    f"<td style='padding:6px 8px;text-align:right;color:#88c0d0;'>{_mx(med.get('pe'))}</td>"
                    f"</tr>"
                )
                st.markdown(
                    f"<table style='width:100%;border-collapse:collapse;font-size:0.82rem;margin-top:12px;'>"
                    f"<thead><tr style='border-bottom:1px solid #2e3440;'>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Ticker</th>"
                    f"<th style='padding:4px 8px;text-align:left;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>Company</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>EV/EBITDA</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>EV/EBIT</th>"
                    f"<th style='padding:4px 8px;text-align:right;color:#7b8799;font-weight:400;font-size:0.72rem;text-transform:uppercase;'>P/E</th>"
                    f"</tr></thead>"
                    f"<tbody>{rows_html}</tbody>"
                    f"</table>",
                    unsafe_allow_html=True,
                )

    # ── Expander 2: Analysis ──────────────────────────────────────────────────
    exec_summary  = explanation.get("executive_summary", [])
    value_drivers = explanation.get("value_drivers", [])
    agenda        = explanation.get("review_agenda", [])
    agenda_issues = [a for a in agenda if a.get("status") != "PASS"]

    # Pull useful context for the prose
    _stats     = valued.get("stats", {})
    _norm      = valued.get("normalised", {})
    _co        = valued.get("company_name", "")
    _sector    = _stats.get("sector", "")
    _country   = _stats.get("country", "")
    _industry  = _stats.get("industry", "")
    _classif   = valued.get("classification", "").replace("_", " ")
    _rev       = _norm.get("revenue")
    _ebit      = _norm.get("ebit")
    _ebit_m    = _norm.get("ebit_margin")
    _rev_cagr  = _norm.get("revenue_cagr_3yr") or _norm.get("revenue_cagr")
    _fcf_m     = _norm.get("fcf_margin")
    _dq        = valued.get("data_quality", {}).get("quality_score")

    def _b(v):
        if v is None: return "—"
        try:
            v = float(v)
            if abs(v) >= 1e9: return f"{ccy} {v/1e9:.1f}B"
            if abs(v) >= 1e6: return f"{ccy} {v/1e6:.0f}M"
            return f"{ccy} {v:,.0f}"
        except: return "—"

    with st.expander("Analysis", expanded=True):

        # — About the company —
        st.markdown(
            "<div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;"
            "letter-spacing:0.06em;margin-bottom:6px;'>About the company</div>",
            unsafe_allow_html=True,
        )
        about_parts = []
        if _co:       about_parts.append(f"**{_co}**")
        if _country:  about_parts.append(f"is headquartered in **{_country}**")
        if _sector:   about_parts.append(f"and operates in the **{_sector}** sector")
        if _industry and _industry != _sector:
            about_parts.append(f"({_industry})")
        if _classif:  about_parts.append(f"— classified as a **{_classif}** business")
        if about_parts:
            st.markdown(" ".join(about_parts) + ".")

        # Use the first exec summary bullet if it has useful company context
        if exec_summary:
            st.markdown(
                f"<div style='font-size:0.85rem;color:#b0bec5;line-height:1.6;"
                f"margin-top:4px;'>{exec_summary[0]}</div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='margin:14px 0 6px;border-top:1px solid #2e3440;'></div>",
                    unsafe_allow_html=True)

        # — Financial picture —
        st.markdown(
            "<div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;"
            "letter-spacing:0.06em;margin-bottom:6px;'>Financial picture</div>",
            unsafe_allow_html=True,
        )
        fin_lines = []
        if _rev:
            fin_lines.append(f"Revenue stands at **{_b(_rev)}**")
            if _rev_cagr is not None:
                direction = "growing" if _rev_cagr > 0 else "contracting"
                fin_lines[-1] += f", {direction} at **{abs(_rev_cagr):.1%} per year** over the last three years"
            fin_lines[-1] += "."
        if _ebit and _ebit_m:
            profitability = (
                "strong" if _ebit_m > 0.20 else
                "healthy" if _ebit_m > 0.10 else
                "modest" if _ebit_m > 0.03 else "thin"
            )
            fin_lines.append(
                f"Operating profit (EBIT) is **{_b(_ebit)}**, representing a "
                f"**{_ebit_m:.1%} margin** — {profitability} for the sector."
            )
        if _fcf_m is not None:
            cash_quality = (
                "excellent cash conversion" if _fcf_m > 0.10 else
                "solid cash generation" if _fcf_m > 0.05 else
                "moderate free cash flow" if _fcf_m > 0 else "negative free cash flow"
            )
            fin_lines.append(
                f"Free cash flow margin is **{_fcf_m:.1%}**, indicating {cash_quality}."
            )
        # Value drivers from explainer
        if value_drivers:
            fin_lines.append("")
            fin_lines.append("**What is driving the valuation:**")
            for d in value_drivers[:4]:
                fin_lines.append(f"- {d}")

        for line in fin_lines:
            st.markdown(
                f"<div style='font-size:0.85rem;color:#b0bec5;line-height:1.6;'>{line}</div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='margin:14px 0 6px;border-top:1px solid #2e3440;'></div>",
                    unsafe_allow_html=True)

        # — Valuation view —
        st.markdown(
            "<div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;"
            "letter-spacing:0.06em;margin-bottom:6px;'>Valuation view</div>",
            unsafe_allow_html=True,
        )
        for bullet in exec_summary[1:]:
            st.markdown(
                f"<div style='font-size:0.85rem;color:#b0bec5;line-height:1.6;"
                f"margin-bottom:6px;'>{bullet}</div>",
                unsafe_allow_html=True,
            )

        # Peer context if available
        if comps and comps.get("available") and rng and len(rng) == 3:
            peer_tickers = ", ".join(p.get("ticker", "") for p in comps.get("peers", [])[:5])
            dcf_in_range = rng[0] <= vps <= rng[2] if vps else False
            range_comment = (
                "consistent with what peers suggest"
                if dcf_in_range else
                f"{'above' if vps and vps > rng[2] else 'below'} the peer-implied range"
            )
            st.markdown(
                f"<div style='font-size:0.85rem;color:#b0bec5;line-height:1.6;margin-bottom:6px;'>"
                f"Against comparable companies ({peer_tickers}), the peer-implied share price range is "
                f"**{ccy} {rng[0]:.2f} – {rng[2]:.2f}**. Our DCF base case of **{ccy} {vps:.2f}** "
                f"is {range_comment}.</div>",
                unsafe_allow_html=True,
            )

        # Flags / things to watch
        if agenda_issues:
            st.markdown("<div style='margin:14px 0 6px;border-top:1px solid #2e3440;'></div>",
                        unsafe_allow_html=True)
            st.markdown(
                "<div style='font-size:0.72rem;color:#7b8799;text-transform:uppercase;"
                "letter-spacing:0.06em;margin-bottom:6px;'>Things to sense-check</div>",
                unsafe_allow_html=True,
            )
            for item in agenda_issues[:4]:
                colour = "#bf616a" if item["status"] == "FLAG" else "#ebcb8b"
                icon   = "⚑" if item["status"] == "FLAG" else "⚠"
                st.markdown(
                    f"<div style='font-size:0.83rem;color:#b0bec5;line-height:1.6;"
                    f"margin-bottom:4px;'>{icon} <span style='color:{colour};font-weight:600;'>"
                    f"{item['item']}</span> — {item['action']}</div>",
                    unsafe_allow_html=True,
                )


# ---------------------------------------------------------------------------
# Main run logic
# ---------------------------------------------------------------------------

if run_clicked and company_input.strip():

    log_placeholder  = st.empty()
    result_container = st.container()

    log_q    = queue.Queue()
    result_q = queue.Queue()
    log_lines: list[dict] = []

    thread = threading.Thread(
        target=_run_pipeline,
        args=(company_input.strip(), log_q, result_q),
        daemon=True,
    )
    thread.start()

    _KIND_CLASS = {
        "pass": "log-pass",
        "warn": "log-warn",
        "flag": "log-flag",
        "info": "log-info",
        "dim":  "log-dim",
    }

    def _render_log(lines):
        html_lines = []
        for entry in lines:
            cls = _KIND_CLASS.get(entry["kind"], "log-dim")
            msg = entry["msg"].replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append(f'<span class="{cls}">{msg}</span>')
        log_placeholder.markdown(
            '<div class="log-box">' + "<br>".join(html_lines) + "</div>",
            unsafe_allow_html=True,
        )

    # Poll until thread done, streaming log lines
    while thread.is_alive() or not log_q.empty():
        updated = False
        try:
            while True:
                item = log_q.get_nowait()
                log_lines.append(item)
                updated = True
        except queue.Empty:
            pass
        if updated:
            _render_log(log_lines)
        thread.join(timeout=0.25)

    _render_log(log_lines)

    # Get result
    if not result_q.empty():
        result = result_q.get()
        if isinstance(result, Exception):
            with result_container:
                st.error(f"Pipeline error: {result}")
        else:
            with result_container:
                _render_result(result)

elif run_clicked and not company_input.strip():
    st.warning("Please enter a company name or ticker.")
