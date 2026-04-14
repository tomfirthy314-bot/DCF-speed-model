import sys
import shutil
import os
from scraper.data_engine import run_data_engine
from scraper.data_qa import run_data_qa, print_data_qa_report
from excel_model import build_model
from standardiser.standardiser import run_standardiser, print_standardiser_report
from canonical_fundamentals.canonical_fundamentals import (
    run_canonical_fundamentals, print_canonical_fundamentals_report,
)
from canonical_fundamentals.high_risk_fields import (
    run_high_risk_validation, print_high_risk_report,
)
from validator.validator import run_validator, print_validator_report
from normaliser.normaliser import run_normaliser, print_normaliser_report
from classifier.classifier import run_classifier, print_classifier_report
from scraper.anomaly_detector import run_anomaly_detector, print_anomaly_report
from scraper.fetchers.fmp import fetch_fmp_crosscheck, is_available as fmp_available
from scraper.fetchers.comparables import fetch_comparables
from assumption_engine.assumption_engine import run_assumption_engine, print_assumption_report
from forecaster.forecaster import run_forecaster, print_forecaster_report
from valuation_engine.valuation_engine import run_valuation_engine, print_valuation_report
from coherence.coherence_engine import run_coherence_engine, print_coherence_report
from remediation.remediation_engine import run_remediation_engine, print_remediation_report
from explainer.explainer import run_explainer, print_explainer_report

# ---------------------------------------------------------------------------
# Development stage tracker
# Update this constant as each new sprint is completed so dev_outputs snapshots
# are saved under the correct stage folder automatically.
# ---------------------------------------------------------------------------
CURRENT_STAGE = "09_remediation_engine"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 run.py \"Company Name or Ticker\"")
        sys.exit(1)

    company_input = sys.argv[1]

    # --- Stage 1: Scrape ---
    data = run_data_engine(company_input)
    if data is None:
        sys.exit(1)

    # --- Stage 1.5: Data QA — cross-source reconciliation ---
    data = run_data_qa(data)
    print_data_qa_report(data)

    # --- Stage 2: Standardise ---
    standardised = run_standardiser(data)
    print_standardiser_report(standardised)

    if standardised["status"] == "fail":
        print("  Standardiser FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 2.5: Canonical Fundamentals ---
    # Derive EBIT, EBITDA, and FCF from primitives and compare against scraped values.
    # Updates canonical_by_year with the higher-confidence version of each field.
    standardised = run_canonical_fundamentals(standardised)
    print_canonical_fundamentals_report(standardised)

    # --- Stage 2.6: High-Risk Field Validation ---
    # Validates and re-derives the financial fields most frequently wrong on free websites:
    #   Gross Profit (mapping collisions), Net Debt (rebuild from components),
    #   Shares (basic vs diluted, staleness), Tax Rate (smooth across years),
    #   NWC (balance sheet derivation + OCF direction cross-check),
    #   OCF/Capex supplemental checks, EV multiples rebuilt from validated components.
    # Non-blocking: pipeline continues regardless of findings.
    # Adds derived_net_debt, derived_nwc, smoothed_tax_rate to canonical_by_year.
    standardised = run_high_risk_validation(standardised)
    print_high_risk_report(standardised)

    # --- Stage 3: Validate ---
    validated = run_validator(standardised)
    print_validator_report(validated)

    if validated["status"] == "fail":
        print("  Validator FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 4: Normalise ---
    normalised = run_normaliser(standardised, validated)
    print_normaliser_report(normalised)

    if normalised["status"] == "fail":
        print("  Normaliser FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 5: Classify ---
    classified = run_classifier(normalised)
    print_classifier_report(classified)

    if classified["status"] == "fail":
        print("  Classifier FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 5.5: Anomaly Detection ---
    anomaly = run_anomaly_detector(classified)
    print_anomaly_report(anomaly)
    # Non-blocking: pipeline continues regardless of anomaly score

    # --- Stage 5.6: FMP Cross-Check (conditional) ---
    flagged_fields = anomaly.get("summary", {}).get("must_recheck_fields", [])
    if flagged_fields and fmp_available():
        print("FMP Cross-Check — verifying flagged fields...")
        fmp_result = fetch_fmp_crosscheck(classified["ticker"], flagged_fields)
        classified["fmp_crosscheck"] = fmp_result
        if fmp_result["available"]:
            print(f"  FMP returned data for {len(fmp_result['by_year'])} year(s). "
                  f"Fields covered: {', '.join(fmp_result['fields_covered'])}")
            print(f"  API calls used: {fmp_result['calls_made']}")
        else:
            print(f"  FMP not available — {fmp_result['reason']}")
        print()
    elif flagged_fields and not fmp_available():
        print(f"  FMP cross-check skipped — set FMP_API_KEY to enable. "
              f"({len(flagged_fields)} field(s) flagged: "
              f"{', '.join(flagged_fields[:5])})\n")

    # --- Stage 6: Assume ---
    assumed = run_assumption_engine(classified)
    print_assumption_report(assumed)

    if assumed["status"] == "fail":
        print("  Assumption Engine FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 7: Forecast ---
    forecasted = run_forecaster(assumed)
    print_forecaster_report(forecasted)

    if forecasted["status"] == "fail":
        print("  Forecaster FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 8: Value ---
    valued = run_valuation_engine(forecasted)
    print_valuation_report(valued)

    if valued["status"] == "fail":
        print("  Valuation Engine FAILED — pipeline blocked. Resolve blockers before continuing.\n")
        sys.exit(1)

    # --- Stage 9: Coherence / Reality Check ---
    coherence = run_coherence_engine(valued)
    valued["coherence"] = coherence
    print_coherence_report(coherence, valued)

    # --- Stage 9.5: Remediation ---
    # Attempt to automatically correct flagged and warned issues.
    # Returns an updated valued dict — assumptions, forecast, valuation, and
    # coherence are all replaced with corrected versions where fixes were applied.
    valued = run_remediation_engine(valued)
    print_remediation_report(valued, valued)

    # --- Stage 10: Explainer ---
    # Runs against the post-remediation state so the narrative reflects corrected values.
    explanation = run_explainer(valued)
    valued["explanation"] = explanation
    print_explainer_report(explanation, valued)

    # --- Stage 10.5: Comparables ---
    print("Fetching comparable companies...")
    stats    = valued.get("stats", {})
    sector   = stats.get("sector")
    industry = stats.get("industry")
    comps    = fetch_comparables(valued["ticker"], sector, industry,
                                 stats=stats, valued=valued)
    valued["comparables"] = comps
    if comps["available"]:
        print(f"  Peers fetched : {len(comps['peers'])} companies")
        med = comps["median"]
        def _mx(v): return f"{v:.1f}x" if v is not None else "n/a"
        print(f"  Peer medians  : EV/Rev {_mx(med.get('ev_revenue'))}  "
              f"EV/EBITDA {_mx(med.get('ev_ebitda'))}  "
              f"EV/EBIT {_mx(med.get('ev_ebit'))}")
        impl = comps.get("implied_from_peers", {})
        rng  = impl.get("vps_range", [])
        if rng:
            ccy = stats.get("currency", "")
            print(f"  Peer-implied VPS range: {ccy} {rng[0]:.2f} — {rng[2]:.2f}")
    else:
        print(f"  Comparables not available — {comps.get('reason', 'unknown')}")
    print()

    # --- Carry forward scraper-level data that pipeline stages drop ---
    valued["reconciled"]     = data.get("reconciled", {})
    valued["raw"]            = data.get("raw", {})
    valued["fmp_crosscheck"] = classified.get("fmp_crosscheck", {})
    valued["data_qa_log"]    = data.get("data_qa_log", [])

    # --- Re-run anomaly detector on final model inputs ---
    # The first run (Stage 5.5) scored raw scraped data. Now that canonical_fundamentals
    # has replaced EBIT/FCF/net_debt with derived values, re-evaluate so the final
    # quality score and issue list reflect what actually goes into the DCF.
    print("Re-evaluating data quality on final model inputs…")
    final_anomaly = run_anomaly_detector(valued)
    print(f"  Final quality score: {final_anomaly.get('quality_score', '—')}/100  "
          f"({final_anomaly.get('summary', {}).get('critical_count', 0)} critical  "
          f"{final_anomaly.get('summary', {}).get('warning_count', 0)} warning(s))\n")
    valued["data_quality"] = final_anomaly

    # --- Excel output ---
    print("Building Excel model...")
    xl_dir = os.path.join(os.path.dirname(__file__), "xl outputs")
    os.makedirs(xl_dir, exist_ok=True)
    output_path = build_model(valued, output_dir=xl_dir)

    # --- Dev snapshot: copy to dev_outputs/ with stage label prepended ---
    dev_dir = os.path.join(os.path.dirname(__file__), "dev_outputs")
    os.makedirs(dev_dir, exist_ok=True)
    snapshot_name = f"{CURRENT_STAGE} - {os.path.basename(output_path)}"
    snapshot_path = os.path.join(dev_dir, snapshot_name)
    shutil.copy2(output_path, snapshot_path)
    print(f"  Dev snapshot saved: dev_outputs/{snapshot_name}\n")
