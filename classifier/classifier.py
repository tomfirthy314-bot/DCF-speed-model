"""
Sprint 2B — Classifier

Purpose:
    Assign the company to the right modelling template based on its sector,
    industry, and observed financial profile. Classification drives which
    forecast logic and default assumptions are used in later stages.

    Every signal is logged so the rationale is fully explainable. If the
    financial profile conflicts with the sector hint, the conflict is flagged
    rather than silently resolved.

Inputs:
    normalised output from normaliser.run_normaliser()
    (which carries stats, canonical_by_year, and normalised metrics forward)

Outputs:
    {
        "status":          "pass" | "pass_with_caution" | "fail",
        "classification":  str,     # e.g. "asset_light", "industrial"
        "label":           str,     # human-readable label
        "template":        str,     # modelling template to use downstream
        "confidence":      str,     # "high" | "medium" | "low"
        "rationale":       str,     # plain-English explanation
        "signal_log":      [...],   # every signal that voted, with its weight
        "scores":          {...},   # raw scores per class
        "conflicts":       [...],   # sector vs financial profile conflicts
        "blockers":        [...],
        "warnings":        [...],
        "ticker":          str,
        "company_name":    str,
        "stats":           {...},   # pass-through
        "canonical_by_year": {...}, # pass-through
        "normalised":      {...},   # pass-through
        "base_year":       str,     # pass-through
        "quality_score":   float,   # pass-through
        "metadata":        {...},
    }

Classification buckets:
    asset_light   — SaaS, consulting, pharma, professional services
                    Signals: high gross margin, low capex, no inventory
    industrial    — Aerospace, defence, engineering, chemicals, auto
                    Signals: high capex, significant inventory, moderate margins
    consumer      — Retail, FMCG, food & beverage, apparel
                    Signals: inventory present, moderate margins, brand-driven
    resources     — Oil & gas, mining, metals, commodities
                    Signals: very high capex, volatile/low margins, commodity exposure
    utilities     — Power, water, telecoms infrastructure, REITs
                    Signals: very high capex, stable revenue, regulated returns
    hybrid        — Conglomerates, mixed models
                    Assigned when no single class wins clearly

Unsupported (pipeline exits cleanly):
    financials    — Banks, insurance. Require DDM or P/BV, not DCF.

Scoring system:
    Each signal awards points to one or more classes.
    Winner = highest total score.
    Confidence: HIGH if lead ≥ 4pts and total ≥ 6pts
                MEDIUM if lead ≥ 2pts
                LOW otherwise (assigned "hybrid" if lead < 2pts)
"""

from datetime import datetime
import statistics


# ---------------------------------------------------------------------------
# Bucket definitions
# ---------------------------------------------------------------------------

BUCKETS: dict[str, dict] = {
    "asset_light": {
        "label":    "Asset-light (software / services / pharma)",
        "template": "asset_light",
        "description": (
            "Low capex intensity, high gross margins, minimal or no inventory. "
            "Revenue is driven by intellectual property, subscriptions, or people. "
            "Typical: SaaS, consulting, pharmaceuticals (ex-manufacturing), "
            "professional services, media."
        ),
    },
    "industrial": {
        "label":    "Industrial / manufacturing",
        "template": "industrial",
        "description": (
            "Capital-intensive with significant physical assets and inventory. "
            "Margins are moderate and working capital is driven by production cycles. "
            "Typical: aerospace, defence, engineering, chemicals, automotive, capital goods."
        ),
    },
    "consumer": {
        "label":    "Consumer / retail",
        "template": "consumer",
        "description": (
            "Brand-driven with inventory cycles and moderate capex. "
            "Revenue linked to consumer spending and distribution reach. "
            "Typical: retail, FMCG, food & beverage, apparel, leisure."
        ),
    },
    "resources": {
        "label":    "Resources / commodity",
        "template": "resources",
        "description": (
            "Commodity price exposure with very high capex and volatile margins. "
            "Revenue is largely volume × price. "
            "Typical: oil & gas, mining, metals & materials."
        ),
    },
    "utilities": {
        "label":    "Utilities / regulated infrastructure",
        "template": "utilities",
        "description": (
            "Stable, often regulated revenue with very high capex and long asset lives. "
            "Returns are bounded by regulation or long-term contracts. "
            "Typical: power, water, gas distribution, telecoms infrastructure, REITs."
        ),
    },
    "hybrid": {
        "label":    "Hybrid / conglomerate",
        "template": "hybrid",
        "description": (
            "Multiple business lines that do not fit cleanly into one template. "
            "Requires analyst judgement on blended assumptions. "
            "Typical: diversified industrials, mixed technology-services groups."
        ),
    },
}

# Sectors where a standard DCF is not appropriate — exit cleanly
UNSUPPORTED: dict[str, str] = {
    "Financial Services": (
        "Banks and insurance companies are not supported by this DCF engine. "
        "They require dividend discount models (DDM) or price-to-book analysis. "
        "Route to a specialist financial-sector model."
    ),
    "Financials": (
        "Banks and insurance companies are not supported by this DCF engine. "
        "They require dividend discount models (DDM) or price-to-book analysis."
    ),
}

# Sector → likely classification (used as a prior, not a hard rule)
SECTOR_HINTS: dict[str, str] = {
    "Technology":              "asset_light",
    "Communication Services":  "asset_light",
    "Healthcare":              "asset_light",   # confirmed/overridden by capex check
    "Industrials":             "industrial",
    "Materials":               "resources",     # could be industrial depending on sub-sector
    "Energy":                  "resources",
    "Consumer Discretionary":  "consumer",
    "Consumer Staples":        "consumer",
    "Utilities":               "utilities",
    "Real Estate":             "utilities",     # REIT — similar high-capex stable model
}

# Industry-level overrides that are more specific than sector
INDUSTRY_OVERRIDES: dict[str, str] = {
    # Healthcare sub-sectors
    "Medical Devices":          "industrial",
    "Medical Instruments":      "industrial",
    "Biotechnology":            "asset_light",
    "Drug Manufacturers":       "asset_light",
    "Pharmaceuticals":          "asset_light",
    # Materials sub-sectors
    "Specialty Chemicals":      "industrial",
    "Building Materials":       "industrial",
    "Steel":                    "resources",
    "Aluminum":                 "resources",
    "Gold":                     "resources",
    "Silver":                   "resources",
    "Copper":                   "resources",
    "Coal":                     "resources",
    "Oil & Gas E&P":            "resources",
    "Oil & Gas Integrated":     "resources",
    "Oil & Gas Refining":       "resources",
    # Industrials sub-sectors that lean consumer
    "Airlines":                 "consumer",
    "Restaurants":              "consumer",
    "Hotels & Motels":          "consumer",
    # Telecoms
    "Telecom Services":         "utilities",
    "Telecommunications":       "utilities",
}

# Scoring thresholds
_HIGH_CONFIDENCE_LEAD  = 4
_HIGH_CONFIDENCE_TOTAL = 6
_MEDIUM_CONFIDENCE_LEAD = 2


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_classifier(normalised_output: dict) -> dict:
    """
    Classify the company into a modelling bucket.
    Returns a structured dict with classification, confidence, rationale, and signal log.
    """
    ticker       = normalised_output.get("ticker", "")
    company_name = normalised_output.get("company_name", "")
    stats        = normalised_output.get("stats", {})
    years_data   = normalised_output.get("canonical_by_year", {})
    norm         = normalised_output.get("normalised", {})
    base_year    = normalised_output.get("base_year")
    quality_score = normalised_output.get("quality_score", 0)

    years = sorted(years_data.keys(), reverse=True)

    blockers:   list[str]  = []
    warnings:   list[str]  = []
    conflicts:  list[str]  = []
    signal_log: list[dict] = []

    sector   = stats.get("sector", "") or ""
    industry = stats.get("industry", "") or ""

    # -------------------------------------------------------------------------
    # Hard exit: unsupported sector
    # -------------------------------------------------------------------------
    for unsupported_key, reason in UNSUPPORTED.items():
        if unsupported_key.lower() in sector.lower():
            blockers.append(f"BLOCKER: Sector '{sector}' is not supported. {reason}")
            return _build_result(
                status="fail", classification="unsupported",
                label="Unsupported sector", template=None,
                confidence="n/a", rationale=reason,
                signal_log=[], scores={}, conflicts=[], blockers=blockers,
                warnings=warnings, ticker=ticker, company_name=company_name,
                stats=stats, years_data=years_data, norm=norm,
                base_year=base_year, quality_score=quality_score,
            )

    # -------------------------------------------------------------------------
    # Extract financial profile inputs
    # -------------------------------------------------------------------------
    capex_pct   = norm.get("capex_pct_revenue")   # absolute (positive decimal)
    ebit_margin = norm.get("ebit_margin")
    nwc_pct     = norm.get("nwc_pct_revenue")

    # Gross margin — compute from canonical history if available
    gross_margin = _calc_gross_margin(years_data, years)

    # Inventory presence — does any year have non-zero inventory?
    has_inventory = any(
        years_data[y].get("inventory") not in (None, 0)
        for y in years
    )

    # Deferred revenue — proxy for subscription/SaaS-like model
    has_deferred_revenue = any(
        years_data[y].get("deferred_revenue") not in (None, 0)
        for y in years
    )

    # Revenue volatility — high volatility signals commodity/cyclical exposure
    rev_volatility = _calc_revenue_volatility(years_data, years)

    # Leverage: debt / revenue as a rough proxy
    leverage = _calc_leverage(years_data, years, norm)

    # -------------------------------------------------------------------------
    # Score each class
    # -------------------------------------------------------------------------
    scores: dict[str, int] = {k: 0 for k in BUCKETS}

    def _signal(description: str, votes: dict[str, int], basis: str):
        """Record a signal and add its votes to the scores."""
        for cls, pts in votes.items():
            if cls in scores:
                scores[cls] += pts
        signal_log.append({
            "signal":      description,
            "votes":       votes,
            "basis":       basis,
        })

    # --- Sector hint (3 pts — strong prior but overridable) ---
    sector_hint = INDUSTRY_OVERRIDES.get(industry) or SECTOR_HINTS.get(sector)
    if sector_hint and sector_hint in scores:
        _signal(
            f"Sector/industry hint → {sector_hint}  (sector='{sector}', industry='{industry}')",
            {sector_hint: 3},
            basis="sector_label",
        )
    else:
        warnings.append(
            f"WARNING: Sector '{sector}' / industry '{industry}' not in hint table — "
            "classification relying entirely on financial profile"
        )

    # --- Gross margin signals ---
    if gross_margin is not None:
        if gross_margin > 0.60:
            _signal(f"Gross margin {gross_margin*100:.1f}% > 60% → asset-light signal",
                    {"asset_light": 3}, basis="gross_margin")
        elif gross_margin > 0.45:
            _signal(f"Gross margin {gross_margin*100:.1f}% 45–60% → moderate asset-light signal",
                    {"asset_light": 1, "consumer": 1}, basis="gross_margin")
        elif gross_margin > 0.25:
            _signal(f"Gross margin {gross_margin*100:.1f}% 25–45% → industrial/consumer range",
                    {"industrial": 1, "consumer": 2}, basis="gross_margin")
        else:
            _signal(f"Gross margin {gross_margin*100:.1f}% < 25% → low-margin signal",
                    {"resources": 2, "industrial": 1}, basis="gross_margin")

    # --- Capex intensity signals ---
    if capex_pct is not None:
        if capex_pct < 0.03:
            _signal(f"Capex {capex_pct*100:.1f}% of revenue < 3% → asset-light signal",
                    {"asset_light": 3}, basis="capex_intensity")
        elif capex_pct < 0.07:
            _signal(f"Capex {capex_pct*100:.1f}% of revenue 3–7% → light-moderate capex",
                    {"asset_light": 1, "consumer": 1}, basis="capex_intensity")
        elif capex_pct < 0.12:
            _signal(f"Capex {capex_pct*100:.1f}% of revenue 7–12% → industrial range",
                    {"industrial": 2, "consumer": 1}, basis="capex_intensity")
        elif capex_pct < 0.20:
            _signal(f"Capex {capex_pct*100:.1f}% of revenue 12–20% → heavy industrial / utilities",
                    {"industrial": 2, "utilities": 2, "resources": 1}, basis="capex_intensity")
        else:
            _signal(f"Capex {capex_pct*100:.1f}% of revenue > 20% → utilities / resources signal",
                    {"utilities": 3, "resources": 3}, basis="capex_intensity")

    # --- Inventory signals ---
    if has_inventory:
        _signal("Inventory present → manufacturing / retail signal",
                {"industrial": 2, "consumer": 2}, basis="inventory_present")
    else:
        _signal("No inventory → asset-light or services signal",
                {"asset_light": 2}, basis="inventory_absent")

    # --- Deferred revenue ---
    if has_deferred_revenue:
        _signal("Deferred revenue present → subscription / SaaS-like signal",
                {"asset_light": 2}, basis="deferred_revenue")

    # --- Revenue volatility ---
    if rev_volatility is not None:
        if rev_volatility > 0.20:
            _signal(f"Revenue volatility {rev_volatility*100:.1f}% (high) → cyclical / commodity signal",
                    {"resources": 2, "industrial": 1}, basis="revenue_volatility")
        elif rev_volatility < 0.05:
            _signal(f"Revenue volatility {rev_volatility*100:.1f}% (low) → stable / regulated signal",
                    {"utilities": 2, "asset_light": 1}, basis="revenue_volatility")

    # --- EBIT margin ---
    if ebit_margin is not None:
        if ebit_margin > 0.20:
            _signal(f"EBIT margin {ebit_margin*100:.1f}% > 20% → premium margin signal",
                    {"asset_light": 2}, basis="ebit_margin")
        elif ebit_margin < 0.05:
            _signal(f"EBIT margin {ebit_margin*100:.1f}% < 5% → thin margin signal",
                    {"consumer": 1, "resources": 1, "industrial": 1}, basis="ebit_margin")

    # --- Leverage ---
    if leverage is not None and leverage > 0.50:
        _signal(f"Leverage (debt/revenue) {leverage:.2f} > 0.5x → capital-intensive signal",
                {"utilities": 1, "industrial": 1}, basis="leverage")

    # -------------------------------------------------------------------------
    # Determine winner
    # -------------------------------------------------------------------------
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_class, top_score   = sorted_scores[0]
    runner_up, runner_score = sorted_scores[1] if len(sorted_scores) > 1 else ("none", 0)
    lead = top_score - runner_score

    # Check for conflict between sector hint and financial winner
    if sector_hint and sector_hint != top_class and lead < _HIGH_CONFIDENCE_LEAD:
        conflicts.append(
            f"Sector hint suggests '{sector_hint}' but financial profile scores "
            f"highest for '{top_class}' (lead: {lead}pt) — treating as hybrid or "
            f"'{top_class}' with caution"
        )

    # Assign hybrid if no clear winner
    if lead < _MEDIUM_CONFIDENCE_LEAD:
        classification = "hybrid"
        confidence     = "low"
    else:
        classification = top_class
        if lead >= _HIGH_CONFIDENCE_LEAD and top_score >= _HIGH_CONFIDENCE_TOTAL:
            confidence = "high"
        elif lead >= _MEDIUM_CONFIDENCE_LEAD:
            confidence = "medium"
        else:
            confidence = "low"

    if conflicts:
        warnings.extend(conflicts)
        if confidence == "high":
            confidence = "medium"

    if confidence == "low":
        warnings.append(
            f"WARNING: Classification confidence is low — "
            f"'{classification}' template selected but analyst should review. "
            "Downstream assumptions will carry a caution flag."
        )

    bucket    = BUCKETS[classification]
    rationale = _build_rationale(
        classification, bucket, sector, industry,
        gross_margin, capex_pct, has_inventory,
        has_deferred_revenue, ebit_margin, confidence, conflicts,
    )

    status = "pass" if not blockers and confidence != "low" else "pass_with_caution"
    if blockers:
        status = "fail"

    return _build_result(
        status=status,
        classification=classification,
        label=bucket["label"],
        template=bucket["template"],
        confidence=confidence,
        rationale=rationale,
        signal_log=signal_log,
        scores=scores,
        conflicts=conflicts,
        blockers=blockers,
        warnings=warnings,
        ticker=ticker,
        company_name=company_name,
        stats=stats,
        years_data=years_data,
        norm=norm,
        base_year=base_year,
        quality_score=quality_score,
    )


# ---------------------------------------------------------------------------
# Financial profile helpers
# ---------------------------------------------------------------------------

def _calc_gross_margin(years_data: dict, years: list) -> float | None:
    """Median gross margin across years where both gross_profit and revenue are available."""
    margins = []
    for y in years:
        gp  = years_data[y].get("gross_profit")
        rev = years_data[y].get("revenue")
        if gp is not None and rev and rev > 0:
            margins.append(gp / rev)
    return statistics.median(margins) if margins else None


def _calc_revenue_volatility(years_data: dict, years: list) -> float | None:
    """
    Coefficient of variation of annual revenue (std / mean).
    Requires at least 3 years.
    """
    rev_vals = [
        years_data[y]["revenue"]
        for y in years
        if years_data[y].get("revenue") not in (None, 0)
    ]
    if len(rev_vals) < 3:
        return None
    mean = statistics.mean(rev_vals)
    if mean == 0:
        return None
    return statistics.stdev(rev_vals) / abs(mean)


def _calc_leverage(years_data: dict, years: list, norm: dict) -> float | None:
    """Debt / revenue using most recent year with both values."""
    revenue = norm.get("revenue")
    for y in years:
        debt = years_data[y].get("debt")
        if debt is not None and revenue and revenue > 0:
            return debt / revenue
    return None


# ---------------------------------------------------------------------------
# Rationale builder
# ---------------------------------------------------------------------------

def _build_rationale(classification, bucket, sector, industry,
                     gross_margin, capex_pct, has_inventory,
                     has_deferred_revenue, ebit_margin, confidence, conflicts) -> str:
    parts = [f"{bucket['label']}."]

    if sector:
        parts.append(f"Sector: {sector}" + (f" / {industry}" if industry else "") + ".")

    profile = []
    if gross_margin is not None:
        profile.append(f"gross margin {gross_margin*100:.1f}%")
    if capex_pct is not None:
        profile.append(f"capex {capex_pct*100:.1f}% of revenue")
    if has_inventory:
        profile.append("inventory present")
    else:
        profile.append("no inventory")
    if has_deferred_revenue:
        profile.append("deferred revenue present")
    if ebit_margin is not None:
        profile.append(f"normalised EBIT margin {ebit_margin*100:.1f}%")
    if profile:
        parts.append("Financial profile: " + ", ".join(profile) + ".")

    parts.append(f"Classification confidence: {confidence.upper()}.")

    if conflicts:
        parts.append("Note: " + " ".join(conflicts))

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Result builder
# ---------------------------------------------------------------------------

def _build_result(status, classification, label, template, confidence, rationale,
                  signal_log, scores, conflicts, blockers, warnings,
                  ticker, company_name, stats, years_data, norm,
                  base_year, quality_score) -> dict:
    return {
        "status":          status,
        "classification":  classification,
        "label":           label,
        "template":        template,
        "confidence":      confidence,
        "rationale":       rationale,
        "signal_log":      signal_log,
        "scores":          scores,
        "conflicts":       conflicts,
        "blockers":        blockers,
        "warnings":        warnings,
        "ticker":          ticker,
        "company_name":    company_name,
        "stats":           stats,
        "canonical_by_year": years_data,
        "normalised":      norm,
        "base_year":       base_year,
        "quality_score":   quality_score,
        "metadata": {
            "classified_at": datetime.utcnow().isoformat() + "Z",
        },
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_classifier_report(result: dict) -> None:
    status = result["status"]
    status_label = {"pass": "PASS", "pass_with_caution": "CAUTION", "fail": "FAIL"}

    print(f"\n{'='*60}")
    print(f"  Classifier — {result['company_name']} ({result['ticker']})")
    print(f"  Status         : [{status_label.get(status, status)}]")
    print(f"  Classification : {result['label']}")
    print(f"  Template       : {result['template']}")
    print(f"  Confidence     : {result['confidence'].upper()}")
    print(f"{'='*60}")

    print(f"\n  Rationale:\n    {result['rationale']}")

    if result["blockers"]:
        print("\n  Blockers:")
        for b in result["blockers"]:
            print(f"    {b}")

    if result["warnings"]:
        print("\n  Warnings:")
        for w in result["warnings"]:
            print(f"    {w}")

    print(f"\n  Scores:")
    sorted_scores = sorted(result["scores"].items(), key=lambda x: x[1], reverse=True)
    for cls, pts in sorted_scores:
        bar = "█" * pts
        print(f"    {cls:<18} {pts:>3}pt  {bar}")

    print(f"\n  Signals ({len(result['signal_log'])}):")
    for s in result["signal_log"]:
        vote_str = ", ".join(f"{k}+{v}" for k, v in s["votes"].items())
        print(f"    [{s['basis']}] {s['signal']}")
        print(f"      → votes: {vote_str}")

    print(f"\n  Classified at : {result['metadata']['classified_at']}")
    print(f"{'='*60}\n")
