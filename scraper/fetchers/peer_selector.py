"""
Peer Selection Engine — Weighted Scoring Model

Selects the 5 most economically comparable public peers using a five-criterion
weighted scoring model rather than a static lookup table.

Scoring weights
───────────────
  Industry / business model match  40%
  Size match                       25%
  Financial profile match          20%
  Geography match                  10%
  Data quality / completeness       5%

Candidate sourcing
──────────────────
  1. FMP /stock_peers (if FMP_API_KEY set)  → up to 10 tickers
  2. Curated _CANDIDATE_POOL by sector / industry → up to 15 tickers
  Merged, de-duplicated, capped at _MAX_CANDIDATES = 20.

Public entry point
──────────────────
  select_peers(ticker, sector, industry, target, notes) → dict
  See docstring on that function for the full output schema.
"""

from __future__ import annotations

import re
import time
from statistics import median as _stat_median
from typing import Any

import yfinance as yf

try:
    from scraper.fetchers.fmp import fetch_fmp_peers, is_available as fmp_available
except ImportError:
    def fmp_available() -> bool: return False
    def fetch_fmp_peers(t: str) -> dict: return {"available": False, "reason": "FMP not imported", "peers": []}


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

_MAX_CANDIDATES = 20
_TOP_N_SELECTED = 5
_TOP_N_REJECTED = 5
_YAHOO_SLEEP    = 0.3

_W_INDUSTRY  = 0.40
_W_SIZE      = 0.25
_W_FINANCIAL = 0.20
_W_GEO       = 0.10
_W_DATA_QUAL = 0.05

_MATURE_TIGHTEN = 0.60   # multiply all financial tolerances by this for stable sectors

_TOLERANCES = {          # default abs tolerances (decimal, not %)
    "ebit_margin":    0.08,
    "ebitda_margin":  0.10,
    "revenue_growth": 0.10,
    "gross_margin":   0.10,
}
_FIN_WEIGHTS = {         # within-financial sub-weights (sum to 1)
    "ebit_margin":    0.35,
    "ebitda_margin":  0.25,
    "revenue_growth": 0.25,
    "gross_margin":   0.15,
}

_KEY_FIELDS = ["revenue", "ebitda", "ebit", "market_cap", "ev"]


# ─────────────────────────────────────────────────────────────────────────────
# Candidate pool  (12–15 per sector / industry bucket)
# ─────────────────────────────────────────────────────────────────────────────

_CANDIDATE_POOL: dict[str, dict[str, list[str]]] = {
    "industrials": {
        "aerospace & defense": ["BA", "LMT", "RTX", "NOC", "HII", "GD", "L3H.TO", "LDOS", "TDG", "HEI", "KTOS", "AXON"],
        "aerospace":           ["BA", "LMT", "RTX", "AIR.PA", "SAF.PA", "TDG", "HEI", "NOC", "GD", "SAFGY"],
        "defense":             ["LMT", "RTX", "NOC", "HII", "GD", "LDOS", "KTOS", "AXON", "BWXT", "CACI"],
        "machinery":           ["HON", "EMR", "ROK", "ITW", "DOV", "PH", "FTV", "AME", "IR", "GWW", "GNRC", "FAST", "RRX"],
        "conglomerat":         ["HON", "MMM", "GE", "ITW", "DHR", "ABB", "SIEGY", "EMR", "DOV", "PH"],
        "_sector":             ["HON", "GE", "CAT", "MMM", "RTX", "BA", "EMR", "DE", "FDX", "UPS", "ITW", "PH", "LMT"],
    },
    "consumer defensive": {
        "household":           ["PG", "CL", "KMB", "CHD", "HENKY", "RBGLY", "BDRFY", "CLX", "COTY", "EDGRKF"],
        "personal care":       ["PG", "CL", "KMB", "CHD", "HENKY", "RBGLY", "BDRFY", "ELF", "COTY", "ULTA"],
        "packaged":            ["NSRGY", "MDLZ", "GIS", "KHC", "MKC", "CPB", "SJM", "CAG", "HRL", "POST", "K", "LANC", "INGR"],
        "food":                ["NSRGY", "MDLZ", "GIS", "KHC", "MKC", "CPB", "SJM", "CAG", "HRL", "POST", "K", "INGR"],
        "beverages":           ["KO", "PEP", "DEO", "STZ", "BUD", "HEINY", "TAP", "MNST", "SAM", "CELH", "FIZZ"],
        "tobacco":             ["BTI", "PM", "MO", "IMBBY", "SWMAY", "JAPAY"],
        "_sector":             ["PG", "NSRGY", "KO", "PEP", "UL", "CL", "KMB", "MDLZ", "GIS", "PM", "BTI", "DEO", "HENKY"],
    },
    "consumer staples": {
        "household":           ["PG", "CL", "KMB", "CHD", "HENKY", "RBGLY", "BDRFY", "CLX", "UL", "COTY"],
        "food":                ["NSRGY", "MDLZ", "GIS", "KHC", "MKC", "CPB", "SJM", "CAG", "HRL", "UL", "K"],
        "beverages":           ["KO", "PEP", "DEO", "STZ", "BUD", "HEINY", "TAP", "MNST", "SAM"],
        "tobacco":             ["BTI", "PM", "MO", "IMBBY", "SWMAY"],
        "_sector":             ["PG", "NSRGY", "KO", "PEP", "UL", "CL", "KMB", "MDLZ", "GIS", "PM", "BTI", "DEO"],
    },
    "consumer discretionary": {
        "auto":                ["TSLA", "GM", "F", "TM", "HMC", "STLA", "VWAGY", "BMWYY", "MBGYY", "RACE", "NIO", "RIVN"],
        "retail":              ["AMZN", "WMT", "TGT", "COST", "DG", "DLTR", "KR", "ROST", "TJX", "BBY", "JWN", "M"],
        "apparel":             ["NKE", "ADDYY", "VFC", "PVH", "RL", "CPRI", "TPR", "GIL", "HBI", "SKX", "ONON"],
        "restaurant":          ["MCD", "SBUX", "YUM", "QSR", "DPZ", "WEN", "CMG", "TXRH", "EAT", "JACK", "SHAK"],
        "travel":              ["BKNG", "EXPE", "ABNB", "HLT", "MAR", "IHG", "CCL", "RCL", "NCLH"],
        "_sector":             ["AMZN", "HD", "NKE", "MCD", "SBUX", "TSLA", "BKNG", "TJX", "ROST", "CMG", "LOW"],
    },
    "technology": {
        "software":            ["MSFT", "ORCL", "SAP", "ADBE", "CRM", "NOW", "WDAY", "INTU", "ANSS", "PTC", "CDNS", "SNPS"],
        "semiconductor":       ["NVDA", "AMD", "INTC", "TSM", "AVGO", "QCOM", "MRVL", "TXN", "MU", "LRCX", "KLAC", "AMAT"],
        "hardware":            ["AAPL", "HPQ", "DELL", "STX", "WDC", "NTAP", "LOGI", "ZBRA", "NCR", "PSTG"],
        "it services":         ["INFY", "WIT", "ACN", "IBM", "CTSH", "EPAM", "CDW", "GLOB", "DXC"],
        "internet":            ["GOOGL", "META", "AMZN", "SNAP", "PINS", "ETSY", "EBAY", "MELI"],
        "_sector":             ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AVGO", "ORCL", "ADBE", "QCOM", "TXN", "ACN"],
    },
    "communication services": {
        "media":               ["DIS", "NFLX", "PARA", "WBD", "FOX", "CMCSA", "AMCX", "SIRI"],
        "telecom":             ["T", "VZ", "TMUS", "VOD", "ORAN", "DTEGY", "LUMN", "LBTYC", "TDS"],
        "internet":            ["GOOGL", "META", "SNAP", "PINS", "MTCH", "IAC", "ZG"],
        "_sector":             ["GOOGL", "META", "DIS", "NFLX", "T", "VZ", "TMUS", "CMCSA", "CHTR", "WBD", "PARA"],
    },
    "healthcare": {
        "pharma":              ["JNJ", "PFE", "MRK", "AZN", "RHHBY", "NVS", "GSK", "SNY", "LLY", "ABBV", "BMY", "AMGN"],
        "pharmaceutical":      ["JNJ", "PFE", "MRK", "AZN", "RHHBY", "NVS", "GSK", "SNY", "LLY", "ABBV", "BMY"],
        "biotech":             ["AMGN", "GILD", "BIIB", "REGN", "VRTX", "BMY", "MRNA", "BNTX", "ALNY", "SRPT"],
        "biotechnology":       ["AMGN", "GILD", "BIIB", "REGN", "VRTX", "MRNA", "BNTX", "ALNY", "IONS"],
        "medical device":      ["MDT", "ABT", "SYK", "EW", "BSX", "ZBH", "BDX", "ISRG", "DXCM", "PODD", "RMD"],
        "medical devices":     ["MDT", "ABT", "SYK", "EW", "BSX", "ZBH", "BDX", "ISRG", "DXCM", "PODD"],
        "services":            ["UNH", "CVS", "CI", "HUM", "ELV", "MCK", "CNC", "MOH", "DGX", "LH"],
        "_sector":             ["JNJ", "UNH", "PFE", "MRK", "LLY", "ABBV", "ABT", "TMO", "MDT", "AMGN", "DHR"],
    },
    "financials": {
        "bank":                ["JPM", "BAC", "WFC", "C", "HSBC", "BCS", "DB", "SAN", "GS", "MS", "USB", "TFC", "PNC"],
        "insurance":           ["BRK-B", "AIG", "ALL", "MET", "PRU", "TRV", "CB", "HIG", "AJG", "AON", "MMC", "WTW"],
        "asset management":    ["BLK", "SCHW", "MS", "GS", "BEN", "AMG", "IVZ", "LAZ", "EVR", "BX"],
        "capital markets":     ["GS", "MS", "BX", "APO", "KKR", "CG", "ARES", "BAM"],
        "_sector":             ["JPM", "BAC", "BRK-B", "GS", "MS", "BLK", "WFC", "C", "SCHW", "CB", "AIG", "PRU"],
    },
    "energy": {
        "oil & gas":           ["XOM", "CVX", "SHEL", "BP", "TTE", "COP", "EOG", "OXY", "MPC", "VLO", "PSX", "DVN"],
        "oil":                 ["XOM", "CVX", "SHEL", "BP", "TTE", "COP", "EOG", "OXY", "DVN", "PXD"],
        "services":            ["SLB", "HAL", "BKR", "NOV", "CHX", "PTEN", "RES", "FTI"],
        "renewable":           ["NEE", "BEP", "ENPH", "FSLR", "RUN", "SEDG", "CWEN", "AES"],
        "_sector":             ["XOM", "CVX", "SHEL", "BP", "TTE", "COP", "SLB", "EOG", "OXY", "MPC", "HAL"],
    },
    "basic materials": {
        "mining":              ["BHP", "RIO", "GLCNF", "FCX", "NEM", "GOLD", "VALE", "TECK", "WPM", "AEM"],
        "chemicals":           ["LIN", "APD", "SHW", "DD", "EMN", "LYB", "DOW", "PPG", "ECL", "RPM", "FMC"],
        "steel":               ["NUE", "STLD", "CLF", "X", "TX", "NML", "CMC", "MT"],
        "_sector":             ["BHP", "RIO", "LIN", "APD", "SHW", "DD", "FCX", "NEM", "DOW", "NUE", "ECL"],
    },
    "real estate": {
        "_sector":             ["AMT", "PLD", "EQIX", "SPG", "O", "DLR", "PSA", "AVB", "EQR", "VTR", "ARE", "WY", "CBRE"],
    },
    "utilities": {
        "_sector":             ["NEE", "DUK", "SO", "D", "EXC", "AEP", "XEL", "PCG", "SRE", "WEC", "ES", "ETR", "CMS"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Sector / geography reference data
# ─────────────────────────────────────────────────────────────────────────────

_SECTOR_ALIASES: dict[str, str] = {
    "consumer defensive":  "consumer staples",
    "consumer cyclical":   "consumer discretionary",
}

_ADJACENT_SECTORS: dict[str, set[str]] = {
    "industrials":              {"basic materials", "technology", "energy"},
    "consumer discretionary":   {"consumer defensive", "consumer staples", "communication services"},
    "consumer defensive":       {"consumer discretionary", "consumer staples", "healthcare"},
    "consumer staples":         {"consumer discretionary", "consumer defensive", "healthcare"},
    "technology":               {"communication services", "industrials", "healthcare"},
    "communication services":   {"technology", "consumer discretionary"},
    "healthcare":               {"consumer defensive", "consumer staples", "technology"},
    "energy":                   {"basic materials", "industrials", "utilities"},
    "basic materials":          {"energy", "industrials"},
    "financials":               {"real estate"},
    "real estate":              {"financials", "utilities"},
    "utilities":                {"energy", "real estate"},
}

# country → (region, is_developed)
_COUNTRY_REGIONS: dict[str, tuple[str, bool]] = {
    "United States":    ("North America",  True),
    "Canada":           ("North America",  True),
    "Mexico":           ("Latin America",  False),
    "United Kingdom":   ("Europe",         True),
    "Germany":          ("Europe",         True),
    "France":           ("Europe",         True),
    "Netherlands":      ("Europe",         True),
    "Switzerland":      ("Europe",         True),
    "Sweden":           ("Europe",         True),
    "Denmark":          ("Europe",         True),
    "Belgium":          ("Europe",         True),
    "Austria":          ("Europe",         True),
    "Spain":            ("Europe",         True),
    "Italy":            ("Europe",         True),
    "Norway":           ("Europe",         True),
    "Finland":          ("Europe",         True),
    "Ireland":          ("Europe",         True),
    "Luxembourg":       ("Europe",         True),
    "Japan":            ("Asia Pacific",   True),
    "Australia":        ("Asia Pacific",   True),
    "Singapore":        ("Asia Pacific",   True),
    "Hong Kong":        ("Asia Pacific",   True),
    "New Zealand":      ("Asia Pacific",   True),
    "South Korea":      ("Asia Pacific",   True),
    "China":            ("Asia Pacific",   False),
    "India":            ("Asia Pacific",   False),
    "Taiwan":           ("Asia Pacific",   False),
    "Indonesia":        ("Asia Pacific",   False),
    "Thailand":         ("Asia Pacific",   False),
    "Malaysia":         ("Asia Pacific",   False),
    "Brazil":           ("Latin America",  False),
    "Argentina":        ("Latin America",  False),
    "Chile":            ("Latin America",  False),
    "Colombia":         ("Latin America",  False),
    "Israel":           ("Middle East",    True),
    "Saudi Arabia":     ("Middle East",    False),
    "UAE":              ("Middle East",    False),
    "South Africa":     ("Africa",         False),
}

# Industry keyword clusters for similarity matching
_INDUSTRY_CLUSTERS: dict[str, frozenset[str]] = {
    "household_personal":  frozenset({"household", "personal", "care", "hygiene", "cleaning", "hpc", "consumer products"}),
    "packaged_food":       frozenset({"packaged", "food", "foods", "snack", "processed", "grocery", "confection"}),
    "beverages":           frozenset({"beverage", "beverages", "drink", "alcoholic", "spirits", "brewery", "wine", "beer", "soft drink"}),
    "tobacco":             frozenset({"tobacco", "cigarette", "nicotine"}),
    "pharma":              frozenset({"pharmaceutical", "pharma", "drug", "medicine", "specialty pharmaceutical"}),
    "biotech":             frozenset({"biotech", "biotechnology", "biological", "genomic", "therapeutic"}),
    "medical_devices":     frozenset({"medical", "device", "devices", "diagnostic", "surgical", "implant", "equipment"}),
    "healthcare_services": frozenset({"health", "hospital", "managed care", "insurance", "services", "clinic"}),
    "semiconductor":       frozenset({"semiconductor", "chip", "microchip", "integrated circuit", "wafer"}),
    "software":            frozenset({"software", "saas", "cloud", "application", "enterprise software"}),
    "it_services":         frozenset({"it services", "consulting", "outsourcing", "information technology services"}),
    "internet":            frozenset({"internet", "online", "digital", "ecommerce", "social media", "search"}),
    "hardware":            frozenset({"hardware", "computer", "storage", "server", "peripheral", "networking equipment"}),
    "aerospace_defense":   frozenset({"aerospace", "defense", "defence", "military", "aviation", "spacecraft"}),
    "machinery":           frozenset({"machinery", "machine", "equipment", "industrial", "manufacturing", "automation"}),
    "oil_gas":             frozenset({"oil", "gas", "petroleum", "refining", "exploration", "production", "pipeline"}),
    "mining":              frozenset({"mining", "mineral", "metal", "gold", "copper", "silver", "iron", "coal"}),
    "chemicals":           frozenset({"chemical", "chemicals", "specialty chemical", "polymer", "coating", "paint"}),
    "banking":             frozenset({"bank", "banking", "financial", "lending", "mortgage", "credit"}),
    "insurance":           frozenset({"insurance", "reinsurance", "underwriting", "property casualty", "life insurance"}),
    "media":               frozenset({"media", "entertainment", "content", "streaming", "broadcast", "film", "television"}),
    "telecom":             frozenset({"telecom", "telecommunication", "wireless", "mobile", "broadband"}),
    "retail":              frozenset({"retail", "store", "department", "specialty retail", "discount", "supermarket"}),
    "auto":                frozenset({"auto", "automotive", "automobile", "vehicle", "car", "electric vehicle"}),
    "apparel":             frozenset({"apparel", "clothing", "fashion", "footwear", "luxury", "accessories"}),
}

_MATURE_SECTORS = {"consumer defensive", "consumer staples", "utilities"}


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def select_peers(
    ticker:   str,
    sector:   str | None,
    industry: str | None,
    target:   dict,
    notes:    list[str],
) -> dict:
    """
    Score a candidate pool and return the top 5 peers.

    Parameters
    ----------
    ticker   : target company ticker (excluded from candidate list)
    sector   : Yahoo sector string for the target
    industry : Yahoo industry string for the target
    target   : dict with keys — revenue, market_cap, ev, ebit_margin,
               ebitda_margin, gross_margin, revenue_growth, country,
               sector, industry, currency.  None values are handled.
    notes    : mutable list; diagnostic messages appended in place

    Returns
    -------
    {
      "selected": [
          { ticker, name, currency, ev, revenue, ebitda, ebit, net_income,
            ev_revenue, ev_ebitda, ev_ebit, pe, price, market_cap, dcf_vps,
            ebit_margin, ebitda_margin, gross_margin, revenue_growth,
            country, peer_sector, peer_industry,
            score, score_breakdown, match_notes, rejection_reason },
          ...  # up to 5
      ],
      "rejected": [
          { ticker, name, score, rejection_reason },
          ...  # up to 5 best-scored rejects + pre-score exclusions
      ],
      "method":  "fmp+pool" | "pool_only" | "fmp_only" | "fallback",
      "notes":   list[str],
    }
    """
    candidates, method = _build_candidate_pool(ticker, sector, industry, notes)

    if not candidates:
        notes.append("No candidate pool could be built for peer selection.")
        return {"selected": [], "rejected": [], "method": "fallback", "notes": notes}

    is_mature = _is_mature_sector(sector, industry)
    if is_mature:
        notes.append("Mature/stable sector detected — financial profile tolerances tightened by 40%.")

    scored:   list[dict] = []
    excluded: list[dict] = []

    for cand_ticker in candidates:
        if cand_ticker.upper() == ticker.upper():
            continue

        data = _fetch_candidate_data(cand_ticker)
        if data is None:
            excluded.append({"ticker": cand_ticker, "name": cand_ticker,
                             "score": None, "rejection_reason": "Could not fetch data from Yahoo"})
            time.sleep(_YAHOO_SLEEP)
            continue

        exc_reason = _apply_exclusions(data)
        if exc_reason:
            excluded.append({"ticker": cand_ticker, "name": data["name"],
                             "score": None, "rejection_reason": exc_reason})
            time.sleep(_YAHOO_SLEEP)
            continue

        ind_score, ind_note = _score_industry(data, target)
        if ind_score < 20:
            excluded.append({"ticker": cand_ticker, "name": data["name"],
                             "score": ind_score,
                             "rejection_reason": f"Business model mismatch (industry score {ind_score:.0f}/100)"})
            time.sleep(_YAHOO_SLEEP)
            continue

        siz_score, siz_note = _score_size(data, target)
        fin_score, fin_note = _score_financial_profile(data, target, is_mature)
        geo_score, geo_note = _score_geography(data, target)
        dq_score,  dq_note  = _score_data_quality(data)

        composite = (
            ind_score * _W_INDUSTRY +
            siz_score * _W_SIZE +
            fin_score * _W_FINANCIAL +
            geo_score * _W_GEO +
            dq_score  * _W_DATA_QUAL
        )

        scored.append({
            **data,
            "dcf_vps":    None,
            "score":      round(composite, 1),
            "score_breakdown": {
                "industry":     round(ind_score, 1),
                "size":         round(siz_score, 1),
                "financial":    round(fin_score, 1),
                "geography":    round(geo_score, 1),
                "data_quality": round(dq_score,  1),
            },
            "match_notes":     [n for n in [ind_note, siz_note, fin_note, geo_note, dq_note] if n],
            "rejection_reason": None,
        })
        time.sleep(_YAHOO_SLEEP)

    # Sort by composite score, tiebreak by data quality
    scored.sort(key=lambda x: (x["score"], x["score_breakdown"]["data_quality"]), reverse=True)

    selected = scored[:_TOP_N_SELECTED]
    rejects_scored = [
        {"ticker": r["ticker"], "name": r["name"],
         "score": r["score"],
         "rejection_reason": f"Scored {r['score']:.0f}/100 — ranked outside top {_TOP_N_SELECTED}"}
        for r in scored[_TOP_N_SELECTED:_TOP_N_SELECTED + _TOP_N_REJECTED]
    ]

    all_rejected = (rejects_scored + excluded)[:_TOP_N_REJECTED + 5]

    if selected:
        notes.append(
            f"Scored {len(scored)} candidates; selected top {len(selected)} "
            f"(scores: {selected[0]['score']:.0f}–{selected[-1]['score']:.0f}/100)."
        )
    else:
        notes.append("Scoring returned no valid peers — recommend expanding candidate pool.")

    return {
        "selected": selected,
        "rejected": all_rejected,
        "method":   method,
        "notes":    notes,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Candidate pool
# ─────────────────────────────────────────────────────────────────────────────

def _build_candidate_pool(
    ticker:   str,
    sector:   str | None,
    industry: str | None,
    notes:    list[str],
) -> tuple[list[str], str]:
    """Merge FMP peers + curated pool, de-duplicate, cap at _MAX_CANDIDATES."""
    fmp_tickers: list[str] = []
    pool_tickers: list[str] = []
    parts: list[str] = []

    if fmp_available():
        result = fetch_fmp_peers(ticker)
        if result.get("available") and result.get("peers"):
            fmp_tickers = result["peers"]
            parts.append("fmp")

    pool_tickers, pool_desc = _get_candidate_pool(sector, industry)
    if pool_tickers:
        parts.append("pool")

    if not parts:
        notes.append("No peer candidates found — FMP unavailable and sector not in curated pool.")
        return [], "fallback"

    # Merge: FMP first (prioritised), then pool, deduplicated
    seen: set[str] = set()
    merged: list[str] = []
    for t in fmp_tickers + pool_tickers:
        key = t.upper()
        if key not in seen and key != ticker.upper():
            seen.add(key)
            merged.append(t)
        if len(merged) >= _MAX_CANDIDATES:
            break

    method = "+".join(parts) if parts else "fallback"
    notes.append(f"Candidate pool: {len(merged)} tickers via {method} ({pool_desc}).")
    return merged, method


def _get_candidate_pool(sector: str | None, industry: str | None) -> tuple[list[str], str]:
    """Two-level dict lookup: sector → industry (substring) → tickers."""
    if not sector:
        return [], "no sector provided"

    s = sector.lower()

    # Find sector bucket (allow aliases + substring match)
    s_norm = _SECTOR_ALIASES.get(s, s)
    bucket: dict | None = None
    matched_sector = ""
    for key in _CANDIDATE_POOL:
        k_norm = _SECTOR_ALIASES.get(key, key)
        if k_norm == s_norm or k_norm in s_norm or s_norm in k_norm:
            bucket = _CANDIDATE_POOL[key]
            matched_sector = key
            break

    if bucket is None:
        return [], f"sector '{sector}' not in pool"

    ind = (industry or "").lower()

    # Find industry key (exact then substring)
    best_tickers: list[str] | None = None
    best_key = ""
    for ind_key, tickers in bucket.items():
        if ind_key == "_sector":
            continue
        if ind_key in ind or (len(ind_key) > 3 and ind_key in ind):
            if best_tickers is None or len(ind_key) > len(best_key):
                best_tickers = tickers
                best_key = ind_key

    sector_fallback = bucket.get("_sector", [])

    if best_tickers:
        combined = list(dict.fromkeys(best_tickers + sector_fallback))[:_MAX_CANDIDATES]
        return combined, f"{matched_sector}/{best_key}"

    if sector_fallback:
        return sector_fallback[:_MAX_CANDIDATES], f"{matched_sector}/_sector"

    return [], f"no match in '{matched_sector}' bucket"


# ─────────────────────────────────────────────────────────────────────────────
# Data fetching
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_candidate_data(ticker: str) -> dict | None:
    """
    Single Yahoo Finance .info call.
    Returns a unified dict sufficient for both scoring and multiples computation.
    Returns None on any failure or if the company has no market cap (private).
    """
    try:
        info = yf.Ticker(ticker).info
    except Exception:
        return None

    if not info or info.get("quoteType") == "MUTUALFUND":
        return None

    mc  = _safe(info.get("marketCap"))
    if not mc:
        return None     # private or delisted

    ev  = _safe(info.get("enterpriseValue"))
    rev = _safe(info.get("totalRevenue"))
    ebitda = _safe(info.get("ebitda"))
    ebit   = _safe(info.get("operatingIncome") or info.get("ebit"))
    ni     = _safe(info.get("netIncomeToCommon"))
    price  = _safe(info.get("regularMarketPrice") or info.get("currentPrice"))
    ccy    = info.get("currency", "")
    name   = info.get("shortName") or info.get("longName") or ticker
    country  = info.get("country", "")
    peer_sec = info.get("sector", "")
    peer_ind = info.get("industry", "")

    # Scoring inputs
    ebit_m   = _safe(info.get("operatingMargins"))
    ebitda_m = _safe(info.get("ebitdaMargins"))
    gross_m  = _safe(info.get("grossMargins"))
    rev_g    = _safe(info.get("revenueGrowth"))
    dte      = _safe(info.get("debtToEquity"))
    bv_eq    = _safe(info.get("bookValue"))
    shares   = _safe(info.get("sharesOutstanding"))
    total_eq = _safe(info.get("totalStockholderEquity"))

    return {
        # Multiples fields (compatible with _fetch_peer_metrics return)
        "ticker":      ticker,
        "name":        name,
        "currency":    ccy,
        "ev":          ev,
        "market_cap":  mc,
        "revenue":     rev,
        "ebitda":      ebitda,
        "ebit":        ebit,
        "net_income":  ni,
        "price":       price,
        "ev_revenue":  _div(ev, rev),
        "ev_ebitda":   _div(ev, ebitda),
        "ev_ebit":     _div(ev, ebit),
        "pe":          _div(mc, ni) if mc and ni and ni > 0 else None,
        # Scoring inputs
        "ebit_margin":    ebit_m,
        "ebitda_margin":  ebitda_m,
        "gross_margin":   gross_m,
        "revenue_growth": rev_g,
        "country":        country,
        "peer_sector":    peer_sec,
        "peer_industry":  peer_ind,
        "debt_to_equity": dte,
        "total_equity":   total_eq,
        "shares_outstanding": shares,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Exclusion rules
# ─────────────────────────────────────────────────────────────────────────────

def _apply_exclusions(candidate: dict) -> str | None:
    """Return a rejection reason if the candidate should be excluded, else None."""
    # Data completeness
    present = sum(1 for f in _KEY_FIELDS if candidate.get(f) is not None)
    if present < 2:
        return f"Insufficient data ({present}/{len(_KEY_FIELDS)} key fields present)"

    # Distress check
    total_eq = candidate.get("total_equity")
    if total_eq is not None and total_eq < 0:
        return "Distressed: negative total equity"

    dte = candidate.get("debt_to_equity")
    if dte is not None and dte > 500:
        return f"Distressed: debt/equity ratio {dte:.0f}% (>{500}%)"

    # Conglomerate / diversified check
    ind_lower = (candidate.get("peer_industry") or "").lower()
    if any(k in ind_lower for k in ("conglomerate", "diversified", "multi-sector")):
        return f"Conglomerate/diversified: weak segment comparability ({candidate.get('peer_industry')})"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Scoring functions
# ─────────────────────────────────────────────────────────────────────────────

def _score_industry(candidate: dict, target: dict) -> tuple[float, str]:
    """Score industry / business model match (0–100)."""
    t_sec = _normalize_sector(target.get("sector") or target.get("peer_sector", ""))
    t_ind = (target.get("industry") or target.get("peer_industry", "")).lower()
    c_sec = _normalize_sector(candidate.get("peer_sector", ""))
    c_ind = (candidate.get("peer_industry", "")).lower()

    if not t_sec and not t_ind:
        return 50.0, "No target sector/industry — neutral score"

    # Exact industry string match
    if t_sec == c_sec and t_ind and c_ind and t_ind == c_ind:
        return 100.0, f"Exact industry match: {candidate.get('peer_industry')}"

    # Cluster-based match
    t_cluster = _assign_cluster(t_ind)
    c_cluster = _assign_cluster(c_ind)

    if t_sec == c_sec:
        if t_cluster and c_cluster and t_cluster == c_cluster:
            return 85.0, f"Same sector + industry cluster ({t_cluster.replace('_', ' ')})"
        # Partial industry keyword overlap
        t_words = _ind_words(t_ind)
        c_words = _ind_words(c_ind)
        if t_words and c_words:
            overlap = len(t_words & c_words) / max(len(t_words | c_words), 1)
            if overlap >= 0.4:
                return 75.0, f"Same sector, strong keyword overlap ({overlap:.0%})"
        return 50.0, f"Same sector ({t_sec or c_sec}), different industry focus"

    # Adjacent sector
    adj = _ADJACENT_SECTORS.get(t_sec or "", set())
    if c_sec in adj:
        note = f"Adjacent sector: {candidate.get('peer_sector')} (target is {target.get('sector')})"
        if t_cluster and c_cluster and t_cluster == c_cluster:
            return 30.0, note + " + matching activity cluster"
        return 20.0, note

    return 0.0, f"Different sector: {candidate.get('peer_sector')} vs {target.get('sector')}"


def _score_size(candidate: dict, target: dict) -> tuple[float, str]:
    """Score size match (0–100). Missing metrics excluded from weighted average."""
    metrics = [
        ("revenue",    target.get("revenue"),    candidate.get("revenue"),    0.40),
        ("market_cap", target.get("market_cap"), candidate.get("market_cap"), 0.30),
        ("ev",         target.get("ev"),         candidate.get("ev"),         0.30),
    ]

    total_w = 0.0
    total_s = 0.0
    notes_parts: list[str] = []

    for label, t_val, c_val, w in metrics:
        if not t_val or not c_val or t_val == 0:
            continue
        ratio = c_val / t_val
        inv   = max(ratio, 1.0 / ratio)   # always >= 1
        if inv <= 2.0:
            s = 100.0
        elif inv <= 3.0:
            s = 60.0
        elif inv <= 5.0:
            s = 20.0
        else:
            s = 0.0
        total_s += s * w
        total_w += w
        if s < 60:
            notes_parts.append(f"{label} ratio {ratio:.1f}x")

    if total_w == 0:
        return 50.0, "Size data unavailable — neutral score"

    score = total_s / total_w
    note  = (f"Size mismatch: {', '.join(notes_parts)}" if notes_parts
             else "Good size match (within 2×)")
    return score, note


def _score_financial_profile(
    candidate: dict,
    target: dict,
    is_mature: bool,
) -> tuple[float, str]:
    """Score financial profile similarity (0–100). Tighter tolerances for mature sectors."""
    mult = _MATURE_TIGHTEN if is_mature else 1.0
    tols = {k: v * mult for k, v in _TOLERANCES.items()}

    total_w = 0.0
    total_s = 0.0
    notes_parts: list[str] = []
    miss: list[str] = []

    for metric, w in _FIN_WEIGHTS.items():
        t_val = target.get(metric)
        c_val = candidate.get(metric)
        if t_val is None or c_val is None:
            miss.append(metric)
            continue
        diff = abs(t_val - c_val)
        tol  = tols[metric]
        if diff <= tol:
            s = 100.0
        elif diff <= 2 * tol:
            s = 50.0
        else:
            s = 0.0
            notes_parts.append(f"{metric.replace('_', ' ')} diff {diff:.1%}")
        total_s += s * w
        total_w += w

    if total_w == 0:
        return 50.0, "Financial profile data unavailable — neutral score"

    score = total_s / total_w
    parts = []
    if notes_parts:
        parts.append(f"Profile gaps: {', '.join(notes_parts)}")
    if miss:
        parts.append(f"Missing: {', '.join(miss)}")
    note = "; ".join(parts) if parts else "Good financial profile match"
    return score, note


def _score_geography(candidate: dict, target: dict) -> tuple[float, str]:
    """Score geography match (0–100)."""
    t_cty = (target.get("country") or "").strip()
    c_cty = (candidate.get("country") or "").strip()

    if not t_cty or not c_cty:
        return 50.0, "Country data unavailable — neutral score"

    if t_cty == c_cty:
        return 100.0, f"Same country ({c_cty})"

    t_region, t_dev = _COUNTRY_REGIONS.get(t_cty, ("Unknown", True))
    c_region, c_dev = _COUNTRY_REGIONS.get(c_cty, ("Unknown", True))

    if t_region == c_region:
        return 70.0, f"Same region ({c_region}): {c_cty} vs {t_cty}"

    if c_dev and t_dev:
        return 40.0, f"Both developed markets ({c_cty}/{c_region} vs {t_cty}/{t_region})"

    return 10.0, f"Different market type ({c_cty}/{c_region} vs {t_cty}/{t_region})"


def _score_data_quality(candidate: dict) -> tuple[float, str]:
    """Score data completeness (0–100)."""
    present = sum(1 for f in _KEY_FIELDS if candidate.get(f) is not None)
    score   = (present / len(_KEY_FIELDS)) * 100
    note    = (f"Data quality: {present}/{len(_KEY_FIELDS)} key fields present"
               if present < len(_KEY_FIELDS) else "Complete data")
    return score, note


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_sector(sector: str | None) -> str:
    if not sector:
        return ""
    s = sector.lower().strip()
    return _SECTOR_ALIASES.get(s, s)


def _assign_cluster(industry: str | None) -> str | None:
    """Return the name of the best-matching _INDUSTRY_CLUSTERS key, or None."""
    if not industry:
        return None
    words = set(re.split(r"[\s&,/()\-]+", industry.lower()))
    best, best_n = None, 0
    for cluster, kws in _INDUSTRY_CLUSTERS.items():
        n = len(words & kws)
        if n > best_n:
            best_n, best = n, cluster
    return best if best_n > 0 else None


def _ind_words(industry: str) -> set[str]:
    """Tokenise industry string into significant words (length > 3)."""
    stopwords = {"and", "the", "of", "&", "other", "general", "activities", "related"}
    return {w for w in re.split(r"[\s&,/()]+", industry.lower())
            if len(w) > 3 and w not in stopwords}


def _is_mature_sector(sector: str | None, industry: str | None) -> bool:
    """True for stable sectors that warrant tighter financial matching tolerances."""
    if not sector:
        return False
    s_norm = _normalize_sector(sector)
    if s_norm in _MATURE_SECTORS:
        return True
    # Healthcare is mature only for pharma (not biotech/devices)
    if s_norm == "healthcare":
        ind = (industry or "").lower()
        return "pharma" in ind or "pharmaceutical" in ind
    # Telecom
    ind = (industry or "").lower()
    return "telecom" in ind or "telecommunication" in ind


def _safe(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f   # NaN → None
    except (TypeError, ValueError):
        return None


def _div(num: float | None, denom: float | None) -> float | None:
    if num is None or not denom or denom == 0:
        return None
    result = num / denom
    return result if 0 < result < 500 else None
