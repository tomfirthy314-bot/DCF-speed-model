"""
Comparables fetcher

Fetches 4–5 sector peers and computes EV-based valuation multiples for each,
then packages them alongside the target company for the Comparables Excel tab.

Peer sourcing (in order of preference):
  1. FMP /stock_peers  — if FMP_API_KEY is set
  2. Sector lookup table — curated list of well-known peers by sector/industry
     (always available, no API key required)

Financial data for each peer:
  Always fetched from Yahoo Finance (yfinance .info dict).
  FMP is not used for peer financials to conserve daily request quota.

Multiples computed:
  EV/Revenue, EV/EBITDA, EV/EBIT, P/E (trailing)

Output structure (stored at data["comparables"]):
  {
    "target": {
      "ticker": "RR.L",  "name": "Rolls-Royce Holdings plc",
      "ev": ..., "revenue": ..., "ebitda": ..., "ebit": ...,
      "ev_revenue": ..., "ev_ebitda": ..., "ev_ebit": ..., "pe": ...,
      "currency": "GBP",
    },
    "peers": [ { same structure }, ... ],
    "median": { "ev_revenue": ..., "ev_ebitda": ..., "ev_ebit": ..., "pe": ... },
    "implied_from_peers": {
      "ev_revenue": ...,   # target revenue × peer median EV/Rev multiple → EV
      "ev_ebitda": ...,
      "ev_ebit":   ...,
      "vps_range": [low, mid, high],   # equity value per share from multiples
    },
    "notes": [ ... ],
  }
"""

from __future__ import annotations

import time
from statistics import median
from typing import Any

import yfinance as yf

from scraper.fetchers.fmp import fetch_fmp_peers, is_available as fmp_available
from scraper.fetchers.peer_selector import select_peers


# ─────────────────────────────────────────────────────────────────────────────
# Sector peer lookup table  (fallback when no FMP key)
# ─────────────────────────────────────────────────────────────────────────────
# Keys are (sector_lower, industry_lower) — partial match on industry.
# Each entry is a list of Yahoo Finance tickers for well-known peers.

_PEER_TABLE: dict[tuple[str, str], list[str]] = {
    # Industrials / Aerospace & Defense
    ("industrials", "aerospace & defense"):  ["BA", "LMT", "RTX", "NOC", "HII"],
    ("industrials", "aerospace"):            ["BA", "LMT", "RTX", "AIR.PA", "SAF.PA"],
    # Industrials — general
    ("industrials", "machinery"):            ["HON", "EMR", "ROK", "ITW", "DOV"],
    ("industrials", "industrial"):           ["HON", "MMM", "GE", "EMR", "ABB"],
    ("industrials", "conglomerates"):        ["HON", "MMM", "GE", "ITW", "EMR"],
    # Consumer Staples
    ("consumer staples", "beverages"):       ["KO", "PEP", "DEO", "BUD", "MNST"],
    ("consumer staples", "food"):            ["NESN.SW", "UL", "KHC", "MKC", "CPB"],
    ("consumer staples", "household"):       ["PG", "UL", "CL", "CHD", "KMB"],
    ("consumer staples", "tobacco"):         ["BTI", "MO", "PM", "IMBBY", "SWMAY"],
    ("consumer staples", ""):                ["PG", "UL", "KO", "NESN.SW", "CL"],
    # Consumer Defensive (Yahoo Finance's name for consumer staples)
    ("consumer defensive", "personal care"): ["PG", "CL", "KMB", "CHD", "HENKY"],
    ("consumer defensive", "household"):     ["PG", "CL", "KMB", "CHD", "HENKY"],
    ("consumer defensive", "packaged"):      ["NESN.SW", "KHC", "MKC", "CPB", "SJM"],
    ("consumer defensive", "food"):          ["NESN.SW", "KHC", "MKC", "CPB", "SJM"],
    ("consumer defensive", "beverages"):     ["KO", "PEP", "DEO", "BUD", "MNST"],
    ("consumer defensive", "tobacco"):       ["BTI", "MO", "PM", "IMBBY", "SWMAY"],
    ("consumer defensive", ""):              ["PG", "NESN.SW", "KO", "CL", "KMB"],
    # Consumer Discretionary
    ("consumer discretionary", "retail"):    ["AMZN", "WMT", "TGT", "COST", "M"],
    ("consumer discretionary", "auto"):      ["TSLA", "GM", "F", "STLA", "TM"],
    ("consumer discretionary", "apparel"):   ["NKE", "VFC", "PVH", "RL", "HBI"],
    ("consumer discretionary", "restaurant"):["MCD", "SBUX", "YUM", "QSR", "DPZ"],
    ("consumer discretionary", ""):         ["AMZN", "HD", "NKE", "MCD", "SBUX"],
    # Technology / Information Technology
    ("technology", "software"):              ["MSFT", "ORCL", "SAP", "ADBE", "CRM"],
    ("technology", "semiconductors"):        ["NVDA", "AMD", "INTC", "TSM", "AVGO"],
    ("technology", "hardware"):              ["AAPL", "HPQ", "DELL", "STX", "WDC"],
    ("technology", "it services"):           ["INFY", "WIT", "ACN", "IBM", "CTSH"],
    ("technology", ""):                      ["AAPL", "MSFT", "GOOGL", "META", "NVDA"],
    # Communication Services
    ("communication services", "media"):     ["DIS", "NFLX", "PARA", "FOX", "WBD"],
    ("communication services", "telecom"):   ["T", "VZ", "TMUS", "VOD", "ORAN"],
    ("communication services", ""):          ["GOOGL", "META", "NFLX", "DIS", "T"],
    # Energy
    ("energy", "oil & gas"):                 ["XOM", "CVX", "SHEL", "BP", "TTE"],
    ("energy", ""):                          ["XOM", "CVX", "SHEL", "BP", "TTE"],
    # Materials
    ("basic materials", "mining"):           ["BHP", "RIO", "GLEN.L", "AAL.L", "FCX"],
    ("basic materials", "chemicals"):        ["LIN", "APD", "SHW", "DD", "EMN"],
    ("basic materials", ""):                 ["BHP", "RIO", "LIN", "APD", "NEM"],
    # Healthcare
    ("healthcare", "pharma"):                ["JNJ", "PFE", "MRK", "AZN", "GSK.L"],
    ("healthcare", "biotech"):               ["AMGN", "GILD", "BIIB", "REGN", "VRTX"],
    ("healthcare", "medical devices"):       ["MDT", "ABT", "SYK", "EW", "BSX"],
    ("healthcare", ""):                      ["JNJ", "PFE", "MRK", "AZN", "UNH"],
    # Financials
    ("financials", "banks"):                 ["JPM", "BAC", "WFC", "C", "HSBA.L"],
    ("financials", "insurance"):             ["BRK-B", "MET", "PRU", "AIG", "ALL"],
    ("financials", "asset management"):      ["BLK", "SCHW", "MS", "GS", "BEN"],
    ("financials", ""):                      ["JPM", "BAC", "GS", "MS", "BLK"],
    # Real Estate
    ("real estate", ""):                     ["AMT", "PLD", "EQIX", "SPG", "O"],
    # Utilities
    ("utilities", ""):                       ["NEE", "DUK", "SO", "D", "EXC"],
}

_MAX_PEERS = 5
_YAHOO_SLEEP = 0.3   # be polite to Yahoo


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def fetch_comparables(
    ticker:    str,
    sector:    str | None,
    industry:  str | None,
    stats:     dict | None = None,
    valued:    dict | None = None,
) -> dict:
    """
    Build the comparables dataset using the weighted peer selection engine.

    Parameters
    ----------
    ticker   : target company ticker (Yahoo format)
    sector   : from Yahoo stats, e.g. "Industrials"
    industry : from Yahoo stats, e.g. "Aerospace & Defense"
    stats    : full stats dict from data_engine (for target company metrics)
    valued   : full pipeline output (for target EV/equity bridge etc.)

    Returns the comparables dict described in the module docstring.
    """
    notes: list[str] = []

    # ── 1. Build target company metrics + scoring profile ────────────────────
    target         = _build_target_metrics(ticker, stats, valued)
    target_profile = _build_target_profile(target, stats, valued)

    # ── 2. Scored peer selection ──────────────────────────────────────────────
    selection = select_peers(ticker, sector, industry, target_profile, notes)
    peers     = selection["selected"]   # already have all financial fields
    peer_source = f"scored engine ({selection['method']})"

    # ── 3. Fallback: if scoring returned nothing, use static lookup ───────────
    if not peers:
        notes.append("Scored selection returned no peers — falling back to static lookup table.")
        fallback_tickers, fallback_source = _get_peers(ticker, sector, industry, notes)
        peers = []
        for pt in fallback_tickers:
            if pt.upper() == ticker.upper():
                continue
            m = _fetch_peer_metrics(pt)
            if m:
                peers.append(m)
            if len(peers) >= _MAX_PEERS:
                break
            time.sleep(_YAHOO_SLEEP)
        peer_source = f"static fallback ({fallback_source})"
        notes.append(f"Fallback peer source: {peer_source} ({len(peers)} fetched).")

    if not peers:
        return {
            "available": False,
            "reason":    "Could not fetch data for any peer companies.",
            "target":    target,
            "peers":     [],
            "median":    {},
            "implied_from_peers": {},
            "peer_selection": selection,
            "notes":     notes,
        }

    # ── 4. Compute medians ────────────────────────────────────────────────────
    peer_median = _compute_medians(peers)

    # ── 5. Implied valuation from peer multiples ──────────────────────────────
    implied = _implied_valuation(target, peer_median, stats, valued)

    notes.append(f"Peer source: {peer_source} ({len(peers)} peers used).")

    return {
        "available":          True,
        "reason":             None,
        "target":             target,
        "peers":              peers,
        "median":             peer_median,
        "implied_from_peers": implied,
        "peer_selection":     selection,   # full scored output incl. rejected
        "notes":              notes,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Peer sourcing
# ─────────────────────────────────────────────────────────────────────────────

def _get_peers(
    ticker: str,
    sector: str | None,
    industry: str | None,
    notes: list,
) -> tuple[list[str], str]:
    """Return (peer_ticker_list, source_description)."""

    # Try FMP first
    if fmp_available():
        result = fetch_fmp_peers(ticker)
        if result["available"] and result["peers"]:
            return result["peers"], "FMP /stock_peers"
        notes.append(
            f"FMP peers not available ({result.get('reason', 'unknown')}) — "
            "falling back to sector lookup table."
        )

    # Sector lookup table
    peers = _lookup_peers(sector, industry)
    if peers:
        return peers, f"sector lookup table ({sector} / {industry})"

    notes.append(
        "No peer match found in lookup table. "
        "Add this sector/industry combination to _PEER_TABLE in comparables.py."
    )
    return [], "none"


def _lookup_peers(sector: str | None, industry: str | None) -> list[str]:
    """Match sector + industry (case-insensitive, partial industry match) → peers."""
    if not sector:
        return []

    s = sector.lower()
    ind = (industry or "").lower()

    # Try exact (sector, industry) match first
    for (sec_key, ind_key), peers in _PEER_TABLE.items():
        if sec_key in s and (not ind_key or ind_key in ind):
            return list(peers)

    # Sector-only fallback (ind_key == "")
    for (sec_key, ind_key), peers in _PEER_TABLE.items():
        if sec_key in s and ind_key == "":
            return list(peers)

    return []


# ─────────────────────────────────────────────────────────────────────────────
# Target company metrics
# ─────────────────────────────────────────────────────────────────────────────

def _build_target_metrics(
    ticker: str,
    stats:  dict | None,
    valued: dict | None,
) -> dict:
    s = stats or {}
    v = valued or {}

    base_val  = (v.get("valuation") or {}).get("base", {})
    bridge    = v.get("equity_bridge_inputs", {})
    norm      = v.get("normalised", {})
    years_d   = v.get("canonical_by_year", {})
    base_yr   = v.get("base_year", "")

    # EV from our DCF model
    ev    = base_val.get("enterprise_value")
    vps   = base_val.get("value_per_share")
    price = s.get("current_price")
    mc    = s.get("market_cap")
    ccy   = s.get("currency", "")

    # Revenue / EBITDA / EBIT from base year actuals
    by = years_d.get(str(base_yr), {})
    rev   = _safe(by.get("revenue"))
    ebitda= _safe(by.get("ebitda"))
    ebit  = _safe(by.get("ebit"))
    ni    = _safe(by.get("net_income"))
    debt  = _safe(bridge.get("debt"))
    cash  = _safe(bridge.get("plus_cash") or by.get("cash"))

    # Market EV (for market multiples)
    mkt_ev = None
    if mc and debt is not None and cash is not None:
        mkt_ev = mc + debt - cash

    ev_rev   = _div(mkt_ev or ev, rev)
    ev_ebitda= _div(mkt_ev or ev, ebitda)
    ev_ebit  = _div(mkt_ev or ev, ebit)
    pe       = _div(mc, ni) if mc and ni and ni > 0 else None

    return {
        "ticker":    ticker,
        "name":      s.get("company_name", ticker),
        "currency":  ccy,
        "ev":        mkt_ev or ev,
        "market_cap":mc,
        "revenue":   rev,
        "ebitda":    ebitda,
        "ebit":      ebit,
        "net_income":ni,
        "ev_revenue":  ev_rev,
        "ev_ebitda":   ev_ebitda,
        "ev_ebit":     ev_ebit,
        "pe":          pe,
        "price":       price,
        "dcf_vps":     vps,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Target scoring profile  (extends _build_target_metrics for peer_selector)
# ─────────────────────────────────────────────────────────────────────────────

def _build_target_profile(target: dict, stats: dict | None, valued: dict | None) -> dict:
    """
    Extend the target metrics dict with fields needed by the scoring engine.
    Returns a new dict — does not mutate target.

    Added keys: ebit_margin, ebitda_margin, gross_margin, revenue_growth,
                country, sector, industry, peer_sector, peer_industry
    """
    s    = stats  or {}
    v    = valued or {}
    norm = v.get("normalised", {}) or {}

    # Margin fields — try normalised first, fall back to deriving from absolute figures
    ebit_m  = _safe(norm.get("ebit_margin"))
    ebitda_m= _safe(norm.get("ebitda_margin"))
    gross_m = _safe(norm.get("gross_margin"))
    rev_g   = _safe(norm.get("revenue_cagr") or norm.get("revenue_growth"))

    # Fallback: derive ebit/ebitda margins from actuals if normalised not available
    rev   = target.get("revenue")
    ebit  = target.get("ebit")
    ebitda= target.get("ebitda")
    if ebit_m is None and rev and ebit:
        ebit_m = ebit / rev
    if ebitda_m is None and rev and ebitda:
        ebitda_m = ebitda / rev

    sector   = s.get("sector",   "")
    industry = s.get("industry", "")
    country  = s.get("country",  "")

    return {
        **target,
        "ebit_margin":    ebit_m,
        "ebitda_margin":  ebitda_m,
        "gross_margin":   gross_m,
        "revenue_growth": rev_g,
        "country":        country,
        "sector":         sector,
        "industry":       industry,
        "peer_sector":    sector,
        "peer_industry":  industry,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Peer metrics via Yahoo
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_peer_metrics(ticker: str) -> dict | None:
    """Fetch a peer company's metrics from Yahoo Finance .info dict."""
    try:
        info = yf.Ticker(ticker).info
    except Exception:
        return None

    if not info or info.get("regularMarketPrice") is None:
        return None

    ev     = _safe(info.get("enterpriseValue"))
    mc     = _safe(info.get("marketCap"))
    rev    = _safe(info.get("totalRevenue"))
    ebitda = _safe(info.get("ebitda"))
    ebit   = _safe(info.get("operatingIncome") or info.get("ebit"))
    ni     = _safe(info.get("netIncomeToCommon"))
    price  = _safe(info.get("regularMarketPrice") or info.get("currentPrice"))
    name   = info.get("shortName") or info.get("longName") or ticker
    ccy    = info.get("currency", "")

    # Sanity: skip if EV or revenue is missing/zero
    if not ev or not rev:
        return None

    return {
        "ticker":    ticker,
        "name":      name,
        "currency":  ccy,
        "ev":        ev,
        "market_cap":mc,
        "revenue":   rev,
        "ebitda":    ebitda,
        "ebit":      ebit,
        "net_income":ni,
        "ev_revenue":  _div(ev, rev),
        "ev_ebitda":   _div(ev, ebitda),
        "ev_ebit":     _div(ev, ebit),
        "pe":          _div(mc, ni) if mc and ni and ni > 0 else None,
        "price":       price,
        "dcf_vps":     None,   # not applicable for peers
    }


# ─────────────────────────────────────────────────────────────────────────────
# Analytics
# ─────────────────────────────────────────────────────────────────────────────

def _compute_medians(peers: list[dict]) -> dict:
    """Compute median multiples across peers (ignoring None values)."""
    def _med(field):
        vals = [p[field] for p in peers if p.get(field) is not None]
        return median(vals) if vals else None

    return {
        "ev_revenue": _med("ev_revenue"),
        "ev_ebitda":  _med("ev_ebitda"),
        "ev_ebit":    _med("ev_ebit"),
        "pe":         _med("pe"),
    }


def _implied_valuation(
    target: dict,
    peer_median: dict,
    stats:  dict | None,
    valued: dict | None,
) -> dict:
    """
    Apply peer median multiples to the target company's actuals to derive
    an implied EV and per-share value range.
    """
    bridge = (valued or {}).get("equity_bridge_inputs", {})
    debt   = _safe(bridge.get("debt")) or 0
    cash   = _safe(bridge.get("plus_cash")) or 0
    lease  = _safe(bridge.get("less_lease_liabilities")) or 0
    shares = _safe(bridge.get("shares_outstanding"))

    def _ev_to_vps(impl_ev):
        if impl_ev is None or not shares or shares == 0:
            return None
        eq = impl_ev - debt - lease + cash
        return eq / shares if eq > 0 else None

    rev    = target.get("revenue")
    ebitda = target.get("ebitda")
    ebit   = target.get("ebit")

    impl_ev_rev   = _mult(rev,    peer_median.get("ev_revenue"))
    impl_ev_ebitda= _mult(ebitda, peer_median.get("ev_ebitda"))
    impl_ev_ebit  = _mult(ebit,   peer_median.get("ev_ebit"))

    vps_rev   = _ev_to_vps(impl_ev_rev)
    vps_ebitda= _ev_to_vps(impl_ev_ebitda)
    vps_ebit  = _ev_to_vps(impl_ev_ebit)

    vps_vals = [v for v in [vps_rev, vps_ebitda, vps_ebit] if v is not None]
    vps_range = [min(vps_vals), median(vps_vals), max(vps_vals)] if vps_vals else []

    return {
        "ev_from_revenue_multiple":  impl_ev_rev,
        "ev_from_ebitda_multiple":   impl_ev_ebitda,
        "ev_from_ebit_multiple":     impl_ev_ebit,
        "vps_from_revenue_multiple": vps_rev,
        "vps_from_ebitda_multiple":  vps_ebitda,
        "vps_from_ebit_multiple":    vps_ebit,
        "vps_range":                 vps_range,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Micro helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None   # NaN check
    except (TypeError, ValueError):
        return None


def _div(num: float | None, denom: float | None) -> float | None:
    if num is None or not denom or denom == 0:
        return None
    result = num / denom
    # Sanity: multiples above 500x are almost certainly stale/bad data
    return result if 0 < result < 500 else None


def _mult(base: float | None, multiple: float | None) -> float | None:
    if base is None or multiple is None:
        return None
    return base * multiple
