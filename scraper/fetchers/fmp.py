"""
Financial Modeling Prep (FMP) fetcher

Two roles:
  1. Cross-check  — fires ONLY when anomaly_detector has flagged fields that
                    need verification. Conserves the 250 req/day free-tier
                    limit by batching: one IS call + one BS call covers all
                    flagged income/balance-sheet fields regardless of how many
                    are flagged.

  2. Peer list    — used by comparables.py to fetch sector peers.
                    One API call returns a list of peer tickers.

Configuration:
  Set FMP_API_KEY environment variable before running, or pass key=... directly.
  If no key is present all functions degrade gracefully and return empty results.

Free tier limits (as of 2026):
  - 250 requests / day
  - 5 years of financial history
  - No real-time data; ~24h delay on financials

FMP ticker format:
  - US:   AAPL, MSFT, ...
  - UK:   RR.L, SHEL.L, ...  (same .L suffix as Yahoo)
  - EU:   AIR.PA, SAP.XETRA  (Yahoo conventions work)
"""

from __future__ import annotations

import os
import time
import requests
from datetime import datetime

_BASE = "https://financialmodelingprep.com/api/v3"
_TIMEOUT = 10   # seconds per request
_RATE_SLEEP = 0.4  # pause between FMP requests to stay well under rate limits

# ── Field mappings: FMP key → canonical pipeline key ─────────────────────────

_IS_MAP = {
    "revenue":                       "revenue",
    "grossProfit":                   "gross_profit",
    # "operatingIncome" → "ebit" intentionally not mapped — derived by canonical_fundamentals only
    "ebitda":                        "ebitda",
    "netIncome":                     "net_income",
    "depreciationAndAmortization":   "da",
    "interestExpense":               "interest_expense",
    "incomeTaxExpense":              "tax_provision",
    "eps":                           "eps_diluted",
}

_CF_MAP = {
    "operatingCashFlow":             "operating_cash_flow",
    "capitalExpenditure":            "capex",
    # "freeCashFlow" → "free_cash_flow" intentionally not mapped — derived by canonical_fundamentals only
}

_BS_MAP = {
    "totalDebt":                     "debt",
    "longTermDebt":                  "long_term_debt",
    "cashAndCashEquivalents":        "cash",
    "totalAssets":                   "total_assets",
    "totalEquity":                   "total_equity",
    "weightedAverageShsOutDil":      "shares_outstanding",
    "accountsReceivables":           "accounts_receivable",
    "inventory":                     "inventory",
    "accountPayables":               "accounts_payable",
    "totalLiabilities":              "total_liabilities",
    "shortTermDebt":                 "short_term_debt",
}

# Fields that live in each statement type (for deciding which calls to make)
_IS_CANONICAL = set(_IS_MAP.values())
_CF_CANONICAL = set(_CF_MAP.values())
_BS_CANONICAL = set(_BS_MAP.values())


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_api_key() -> str | None:
    # Check environment variable first, then Streamlit secrets (for cloud deployment)
    key = os.environ.get("FMP_API_KEY")
    if key:
        return key
    try:
        import streamlit as st
        return st.secrets.get("FMP_API_KEY")
    except Exception:
        return None


def is_available() -> bool:
    return bool(get_api_key())


# ─────────────────────────────────────────────────────────────────────────────
# Role 1: Cross-check
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fmp_crosscheck(
    ticker: str,
    flagged_fields: list[str],
    key: str | None = None,
) -> dict:
    """
    Fetch FMP data for fields that the anomaly detector has flagged.

    Only the minimum necessary statement calls are made:
      - IS  if any flagged field maps to income statement
      - BS  if any flagged field maps to balance sheet
      - CF  if any flagged field maps to cash flow statement

    Returns:
      {
        "available":  bool,
        "reason":     str | None,
        "calls_made": int,
        "by_year":    { "2024": { canonical_field: value, ... }, ... },
        "fields_covered": [ list of canonical fields returned ],
      }
    """
    key = key or get_api_key()
    if not key:
        return _unavailable("No FMP_API_KEY set — skipping cross-check.")

    if not flagged_fields:
        return _unavailable("No flagged fields to cross-check.")

    need_is = any(f in _IS_CANONICAL for f in flagged_fields)
    need_bs = any(f in _BS_CANONICAL for f in flagged_fields)
    need_cf = any(f in _CF_CANONICAL for f in flagged_fields)

    fmp_ticker = _to_fmp_ticker(ticker)
    by_year: dict[str, dict] = {}
    calls_made = 0
    fields_covered: list[str] = []

    if need_is:
        rows = _fetch_statement(fmp_ticker, "income-statement", key)
        calls_made += 1
        _merge_rows(rows, _IS_MAP, by_year, fields_covered)
        time.sleep(_RATE_SLEEP)

    if need_bs:
        rows = _fetch_statement(fmp_ticker, "balance-sheet-statement", key)
        calls_made += 1
        _merge_rows(rows, _BS_MAP, by_year, fields_covered)
        time.sleep(_RATE_SLEEP)

    if need_cf:
        rows = _fetch_statement(fmp_ticker, "cash-flow-statement", key)
        calls_made += 1
        _merge_rows(rows, _CF_MAP, by_year, fields_covered)
        time.sleep(_RATE_SLEEP)

    if not by_year:
        return _unavailable(
            f"FMP returned no data for {fmp_ticker}. "
            "Ticker format may differ from Yahoo convention."
        )

    return {
        "available":       True,
        "reason":          None,
        "calls_made":      calls_made,
        "by_year":         by_year,
        "fields_covered":  sorted(set(fields_covered)),
        "ticker_used":     fmp_ticker,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Role 2: Peer list
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fmp_peers(ticker: str, key: str | None = None) -> dict:
    """
    Return a list of peer tickers from FMP's stock_peers endpoint.

    Returns:
      {
        "available": bool,
        "reason":    str | None,
        "peers":     [ "AAPL", "MSFT", ... ]   (up to 10)
      }
    """
    key = key or get_api_key()
    if not key:
        return {"available": False, "reason": "No FMP_API_KEY set.", "peers": []}

    fmp_ticker = _to_fmp_ticker(ticker)
    url = f"{_BASE}/stock_peers"
    params = {"symbol": fmp_ticker, "apikey": key}

    try:
        resp = requests.get(url, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return {"available": False, "reason": f"FMP peers request failed: {e}", "peers": []}

    # Response is a list: [{"symbol": "RR.L", "peersList": ["BA", "LMT", ...]}]
    peers = []
    if isinstance(data, list) and data:
        peers = data[0].get("peersList", [])
    elif isinstance(data, dict):
        peers = data.get("peersList", [])

    if not peers:
        return {
            "available": False,
            "reason":    f"FMP returned no peers for {fmp_ticker}.",
            "peers":     [],
        }

    return {
        "available": True,
        "reason":    None,
        "peers":     peers[:10],   # cap at 10
    }


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_fmp_ticker(ticker: str) -> str:
    """
    FMP uses the same .L suffix as Yahoo for LSE companies, and .PA for
    Euronext Paris etc.  Generally Yahoo and FMP tickers are compatible,
    but FMP sometimes drops the exchange suffix for major US names.
    Return as-is and let the caller handle 404s.
    """
    return ticker


def _fetch_statement(ticker: str, statement: str, key: str) -> list[dict]:
    """Fetch one statement from FMP.  Returns list of annual rows or []."""
    url = f"{_BASE}/{statement}/{ticker}"
    params = {"limit": 5, "apikey": key}
    try:
        resp = requests.get(url, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "financials" in data:
            return data["financials"]   # older API format
        return []
    except Exception:
        return []


def _merge_rows(
    rows: list[dict],
    field_map: dict,
    by_year: dict,
    fields_covered: list,
) -> None:
    """Parse FMP rows and merge into by_year dict using canonical field names."""
    for row in rows:
        date_str = row.get("date", "")
        try:
            year = str(datetime.strptime(date_str[:10], "%Y-%m-%d").year)
        except (ValueError, TypeError):
            continue

        if year not in by_year:
            by_year[year] = {}

        for fmp_key, canonical in field_map.items():
            raw = row.get(fmp_key)
            if raw is None:
                continue
            try:
                val = float(raw)
            except (TypeError, ValueError):
                continue
            by_year[year][canonical] = val
            if canonical not in fields_covered:
                fields_covered.append(canonical)


def _unavailable(reason: str) -> dict:
    return {
        "available":       False,
        "reason":          reason,
        "calls_made":      0,
        "by_year":         {},
        "fields_covered":  [],
    }
