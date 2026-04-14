"""
Macrotrends.net scraper — historical trend data for DCF analysis.

Uses the internal iframe endpoint which returns clean JSON (chartData variable).
Values returned by Macrotrends are in billions; we convert to raw dollars
to stay consistent with Yahoo Finance and EDGAR.

Metric availability note:
- Income statement metrics: reliably available
- Cash flow (operating CF, free cash flow): available
- Balance sheet (assets, debt, cash): available
- Capex / margins: blocked by Cloudflare on this endpoint — use Yahoo instead.
"""

import re
import json
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.macrotrends.net/",
}

IFRAME_URL = (
    "https://www.macrotrends.net/production/stocks/desktop/fundamental_iframe.php"
    "?t={ticker}&type={metric_type}&statement={statement}&freq=A&sub="
)

# (our_label, macrotrends type slug, statement slug, unit)
# unit: "billions" → multiply by 1e9; "millions" → multiply by 1e6; "ratio" → as-is
METRICS = [
    ("revenue",                "revenue",                           "income-statement",  "billions"),
    ("gross_profit",           "gross-profit",                      "income-statement",  "billions"),
    ("operating_income",       "operating-income",                  "income-statement",  "billions"),
    # "ebit" intentionally not scraped — derived by canonical_fundamentals only
    ("ebitda",                 "ebitda",                            "income-statement",  "billions"),
    ("net_income",             "net-income",                        "income-statement",  "billions"),
    ("eps_diluted",            "eps-earnings-per-share-diluted",    "income-statement",  "ratio"),
    ("net_profit_margin_pct",  "net-profit-margin",                 "income-statement",  "ratio"),
    ("operating_cash_flow",    "cash-flow-from-operating-activities","cash-flow-statement","billions"),
    # "free_cash_flow" intentionally not scraped — derived by canonical_fundamentals only
    ("total_assets",           "total-assets",                      "balance-sheet",     "billions"),
    ("long_term_debt",         "long-term-debt",                    "balance-sheet",     "billions"),
    ("cash_and_equivalents",   "cash-on-hand",                      "balance-sheet",     "billions"),
    # shares_outstanding intentionally omitted — Macrotrends unit scaling is unreliable
    # for non-US companies (e.g. reports UK shares in billions not millions).
    # Yahoo Finance stats (sharesOutstanding) is the authoritative source.
]


def fetch_macrotrends_data(ticker: str, company_name: str) -> dict:
    """
    Scrape 10-20 years of historical financials from Macrotrends.
    Returns a structured dict with data by year.
    """
    # Strip exchange suffix for Macrotrends (e.g. RR.L → RR, SAP.DE → SAP)
    mt_ticker = ticker.split(".")[0].upper()

    print(f"  [Macrotrends] Fetching {mt_ticker} (derived from {ticker})...")

    results_by_metric: dict[str, dict[str, float]] = {}

    for label, metric_type, statement, unit in METRICS:
        data = _fetch_metric(mt_ticker, metric_type, statement, unit)
        results_by_metric[label] = data
        if data:
            years = sorted(data.keys())
            print(f"  [Macrotrends] {label}: {len(data)} years ({years[0]}–{years[-1]})")
        else:
            print(f"  [Macrotrends] {label}: no data")

    # Collect all years across all metrics
    all_years = sorted(
        {year for metric_data in results_by_metric.values() for year in metric_data},
        reverse=True,
    )

    if not all_years:
        return {
            "source": "macrotrends",
            "available": False,
            "reason": (
                f"No data found for ticker '{mt_ticker}'. "
                "Macrotrends covers US-listed companies primarily. "
                "Non-US companies may be unavailable."
            ),
            "years_available": [],
            "financials_by_year": {},
        }

    # Pivot: {year: {label: value}}
    financials_by_year = {
        year: {label: results_by_metric[label].get(year) for label, *_ in METRICS}
        for year in all_years
    }

    return {
        "source": "macrotrends",
        "available": True,
        "years_available": all_years,
        "financials_by_year": financials_by_year,
    }


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _fetch_metric(ticker: str, metric_type: str, statement: str, unit: str) -> dict[str, float]:
    """
    Call the Macrotrends iframe endpoint for one metric and parse the result.
    Returns {year_string: value_in_raw_dollars (or ratio)}.
    """
    url = IFRAME_URL.format(ticker=ticker, metric_type=metric_type, statement=statement)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return {}
    except Exception:
        return {}

    # Extract chartData JSON from the embedded JavaScript
    # Format: chartData = [{"date":"2024-09-30","v1":...,"v2":VALUE,"v3":YOY%}, ...]
    m = re.search(r"chartData\s*=\s*(\[.*)", resp.text)
    if not m:
        return {}

    raw_str = m.group(1)
    # The array ends at the first semicolon after the opening bracket
    bracket_depth = 0
    end_idx = 0
    for i, ch in enumerate(raw_str):
        if ch == "[":
            bracket_depth += 1
        elif ch == "]":
            bracket_depth -= 1
            if bracket_depth == 0:
                end_idx = i + 1
                break

    try:
        entries = json.loads(raw_str[:end_idx])
    except json.JSONDecodeError:
        return {}

    results: dict[str, float] = {}
    for entry in entries:
        date_str = entry.get("date", "")
        year = date_str[:4]
        if not year:
            continue

        # v2 = value for the current period (v1 = prior period, v3 = YoY %)
        val = entry.get("v2")
        if val is None or val == "NULL":
            val = entry.get("v1")
        if val is None or val == "NULL":
            continue

        try:
            val = float(val)
        except (ValueError, TypeError):
            continue

        # Convert to raw units
        if unit == "billions":
            val = val * 1_000_000_000
        elif unit == "millions":
            val = val * 1_000_000
        # ratio/percentage: leave as-is

        results[year] = val

    return results
