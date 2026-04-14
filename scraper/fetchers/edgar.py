import requests

# SEC requires a User-Agent header identifying the requester
HEADERS = {
    "User-Agent": "DCF-Tool research@example.com",
    "Accept-Encoding": "gzip, deflate",
}

COMPANY_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index?q=%22{query}%22&dateRange=custom&startdt=2020-01-01&forms=10-K"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"


def find_cik(ticker: str) -> str | None:
    """Look up the SEC CIK number for a given ticker symbol."""
    resp = requests.get(COMPANY_TICKERS_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    ticker_upper = ticker.upper().split(".")[0]  # strip exchange suffix (e.g. .L)

    for entry in data.values():
        if entry.get("ticker", "").upper() == ticker_upper:
            cik = str(entry["cik_str"]).zfill(10)
            return cik

    return None


def fetch_edgar_data(ticker: str) -> dict:
    """
    Pull the latest annual figures from SEC EDGAR for a US-listed company.
    Returns a dict with validated figures, or an empty result if not found.
    """
    cik = find_cik(ticker)

    if not cik:
        return {
            "source": "edgar",
            "available": False,
            "reason": f"Ticker '{ticker}' not found in SEC EDGAR (non-US or unlisted).",
            "latest_year": None,
            "financials": {},
        }

    facts = _fetch_company_facts(cik)
    if not facts:
        return {
            "source": "edgar",
            "available": False,
            "reason": "Could not retrieve XBRL facts from EDGAR.",
            "latest_year": None,
            "financials": {},
        }

    financials, latest_year = _extract_annual_financials(facts)

    return {
        "source": "edgar",
        "available": True,
        "cik": cik,
        "latest_year": latest_year,
        "financials": financials,
    }


# --- Helpers ---

def _fetch_company_facts(cik: str) -> dict | None:
    url = COMPANY_FACTS_URL.format(cik=cik)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [EDGAR] Warning: could not fetch company facts — {e}")
        return None


def _extract_annual_financials(facts: dict) -> tuple[dict, str | None]:
    """
    Pull the most recent annual 10-K figures from XBRL company facts.
    Returns (financials_dict, latest_fiscal_year_string).
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})

    # Map of our label -> list of possible XBRL concept names (first match wins)
    concept_map = {
        "revenue": [
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "Revenues",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
        ],
        "net_income": [
            "NetIncomeLoss",
            "ProfitLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic",
        ],
        "operating_income": [
            "OperatingIncomeLoss",
        ],
        "operating_cash_flow": [
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        ],
        "capex": [
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "CapitalExpendituresIncurredButNotYetPaid",
        ],
        "total_assets": [
            "Assets",
        ],
        "total_debt": [
            "LongTermDebt",
            "LongTermDebtAndCapitalLeaseObligations",
        ],
        "cash_and_equivalents": [
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsAndShortTermInvestments",
        ],
    }

    financials = {}
    all_years = []

    for label, concepts in concept_map.items():
        value, year = _get_latest_annual(us_gaap, concepts)
        financials[label] = value
        if year:
            all_years.append(year)

    latest_year = max(all_years) if all_years else None
    return financials, latest_year


def _get_latest_annual(us_gaap: dict, concepts: list[str]) -> tuple[float | None, str | None]:
    """
    Try each concept name in order. Return the most recent annual (10-K) value.
    """
    for concept in concepts:
        data = us_gaap.get(concept)
        if not data:
            continue

        units = data.get("units", {})
        # Values are usually in USD
        entries = units.get("USD") or units.get("shares") or []

        # Filter to annual 10-K filings only
        annual = [e for e in entries if e.get("form") == "10-K" and e.get("fp") == "FY"]
        if not annual:
            continue

        # Sort by end date descending, take the most recent
        annual.sort(key=lambda e: e["end"], reverse=True)
        latest = annual[0]
        return float(latest["val"]), latest["end"][:4]  # year string

    return None, None
