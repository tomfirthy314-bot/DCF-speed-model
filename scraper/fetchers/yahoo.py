"""
Yahoo Finance fetcher (via yfinance library) — primary data source.

Pulls:
  - Income statement: revenue, gross profit, operating income, EBIT, EBITDA,
                      net income, interest expense, pre-tax income, tax provision
  - Balance sheet:    total assets, total debt, short-term debt, cash,
                      total equity, retained earnings, current assets/liabilities
  - Cash flow:        operating CF, capex, free cash flow, D&A
  - Key stats:        market cap, beta, shares outstanding, current price,
                      sector, country, trailing/forward PE, price-to-book, ROE, ROA
"""

import time
import yfinance as yf
import pandas as pd
import requests


def _yf_info_with_retry(ticker: str, retries: int = 3) -> dict:
    """Fetch yfinance info with exponential backoff on rate-limit errors."""
    for attempt in range(retries):
        try:
            info = yf.Ticker(ticker).info
            return info
        except Exception as e:
            msg = str(e).lower()
            if "too many requests" in msg or "rate limit" in msg or "429" in msg:
                wait = 4 ** attempt  # 1s, 4s, 16s
                time.sleep(wait)
            else:
                raise
    return {}  # exhausted retries

# ---------------------------------------------------------------------------
# Well-known name aliases → preferred ticker
# Catches the very common cases instantly without a network round-trip.
# ---------------------------------------------------------------------------
_NAME_ALIASES: dict[str, str] = {
    "google":       "GOOGL",
    "alphabet":     "GOOGL",
    "microsoft":    "MSFT",
    "apple":        "AAPL",
    "amazon":       "AMZN",
    "meta":         "META",
    "facebook":     "META",
    "tesla":        "TSLA",
    "nvidia":       "NVDA",
    "netflix":      "NFLX",
    "salesforce":   "CRM",
    "uber":         "UBER",
    "airbnb":       "ABNB",
    "spotify":      "SPOT",
    "palantir":     "PLTR",
    "shopify":      "SHOP",
    # UK blue-chips
    "rolls royce":  "RR.L",
    "rolls-royce":  "RR.L",
    "bp":           "BP.L",
    "shell":        "SHEL.L",
    "hsbc":         "HSBA.L",
    "barclays":     "BARC.L",
    "lloyds":       "LLOY.L",
    "astrazeneca":  "AZN.L",
    "gsk":          "GSK.L",
    "glaxo":        "GSK.L",
    "unilever":     "ULVR.L",
    "diageo":       "DGE.L",
    "vodafone":     "VOD.L",
    "bt":           "BT-A.L",
    "tesco":        "TSCO.L",
    "marks spencer": "MKS.L",
    "marks & spencer": "MKS.L",
    "national grid": "NG.L",
    # European
    "volkswagen":   "VOW3.DE",
    "vw":           "VOW3.DE",
    "bmw":          "BMW.DE",
    "mercedes":     "MBG.DE",
    "siemens":      "SIE.DE",
    "sap":          "SAP.DE",
    "lvmh":         "MC.PA",
    "airbus":       "AIR.PA",
    "nestle":       "NESN.SW",
    "novartis":     "NOVN.SW",
    "roche":        "ROG.SW",
    "asml":         "ASML.AS",
    "toyota":       "TM",
    "sony":         "SONY",
    "samsung":      "005930.KS",
}


def _yahoo_search(query: str) -> tuple[str, str] | None:
    """
    Hit Yahoo Finance's search API and return (ticker, name) for the best
    equity match, or None if nothing useful comes back.
    """
    try:
        url = "https://query1.finance.yahoo.com/v1/finance/search"
        params = {"q": query, "quotesCount": 6, "newsCount": 0, "enableFuzzyQuery": True}
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, params=params, headers=headers, timeout=8)
        resp.raise_for_status()
        quotes = resp.json().get("quotes", [])
        # Prefer EQUITY type; skip mutual funds, ETFs, index entries
        for q in quotes:
            if q.get("quoteType", "").upper() in ("EQUITY", "MUTUALFUND"):
                if q.get("quoteType", "").upper() == "EQUITY":
                    return q["symbol"], q.get("longname") or q.get("shortname") or q["symbol"]
        # Fall back to first result of any type if nothing else
        if quotes:
            q = quotes[0]
            return q["symbol"], q.get("longname") or q.get("shortname") or q["symbol"]
    except Exception:
        pass
    return None


def search_ticker(company_input: str) -> tuple[str, str]:
    """
    Resolve a company name or ticker to (ticker, full_company_name).

    Resolution order:
    1. Static alias table  — catches "Google", "Rolls Royce" etc. instantly
    2. Direct yfinance lookup — works for any valid ticker (AAPL, RR.L, SAP.DE)
    3. Yahoo Finance search API — fuzzy name search for everything else
    """
    clean = company_input.strip()

    # 1. Alias table (case-insensitive)
    alias_ticker = _NAME_ALIASES.get(clean.lower())
    if alias_ticker:
        info = _yf_info_with_retry(alias_ticker)
        if info.get("symbol"):
            return info["symbol"], info.get("longName", alias_ticker)

    # 2. Direct ticker lookup (works instantly for AAPL, GOOGL, RR.L, SAP.DE …)
    info = _yf_info_with_retry(clean)
    if info.get("symbol") and info.get("regularMarketPrice") is not None:
        return info["symbol"], info.get("longName", clean)

    # 3. Yahoo Finance search API — fuzzy name matching
    result = _yahoo_search(clean)
    if result:
        resolved_ticker, resolved_name = result
        info2 = _yf_info_with_retry(resolved_ticker)
        if info2.get("symbol"):
            return info2["symbol"], info2.get("longName", resolved_name)
        return resolved_ticker, resolved_name

    raise ValueError(
        f"Could not resolve '{company_input}' to a listed company. "
        "Try a ticker directly (e.g. 'AAPL', 'RR.L', 'SAP.DE')."
    )


def fetch_yahoo_data(ticker: str) -> dict:
    """
    Pull all three financial statements plus key stats from Yahoo Finance.
    Returns a structured dict keyed by year for financial data.
    """
    t    = yf.Ticker(ticker)
    info = _yf_info_with_retry(ticker)

    # -------------------------------------------------------------------------
    # Key stats & ratios (point-in-time, not historical)
    # -------------------------------------------------------------------------
    # Yahoo Finance reports UK/LSE stock prices in GBp (pence) but financial
    # statements in GBP (pounds). Detect this and convert price to GBP so
    # everything downstream stays on the same currency unit.
    financial_currency = info.get("financialCurrency", "")
    price_currency     = info.get("currency", "")
    raw_price = info.get("currentPrice") or info.get("regularMarketPrice")
    if price_currency == "GBp" and raw_price is not None:
        current_price = raw_price / 100   # pence → pounds
    else:
        current_price = raw_price

    stats = {
        "ticker":               ticker,
        "company_name":         info.get("longName"),
        "currency":             financial_currency,   # always the financial statement currency
        "exchange":             info.get("exchange"),
        "sector":               info.get("sector"),
        "industry":             info.get("industry"),
        "country":              info.get("country"),

        # Market data (price normalised to financial statement currency)
        "current_price":        current_price,
        "market_cap":           info.get("marketCap"),
        "shares_outstanding":   info.get("sharesOutstanding"),
        "shares_float":         info.get("floatShares"),

        # Risk / WACC inputs
        "beta":                 info.get("beta"),
        "trailing_pe":          info.get("trailingPE"),
        "forward_pe":           info.get("forwardPE"),
        "price_to_book":        info.get("priceToBook"),

        # Profitability
        "return_on_equity":     info.get("returnOnEquity"),
        "return_on_assets":     info.get("returnOnAssets"),
        "profit_margins":       info.get("profitMargins"),
        "operating_margins":    info.get("operatingMargins"),
        "gross_margins":        info.get("grossMargins"),

        # Growth
        "revenue_growth":       info.get("revenueGrowth"),
        "earnings_growth":      info.get("earningsGrowth"),

        # Debt & liquidity
        "total_debt":           info.get("totalDebt"),
        "cash":                 info.get("totalCash"),
        "current_ratio":        info.get("currentRatio"),
        "debt_to_equity":       info.get("debtToEquity"),

        # Dividends
        "dividend_yield":       info.get("dividendYield"),
        "payout_ratio":         info.get("payoutRatio"),
    }

    # -------------------------------------------------------------------------
    # Financial statements (annual, last 4-5 years)
    # -------------------------------------------------------------------------
    income_stmt  = _clean(t.financials)
    balance_sheet = _clean(t.balance_sheet)
    cash_flow    = _clean(t.cashflow)

    years = income_stmt.columns.tolist() if not income_stmt.empty else []

    financials_by_year: dict[str, dict] = {}
    for col in years:
        year = str(col.year) if hasattr(col, "year") else str(col)

        financials_by_year[year] = {
            # --- Income statement ---
            "revenue":              _get(income_stmt,  "Total Revenue",               col),
            "gross_profit":         _get(income_stmt,  "Gross Profit",                col),
            "operating_income":     _get(income_stmt,  "Operating Income",            col),
            # "ebit" intentionally not scraped — derived by canonical_fundamentals only
            "ebitda":               _get(income_stmt,  "EBITDA",                      col),
            "pre_tax_income":       _get(income_stmt,  "Pretax Income",               col),
            "tax_provision":        _get(income_stmt,  "Tax Provision",               col),
            "net_income":           _get(income_stmt,  "Net Income",                  col),
            "interest_expense":     _get(income_stmt,  "Interest Expense",            col),

            # --- Balance sheet: assets ---
            "total_assets":         _get(balance_sheet, "Total Assets",                        col),
            "current_assets":       _get(balance_sheet, "Current Assets",                      col),
            "accounts_receivable":  _get(balance_sheet, "Accounts Receivable",                 col),
            "inventory":            _get(balance_sheet, "Inventory",                           col),

            # --- Balance sheet: liabilities ---
            "current_liabilities":  _get(balance_sheet, "Current Liabilities",                 col),
            "accounts_payable":     _get(balance_sheet, "Accounts Payable",                    col),
            "deferred_revenue":     _get(balance_sheet, "Deferred Revenue",                    col),
            "total_debt":           _get(balance_sheet, "Total Debt",                          col),
            "long_term_debt":       _get(balance_sheet, "Long Term Debt",                      col),
            "lease_liabilities":    (_get(balance_sheet, "Finance Lease Obligations", col)
                                    or _get(balance_sheet, "Capital Lease Obligations", col)),

            # --- Balance sheet: equity ---
            "cash_and_equivalents": _get(balance_sheet, "Cash And Cash Equivalents",           col),
            "total_equity":         _get(balance_sheet, "Stockholders Equity",                 col),
            "retained_earnings":    _get(balance_sheet, "Retained Earnings",                   col),
            # Shares outstanding — pulled from balance sheet (not .info) so rate-limiting doesn't affect it
            "shares_outstanding":   (_get(balance_sheet, "Ordinary Shares Number",             col)
                                    or _get(balance_sheet, "Share Issued",                     col)
                                    or _get(income_stmt,  "Diluted Average Shares",            col)
                                    or _get(income_stmt,  "Basic Average Shares",              col)),

            # --- Cash flow ---
            "operating_cash_flow":  _get(cash_flow, "Operating Cash Flow",                     col),
            "capex":                _get(cash_flow, "Capital Expenditure",                     col),
            # "free_cash_flow" intentionally not scraped — derived by canonical_fundamentals only
            "depreciation_amort":   _get(cash_flow, "Depreciation And Amortization",           col),
            "change_in_working_cap":_get(cash_flow, "Change In Working Capital",               col),

            # --- EXCEL: fields calculated in the DCF model, not scraped ---
            # net_debt                = total_debt - cash_and_equivalents
            # cost_of_debt            = interest_expense / total_debt
            # effective_tax_rate      = tax_provision / pre_tax_income
            # nopat                   = ebit * (1 - effective_tax_rate)
            # ebitda_margin           = ebitda / revenue
            # ebit_margin             = ebit / revenue
            # gross_margin_pct        = gross_profit / revenue   (per year)
            # change_in_nwc_manual    = delta(current_assets - current_liabilities) if change_in_working_cap unavailable
            # capital_structure_debt  = total_debt / (total_debt + total_equity)
            # capital_structure_equity= total_equity / (total_debt + total_equity)
            # fcff                    = nopat + depreciation_amort - capex - change_in_nwc
            #
            # --- EXCEL: leave blank rows for analyst inputs (not publicly available) ---
            # risk_free_rate          → manual input (e.g. 10yr gilt / treasury yield on valuation date)
            # equity_risk_premium     → manual input (e.g. Damodaran ERP estimate)
            # wacc                    → calculated from cost_of_debt, cost_of_equity, capital_structure
            # maintenance_capex       → manual input or analyst estimate (rarely disclosed publicly)
            # growth_capex            → manual input or analyst estimate
            # terminal_growth_rate    → manual input (DCF assumption)
            # exit_multiple           → manual input (DCF assumption)
        }

    return {
        "source": "yahoo",
        "stats": stats,
        "financials_by_year": financials_by_year,
        "raw": {
            "income_stmt":   income_stmt,
            "balance_sheet": balance_sheet,
            "cash_flow":     cash_flow,
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _resolve_col(df: pd.DataFrame, col):
    """
    Return the column in df that best matches col.

    yfinance sometimes returns different date columns across the three
    statements (income, balance, cashflow) — either because:
      (a) one statement covers fewer years than another, or
      (b) one column is timezone-aware and another is timezone-naive.

    Strategy: exact match first; then fall back to same calendar year.
    If the year is absent entirely, return None so the caller yields None
    rather than raising a KeyError.
    """
    if col in df.columns:
        return col
    if not hasattr(col, "year"):
        return None
    try:
        same_year = [c for c in df.columns if hasattr(c, "year") and c.year == col.year]
        if not same_year:
            return None
        if len(same_year) == 1:
            return same_year[0]
        # Multiple columns in same year — pick closest by day count
        def _days(c):
            try:
                a = pd.Timestamp(col).tz_localize(None)
                b = pd.Timestamp(c).tz_localize(None)
                return abs((a - b).days)
            except Exception:
                return 999
        return min(same_year, key=_days)
    except Exception:
        return None


def _get(df: pd.DataFrame, row_name: str, col) -> float | None:
    """Retrieve one cell by row label and column; tolerates minor label variations."""
    if df.empty:
        return None
    actual_col = _resolve_col(df, col)
    if actual_col is None:
        return None
    if row_name in df.index:
        val = df.loc[row_name, actual_col]
    else:
        matches = [i for i in df.index if i.lower() == row_name.lower()]
        if not matches:
            return None
        val = df.loc[matches[0], actual_col]
    return float(val) if pd.notna(val) else None
