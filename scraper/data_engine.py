from scraper.fetchers.yahoo import search_ticker, fetch_yahoo_data
from scraper.fetchers.edgar import fetch_edgar_data
from scraper.fetchers.macrotrends import fetch_macrotrends_data
from scraper.fetchers.companies_house import fetch_companies_house
from scraper.fetchers.fmp import fetch_fmp_stats, fetch_fmp_financials, is_available as fmp_available


def run_data_engine(company_input: str) -> dict | None:
    print(f"\n{'='*60}")
    print(f"  Company Research: {company_input}")
    print(f"{'='*60}\n")

    # ----------------------------------------------------------------
    # Step 1: Resolve ticker via Yahoo Finance
    # ----------------------------------------------------------------
    print("[1/4] Resolving company ticker...")
    try:
        ticker, company_name = search_ticker(company_input)
    except ValueError as e:
        print(f"\n  ERROR: {e}")
        return None

    print(f"  Ticker  : {ticker}")
    print(f"  Company : {company_name}\n")

    # ----------------------------------------------------------------
    # Step 2: Yahoo Finance — primary data pull
    # ----------------------------------------------------------------
    print("[2/4] Fetching Yahoo Finance data (primary source)...")
    yahoo = fetch_yahoo_data(ticker)
    stats = yahoo["stats"]
    yahoo_years = yahoo["financials_by_year"]

    # FMP stats fallback — fires when Yahoo .info was rate-limited and left
    # critical fields empty (price, market cap, currency, sector, etc.)
    _yahoo_gaps = [k for k in ("current_price", "market_cap", "currency", "sector", "beta")
                   if not stats.get(k)]
    fmp_stats_data = None
    if _yahoo_gaps and fmp_available():
        print(f"  Yahoo missing: {_yahoo_gaps} — trying FMP for market stats...")
        fmp_stats_data = fetch_fmp_stats(ticker)
        if fmp_stats_data.get("available"):
            for k, v in fmp_stats_data.items():
                if k in ("available", "reason"):
                    continue
                if stats.get(k) is None and v is not None:
                    stats[k] = v
            print(f"  FMP stats filled: {[k for k in _yahoo_gaps if stats.get(k)]}")
        else:
            print(f"  FMP stats unavailable — {fmp_stats_data.get('reason', '')}")

    print(f"  Currency      : {stats.get('currency')}")
    print(f"  Market Cap    : {_fmt(stats.get('market_cap'))}")
    print(f"  Current Price : {stats.get('current_price')}")
    print(f"  Beta          : {stats.get('beta')}")
    print(f"  Shares Out.   : {_fmt(stats.get('shares_outstanding'))}")
    print(f"  Sector        : {stats.get('sector')}")
    print(f"  Years avail.  : {sorted(yahoo_years.keys(), reverse=True)}")
    print()

    # ----------------------------------------------------------------
    # Step 3: SEC EDGAR — accuracy validator (US companies only)
    # ----------------------------------------------------------------
    print("[3/4] Validating against SEC EDGAR (US companies only)...")

    country = stats.get("country", "")
    if country and country.lower() not in ("united states", "us"):
        edgar = {
            "available": False,
            "reason": f"Company is registered in {country} — EDGAR covers US-listed companies only.",
        }
        print(f"  Skipped — {edgar['reason']}")
    else:
        edgar = fetch_edgar_data(ticker)

        if edgar["available"]:
            # Sanity check: if EDGAR revenue is less than 1% of Yahoo revenue,
            # it has almost certainly found the wrong entity (e.g. a US subsidiary).
            yahoo_rev = next(
                (v.get("revenue") for v in yahoo_years.values() if v.get("revenue")),
                None,
            )
            edgar_rev = edgar["financials"].get("revenue")
            if yahoo_rev and edgar_rev and edgar_rev < yahoo_rev * 0.01:
                edgar = {
                    "available": False,
                    "reason": (
                        f"EDGAR returned revenue of {_fmt(edgar_rev)} vs Yahoo's "
                        f"{_fmt(yahoo_rev)} — likely a wrong entity (US subsidiary). "
                        "EDGAR data discarded."
                    ),
                }
                print(f"  Discarded — {edgar['reason']}")
            else:
                print(f"  CIK found    : {edgar['cik']}")
                print(f"  Latest year  : {edgar['latest_year']}")
                ef = edgar["financials"]
                print(f"  Revenue      : {_fmt(ef.get('revenue'))}")
                print(f"  Net Income   : {_fmt(ef.get('net_income'))}")
                print(f"  Op. Cash Fl  : {_fmt(ef.get('operating_cash_flow'))}")
        else:
            print(f"  Not available — {edgar['reason']}")
    print()

    # ----------------------------------------------------------------
    # Step 4: Macrotrends — historical trend data
    # ----------------------------------------------------------------
    print("[4/4] Scraping Macrotrends (10-20yr historical trends)...")
    macrotrends = fetch_macrotrends_data(ticker, company_name)

    if macrotrends["available"]:
        yrs = macrotrends["years_available"]
        print(f"  Years available: {yrs[:5]}{'...' if len(yrs) > 5 else ''} ({len(yrs)} total)")
    else:
        print(f"  Not available — {macrotrends.get('reason', 'unknown')}")
    print()

    # ----------------------------------------------------------------
    # Step 5: Companies House  (UK companies only — statutory anchor)
    # ----------------------------------------------------------------
    print("[5/6] Companies House (UK statutory registry)...")
    ch = fetch_companies_house(ticker, company_name, country=country)

    if ch["available"]:
        print(f"  Company No.   : {ch['company_number']}")
        print(f"  Registered    : {ch['registered_name']}")
        print(f"  Status        : {ch['status']}")
        print(f"  SIC           : {', '.join(ch['sic_codes'])}  "
              f"({', '.join(ch['sic_descriptions'])})")
        print(f"  Last accounts : {ch['last_accounts_date'] or 'unknown'}")
        for note in ch["notes"]:
            print(f"  Note          : {note}")
    else:
        print(f"  Skipped — {ch['reason']}")
    print()

    # ----------------------------------------------------------------
    # Step 5b: FMP financials — backup for years with sparse Yahoo data
    # ----------------------------------------------------------------
    fmp_financials = {"available": False, "financials_by_year": {}}
    if fmp_available():
        yahoo_year_count = len(yahoo_years)
        yahoo_has_gaps   = yahoo_year_count < 3 or any(
            yahoo_years.get(y, {}).get("revenue") is None
            for y in list(yahoo_years)[:2]
        )
        if yahoo_has_gaps or _yahoo_gaps:
            print("[5b] Fetching FMP financials (backup source)...")
            fmp_financials = fetch_fmp_financials(ticker)
            if fmp_financials["available"]:
                print(f"  FMP years: {fmp_financials['years_available'][:5]}")
            else:
                print(f"  FMP financials unavailable — {fmp_financials.get('reason', '')}")
            print()

    # ----------------------------------------------------------------
    # Step 6: Cross-reference & reconcile
    # ----------------------------------------------------------------
    print("[6/6] Cross-referencing sources...")
    reconciled = _reconcile(yahoo_years, edgar, macrotrends, fmp_financials)

    # Guard: if we have no financial years at all, the company is almost certainly
    # private, delisted, or misidentified. Fail clearly rather than crashing later.
    if not reconciled:
        co = company_name or company_input
        print(f"\n  ERROR: No financial statement data found for '{co}' ({ticker}).")
        print("  This usually means the company is privately held, not exchange-listed,")
        print("  or the name resolved to the wrong entity.")
        print("  Try a listed peer instead (e.g. for Decathlon, there is no public ticker).")
        return None

    _print_summary(reconciled, stats)

    print("\n" + "="*60)
    print("  Phase 1 complete.")
    print("="*60 + "\n")

    return {
        "ticker": ticker,
        "company_name": company_name,
        "stats": stats,
        "reconciled": reconciled,
        "raw": {
            "yahoo": yahoo,
            "edgar": edgar,
            "macrotrends": macrotrends,
            "companies_house": ch,
        },
    }


# ---------------------------------------------------------------------------
# Reconciliation logic
# ---------------------------------------------------------------------------

def _reconcile(yahoo_years: dict, edgar: dict, macrotrends: dict,
               fmp_financials: dict | None = None) -> dict:
    """
    Merge all sources into a single dataset.

    Priority rules:
      1. Yahoo Finance is the base for recent years (typically last 4-5 years).
      2. EDGAR overrides Yahoo for the latest annual period on US companies.
      3. Macrotrends backfills historical years beyond Yahoo's coverage
         and adds long-run trend fields (EPS, margins as %).
      4. FMP fills gaps where Yahoo is missing fields (rate-limited) and
         backfills years not covered by Yahoo.
    """
    reconciled: dict[str, dict] = {}

    # --- Base: Yahoo ---
    for year, data in yahoo_years.items():
        reconciled[year] = dict(data)
        reconciled[year]["_sources"] = {k: "yahoo" for k in data}

    # --- EDGAR override on latest year ---
    if edgar["available"] and edgar["latest_year"]:
        ey = edgar["latest_year"]
        ef = edgar["financials"]

        if ey not in reconciled:
            reconciled[ey] = {"_sources": {}}

        for field in (
            "revenue", "net_income", "operating_income",
            "operating_cash_flow", "capex",
            "total_assets", "total_debt", "cash_and_equivalents",
        ):
            edgar_val = ef.get(field)
            if edgar_val is None:
                continue

            yahoo_val = reconciled[ey].get(field)
            if yahoo_val is not None:
                pct_diff = abs(edgar_val - yahoo_val) / max(abs(yahoo_val), 1) * 100
                status = "MISMATCH" if pct_diff > 5 else "OK"
                arrow = " → using EDGAR" if pct_diff > 5 else ""
                print(f"  [{status}] {field} ({ey}): "
                      f"Yahoo={_fmt(yahoo_val)} vs EDGAR={_fmt(edgar_val)} "
                      f"({pct_diff:.1f}% diff){arrow}")

            reconciled[ey][field] = edgar_val
            reconciled[ey]["_sources"][field] = "edgar"

    # --- Macrotrends: backfill historical years + enrich recent years ---
    if macrotrends["available"]:
        for year, mt_data in macrotrends["financials_by_year"].items():
            if year not in reconciled:
                # Historical year not covered by Yahoo — add it from Macrotrends
                reconciled[year] = {}
                reconciled[year]["_sources"] = {}
                for field, val in mt_data.items():
                    if val is not None:
                        reconciled[year][field] = val
                        reconciled[year]["_sources"][field] = "macrotrends"
            else:
                # Year already exists — add Macrotrends-only fields (EPS, margin %)
                for field in ("eps_diluted", "net_profit_margin_pct", "shares_outstanding"):
                    val = mt_data.get(field)
                    if val is not None and field not in reconciled[year]:
                        reconciled[year][field] = val
                        reconciled[year]["_sources"][field] = "macrotrends"

    # --- FMP: fill gaps and backfill years not covered by Yahoo ---
    if fmp_financials and fmp_financials.get("available"):
        for year, fmp_data in fmp_financials["financials_by_year"].items():
            if year not in reconciled:
                # Year entirely missing from Yahoo/Macrotrends — add from FMP
                reconciled[year] = {"_sources": {}}
                for field, val in fmp_data.items():
                    if val is not None:
                        reconciled[year][field] = val
                        reconciled[year]["_sources"][field] = "fmp"
            else:
                # Year exists — fill any fields that are None in current data
                for field, val in fmp_data.items():
                    if val is not None and reconciled[year].get(field) is None:
                        reconciled[year][field] = val
                        reconciled[year]["_sources"][field] = "fmp"

    return dict(sorted(reconciled.items(), reverse=True))


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def _print_summary(reconciled: dict, stats: dict):
    currency = stats.get("currency", "")
    years = list(reconciled.keys())
    if not years:
        print("  No financial data to summarise.")
        return
    print(f"\n  Reconciled dataset: {len(years)} years ({years[-1]}–{years[0]})\n")

    display_years = years[:6]
    col_w = 13
    row_fmt = "{:<24}" + (f"{{:>{col_w}}}" * len(display_years))

    print("  " + row_fmt.format("Metric", *display_years))
    print("  " + "-" * (24 + col_w * len(display_years)))

    metrics = [
        ("Revenue",           "revenue"),
        ("Gross Profit",      "gross_profit"),
        ("Operating Income",  "operating_income"),
        ("EBITDA",            "ebitda"),
        ("Net Income",        "net_income"),
        ("Op. Cash Flow",     "operating_cash_flow"),
        ("CapEx",             "capex"),
        ("Free Cash Flow",    "free_cash_flow"),
        ("Total Debt",        "total_debt"),
        ("Cash & Equiv.",     "cash_and_equivalents"),
        ("EPS (diluted)",     "eps_diluted"),
    ]

    for label, key in metrics:
        vals = []
        for y in display_years:
            v = reconciled.get(y, {}).get(key)
            vals.append(_fmt_short(v) if v is not None else "—")
        print("  " + row_fmt.format(label, *vals))

    print(f"\n  Values in {currency}. Income/CF/BS figures in actual currency units.")

    # Source legend
    sources_used = set()
    for yr_data in reconciled.values():
        sources_used.update(yr_data.get("_sources", {}).values())
    print(f"  Sources used: {', '.join(sorted(sources_used))}")


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt(val) -> str:
    if val is None:
        return "N/A"
    try:
        val = float(val)
    except (TypeError, ValueError):
        return str(val)
    if abs(val) >= 1e12:
        return f"{val/1e12:.2f}T"
    if abs(val) >= 1e9:
        return f"{val/1e9:.2f}B"
    if abs(val) >= 1e6:
        return f"{val/1e6:.2f}M"
    return f"{val:,.0f}"


def _fmt_short(val) -> str:
    if val is None:
        return "—"
    try:
        val = float(val)
    except (TypeError, ValueError):
        return str(val)
    if abs(val) >= 1e12:
        return f"{val/1e12:.1f}T"
    if abs(val) >= 1e9:
        return f"{val/1e9:.1f}B"
    if abs(val) >= 1e6:
        return f"{val/1e6:.0f}M"
    if abs(val) < 100:
        return f"{val:.2f}"
    return f"{val:,.0f}"
