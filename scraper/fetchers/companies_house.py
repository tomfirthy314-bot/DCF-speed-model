"""
Companies House fetcher  —  UK statutory registry

What this provides:
  - Company number (definitive identifier)
  - SIC code(s) — statutory sector classification
  - Incorporation date
  - Registered name (confirms we have the right entity)
  - Last accounts made up to (date — staleness check for scraped financials)
  - Last annual return / confirmation statement date
  - Company status (active / dissolved / etc.)

What it does NOT currently provide:
  - Actual financial figures (revenue, EBIT, etc.)
  Financial data lives inside iXBRL/XHTML filing documents attached to
  accounts submissions.  Extracting structured numbers from those requires
  XBRL namespace parsing which is out of scope for this sprint.
  Planned for a future sprint using the `xbrl` Python library.

How it is used in the pipeline:
  - Confirms the target company is the one we think we're modelling
  - SIC code cross-checks the classifier's sector assignment
  - Last accounts date surfaces data staleness (e.g. Yahoo showing 2025
    data that doesn't match a CH filing date of March 2025)
  - For UK companies, the registered name is the authoritative legal name

Configuration:
  Set COMPANIES_HOUSE_API_KEY environment variable.
  Free registration at developer.companieshouse.gov.uk.
  If no key: function degrades gracefully and returns available=False.

API authentication:
  Companies House uses HTTP Basic Auth with the API key as the username
  and an empty password.

Only fires for UK-registered companies (detected via ticker .L suffix or
country field in stats).
"""

from __future__ import annotations

import os
import re
import requests
from typing import Any

_BASE    = "https://api.company-information.service.gov.uk"
_TIMEOUT = 10


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def fetch_companies_house(
    ticker: str,
    company_name: str,
    country: str = "",
    key: str | None = None,
) -> dict:
    """
    Fetch company profile from Companies House.

    Returns:
      {
        "available":           bool,
        "reason":              str | None,
        "company_number":      str,
        "registered_name":     str,
        "status":              str,      e.g. "active"
        "incorporation_date":  str,      e.g. "1884-03-15"
        "sic_codes":           [str],    e.g. ["30110"]
        "sic_descriptions":    [str],    e.g. ["Building of ships and floating structures"]
        "last_accounts_date":  str,      e.g. "2024-12-31"
        "accounts_type":       str,      e.g. "full"
        "last_filing_date":    str,      date of most recent any-type filing
        "registered_address":  str,
        "notes":               [str],    human-readable observations
      }
    """
    key = key or os.environ.get("COMPANIES_HOUSE_API_KEY")
    if not key:
        return _unavailable("No COMPANIES_HOUSE_API_KEY set — skipping CH lookup.")

    if not _is_uk(ticker, country):
        return _unavailable(
            f"Companies House is a UK registry. "
            f"Ticker '{ticker}' / country '{country}' does not appear to be UK-listed."
        )

    # ── Step 1: search for company number ─────────────────────────────────────
    search_term = _clean_search_term(company_name)
    company_number, registered_name = _search_company(search_term, key)

    if not company_number:
        # Fallback: try with shorter name (drop "plc", "limited", etc.)
        short = _strip_legal_suffix(search_term)
        if short != search_term:
            company_number, registered_name = _search_company(short, key)

    if not company_number:
        return _unavailable(
            f"Could not find '{company_name}' on Companies House. "
            "Search returned no confident match."
        )

    # ── Step 2: fetch company profile ─────────────────────────────────────────
    profile = _get_profile(company_number, key)
    if not profile:
        return _unavailable(
            f"Found company number {company_number} but profile request failed."
        )

    # ── Step 3: extract fields ────────────────────────────────────────────────
    sic_codes  = profile.get("sic_codes", [])
    sic_descs  = _sic_descriptions(sic_codes)
    status     = profile.get("company_status", "unknown")
    inc_date   = (profile.get("date_of_creation") or "")

    acc_info   = profile.get("accounts", {}) or {}
    last_acc   = (acc_info.get("last_accounts") or {})
    acc_date   = last_acc.get("made_up_to", "")
    acc_type   = last_acc.get("type", "")

    address_obj = profile.get("registered_office_address") or {}
    address     = _format_address(address_obj)

    # ── Step 4: most recent filing date ───────────────────────────────────────
    last_filing = _get_last_filing_date(company_number, key)

    # ── Step 5: derive notes ──────────────────────────────────────────────────
    notes = _build_notes(status, acc_date, acc_type, sic_codes, last_filing)

    return {
        "available":           True,
        "reason":              None,
        "company_number":      company_number,
        "registered_name":     registered_name,
        "status":              status,
        "incorporation_date":  inc_date,
        "sic_codes":           sic_codes,
        "sic_descriptions":    sic_descs,
        "last_accounts_date":  acc_date,
        "accounts_type":       acc_type,
        "last_filing_date":    last_filing,
        "registered_address":  address,
        "notes":               notes,
    }


# ─────────────────────────────────────────────────────────────────────────────
# API calls
# ─────────────────────────────────────────────────────────────────────────────

def _search_company(query: str, key: str) -> tuple[str | None, str | None]:
    """Search CH and return (company_number, registered_name) for best match."""
    url    = f"{_BASE}/search/companies"
    params = {"q": query, "items_per_page": 5}
    data   = _get(url, params, key)

    if not data or "items" not in data:
        return None, None

    items = data["items"]
    if not items:
        return None, None

    # Prefer "active" companies; take the first active one
    for item in items:
        if item.get("company_status", "").lower() == "active":
            return item["company_number"], item.get("title", "")

    # No active match — return first result
    first = items[0]
    return first["company_number"], first.get("title", "")


def _get_profile(company_number: str, key: str) -> dict | None:
    url  = f"{_BASE}/company/{company_number}"
    data = _get(url, {}, key)
    return data


def _get_last_filing_date(company_number: str, key: str) -> str:
    """Return the date of the most recent filing of any type."""
    url    = f"{_BASE}/company/{company_number}/filing-history"
    params = {"items_per_page": 1, "category": "accounts"}
    data   = _get(url, params, key)

    if data and data.get("items"):
        return data["items"][0].get("date", "")
    return ""


def _get(url: str, params: dict, key: str) -> dict | None:
    """Authenticated GET to Companies House API."""
    try:
        resp = requests.get(
            url, params=params, auth=(key, ""), timeout=_TIMEOUT
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_UK_TICKERS = re.compile(r"\.L$", re.IGNORECASE)

def _is_uk(ticker: str, country: str) -> bool:
    if _UK_TICKERS.search(ticker):
        return True
    if country and "united kingdom" in country.lower():
        return True
    return False


_LEGAL_SUFFIXES = re.compile(
    r"\s+(plc|limited|ltd|group|holdings|inc|corp|llc|llp|lp)\.?\s*$",
    re.IGNORECASE,
)

def _clean_search_term(name: str) -> str:
    return name.strip()

def _strip_legal_suffix(name: str) -> str:
    return _LEGAL_SUFFIXES.sub("", name).strip()


# SIC code → plain English (subset covering the most common sectors)
_SIC_LOOKUP: dict[str, str] = {
    "26110": "Manufacture of electronic components",
    "26200": "Manufacture of computers and peripheral equipment",
    "27110": "Manufacture of electric motors, generators and transformers",
    "28110": "Manufacture of engines and turbines",
    "28120": "Manufacture of fluid power equipment",
    "29100": "Manufacture of motor vehicles",
    "30110": "Building of ships",
    "30300": "Manufacture of air and spacecraft",   # Rolls-Royce
    "35110": "Production of electricity",
    "41100": "Development of building projects",
    "46190": "Agents in sale of variety of goods",
    "47110": "Retail — non-specialised stores",
    "47730": "Dispensing chemist",
    "49100": "Passenger rail transport",
    "58290": "Other software publishing",
    "62010": "Computer programming activities",
    "62020": "Computer consultancy activities",
    "64110": "Central banking",
    "64191": "Banks",
    "64192": "Building societies",
    "64205": "Activities of financial holding companies",
    "64209": "Activities of other holding companies",
    "65110": "Life insurance",
    "65120": "Non-life insurance",
    "66110": "Administration of financial markets",
    "66120": "Security and commodity contracts dealing",
    "68100": "Buying and selling of own real estate",
    "68209": "Other letting and operating of own/leased real estate",
    "70100": "Activities of head offices",
    "71121": "Engineering design",
    "72190": "Other research and experimental development",
    "74909": "Other professional, scientific and technical activities",
    "82990": "Other business support service activities",
    "86210": "General medical practice activities",
    "86900": "Other human health activities",
}

def _sic_descriptions(codes: list[str]) -> list[str]:
    return [_SIC_LOOKUP.get(str(c), f"SIC {c}") for c in codes]


def _format_address(addr: dict) -> str:
    parts = [
        addr.get("address_line_1", ""),
        addr.get("address_line_2", ""),
        addr.get("locality", ""),
        addr.get("postal_code", ""),
        addr.get("country", ""),
    ]
    return ", ".join(p for p in parts if p)


def _build_notes(
    status: str,
    acc_date: str,
    acc_type: str,
    sic_codes: list,
    last_filing: str,
) -> list[str]:
    notes = []

    if status and status.lower() != "active":
        notes.append(f"WARNING: Company status is '{status}' — not active.")

    if acc_date:
        from datetime import date, datetime
        try:
            acc_dt = datetime.strptime(acc_date, "%Y-%m-%d").date()
            age_days = (date.today() - acc_dt).days
            if age_days > 548:   # > 18 months
                notes.append(
                    f"Last accounts filed to {acc_date} — "
                    f"{age_days // 365}yr {(age_days % 365) // 30}mo ago. "
                    "Scraped financials may post-date the statutory filing."
                )
            else:
                notes.append(
                    f"Last {acc_type or 'annual'} accounts made up to {acc_date}."
                )
        except ValueError:
            notes.append(f"Last accounts date: {acc_date}.")
    else:
        notes.append("No accounts date found on Companies House.")

    if last_filing:
        notes.append(f"Most recent CH filing: {last_filing}.")

    if not sic_codes:
        notes.append("No SIC codes registered — sector classification cannot be verified.")

    return notes


def _unavailable(reason: str) -> dict:
    return {
        "available":           False,
        "reason":              reason,
        "company_number":      None,
        "registered_name":     None,
        "status":              None,
        "incorporation_date":  None,
        "sic_codes":           [],
        "sic_descriptions":    [],
        "last_accounts_date":  None,
        "accounts_type":       None,
        "last_filing_date":    None,
        "registered_address":  None,
        "notes":               [],
    }
