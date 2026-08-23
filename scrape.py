#!/usr/bin/env python3
"""
Bursa Malaysia Weekly Comps Digest - data bridge scraper.

Fetches raw structured data from klsescreener.com (Table 1: full sector
constituent lists; Table 2: per-stock detail for the largest names per
sector) and writes it as JSON to data/latest.json for a Claude Routine
to pick up via a plain HTTPS GET (raw.githubusercontent.com), sidestepping
Claude's own WebFetch tool, which is blocked in unattended/scheduled
sessions.

On any failure, writes data/status.json with an error message instead of
a partial/broken latest.json, so the downstream Routine can detect
failure and send an honest failure email rather than silently reusing
old data or sending broken numbers.
"""

import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SECTOR_URLS = {
    "AI/Technology": "https://www.klsescreener.com/v2/markets/bursa/0005I",
    "Financial Services": "https://www.klsescreener.com/v2/markets/bursa/0010I",
    "Healthcare": "https://www.klsescreener.com/v2/markets/bursa/0062I",
}

# Table 2 stock list: (numeric_code, name-as-in-routine-prompt) - keep this
# in sync with Step 2 of trig_01BCL9YRWLBv52qgD4SCZJvy's prompt.
STOCK_CODES = {
    "AI/Technology": [
        ("0097", "ViTrox Corp"), ("3867", "Malaysian Pacific Industries"),
        ("0128", "Frontken Corp"), ("0166", "Inari Amertron"),
        ("5005", "Unisem Malaysia"), ("5292", "UWC Bhd"),
        ("0208", "Greatech Technology"), ("5357", "SkyeChip Bhd"),
    ],
    "Financial Services": [
        ("5819", "Hong Leong Bank"), ("1295", "Public Bank"),
        ("1155", "Malayan Banking"), ("5258", "BIMB Holdings"),
        ("1023", "CIMB Group"), ("5185", "Affin Bank"),
        ("1066", "RHB Bank"), ("1082", "Hong Leong Financial Grp"),
        ("1015", "AMMB Holdings"), ("2488", "Alliance Bank"),
    ],
    "Healthcare": [
        ("5225", "IHH Healthcare"), ("5555", "Sunway Healthcare"),
        ("5878", "KPJ Healthcare"), ("7113", "Top Glove Corp"),
        ("5168", "Hartalega Holdings"), ("7153", "Kossan Rubber Industries"),
        ("7081", "Pharmaniaga"), ("7148", "Duopharma Biotech"),
    ],
}

STOCK_URL_TMPL = "https://www.klsescreener.com/v2/stocks/view/{code}"

OUT_LATEST = "data/latest.json"
OUT_STATUS = "data/status.json"


def fetch(url, retries=3, timeout=20):
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_err}")


def _clean_num(text):
    """Pull a float out of a messy cell like 'RM 1.23', '12.3%', '1.7B', '-'."""
    if text is None:
        return None
    t = text.strip().replace(",", "")
    if t in ("", "-", "—", "N/A", "n/a", "n/m"):
        return None
    mult = 1
    if t[-1:].upper() == "B":
        mult = 1_000_000_000
        t = t[:-1]
    elif t[-1:].upper() == "M":
        mult = 1_000_000
        t = t[:-1]
    elif t[-1:].upper() == "K":
        mult = 1_000
        t = t[:-1]
    t = t.replace("RM", "").replace("%", "").strip()
    m = re.search(r"-?\d+\.?\d*", t)
    if not m:
        return None
    try:
        return float(m.group()) * mult
    except ValueError:
        return None


def _header_index(headers, *keywords):
    """Find the column index whose header text contains any of the keywords."""
    for i, h in enumerate(headers):
        hl = h.lower()
        if any(k in hl for k in keywords):
            return i
    return None


def parse_sector_table(html, sector_name):
    """Parse the sector-list page into Table 1 rows.

    Strategy: find the <table> with the most rows - that's the constituent
    list, not a nav/footer table. Column positions are located by header
    keyword match rather than hardcoded indices, since the exact column
    order can't be confirmed without live access from this environment.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError(f"No <table> elements found on {sector_name} sector page")

    best_table, best_rows = None, []
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) > len(best_rows):
            best_table, best_rows = table, rows

    if best_table is None or len(best_rows) < 2:
        raise RuntimeError(f"Could not find a data table with rows on {sector_name} page")

    header_cells = [c.get_text(strip=True) for c in best_rows[0].find_all(["th", "td"])]
    idx_code = _header_index(header_cells, "code")
    idx_name = _header_index(header_cells, "name", "company")
    idx_price = _header_index(header_cells, "price", "last")
    idx_pe = _header_index(header_cells, "p/e", "pe")
    idx_dy = _header_index(header_cells, "yield", "dy")
    idx_mcap = _header_index(header_cells, "cap")
    idx_52h = _header_index(header_cells, "52w high", "52 week high", "high")
    idx_52l = _header_index(header_cells, "52w low", "52 week low", "low")

    out = []
    for tr in best_rows[1:]:
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if not cells or len(cells) < 3:
            continue

        def cell(idx):
            return cells[idx] if idx is not None and idx < len(cells) else None

        code_raw = cell(idx_code)
        name_raw = cell(idx_name)
        if not code_raw and not name_raw:
            continue
        code = re.sub(r"\D", "", code_raw or "")
        ticker = None
        link = tr.find("a")
        if link and link.get_text(strip=True):
            maybe_ticker = link.get_text(strip=True)
            if re.match(r"^[A-Z0-9&\-]+$", maybe_ticker):
                ticker = maybe_ticker

        out.append({
            "code": code or None,
            "ticker": ticker,
            "name": name_raw,
            "price": _clean_num(cell(idx_price)),
            "pe": _clean_num(cell(idx_pe)),
            "div_yield": _clean_num(cell(idx_dy)),
            "market_cap": _clean_num(cell(idx_mcap)),
            "week52_high": _clean_num(cell(idx_52h)),
            "week52_low": _clean_num(cell(idx_52l)),
        })

    if len(out) < 5:
        raise RuntimeError(
            f"Only parsed {len(out)} rows from {sector_name} page - "
            f"header detection likely failed. Headers seen: {header_cells}"
        )
    return out


def parse_stock_page(html, code, name):
    """Parse a per-stock detail page into Table 2 fields.

    klsescreener's stock pages are typically label/value pairs rather than
    one big table, so this scans cell pairs for a label match and takes
    the adjacent cell as the value - more robust to layout than fixed
    column positions.
    """
    soup = BeautifulSoup(html, "html.parser")

    labels = {
        "pe": ["p/e ratio", "p/e", "price earnings"],
        "pb": ["p/b ratio", "p/b", "price to book", "price/book"],
        "psr": ["price to sales", "p/s ratio", "p/s"],
        "roe": ["roe", "return on equity"],
        "eps": ["eps", "earnings per share"],
        "dps": ["dps", "dividend per share"],
        "dy": ["dividend yield", "div yield", "yield"],
        "week52_high": ["52 week high", "52w high", "52-week high"],
        "week52_low": ["52 week low", "52w low", "52-week low"],
        "target_price": ["target price", "fair value", "consensus target"],
    }

    text_pairs = []
    for row in soup.find_all(["tr", "div", "li"]):
        cells = row.find_all(["td", "th", "span", "div"], recursive=False)
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)
            if label and value:
                text_pairs.append((label.lower(), value))

    def find_value(keys):
        for label, value in text_pairs:
            if any(k in label for k in keys):
                return value
        return None

    price_val = find_value(["price", "last price"])
    result = {
        "code": code,
        "name": name,
        "price": _clean_num(price_val),
    }
    for field, keys in labels.items():
        result[field] = _clean_num(find_value(keys))

    source = None
    full_text = soup.get_text(" ", strip=True).lower()
    for bank in ["kenanga", "rhb", "maybank", "cimb"]:
        if bank in full_text and result.get("target_price") is not None:
            source = bank.title()
            break
    result["target_price_source"] = source

    return result


def main():
    os.makedirs("data", exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    table1 = {}
    table2 = {}
    errors = []

    for sector, url in SECTOR_URLS.items():
        try:
            html = fetch(url)
            table1[sector] = parse_sector_table(html, sector)
        except Exception as e:
            errors.append(f"Table 1 / {sector}: {e}")
            table1[sector] = []

    debug_dumped = False
    for sector, stocks in STOCK_CODES.items():
        rows = []
        for code, name in stocks:
            try:
                html = fetch(STOCK_URL_TMPL.format(code=code))
                if not debug_dumped:
                    # Save one raw sample page for calibrating the parser -
                    # this sandbox has no other way to see the real markup.
                    with open("data/debug_stock_page.html", "w") as f:
                        f.write(html)
                    debug_dumped = True
                rows.append(parse_stock_page(html, code, name))
            except Exception as e:
                errors.append(f"Table 2 / {sector} / {code} {name}: {e}")
            time.sleep(0.5)  # be polite to the source site
        table2[sector] = rows

    # Require real data in every sector for Table 1 to call this a success -
    # a totally empty sector means the parser broke on that page, not that
    # the sector genuinely has under 5 constituents.
    table1_ok = all(len(table1.get(s, [])) >= 5 for s in SECTOR_URLS)
    table2_ok = all(len(table2.get(s, [])) >= 1 for s in STOCK_CODES)
    # A row with a code/name but no price/pe means the per-stock parser
    # matched nothing on that page - don't call that a success even though
    # a "row" technically exists (caught a real bug on the first live run).
    table2_fields_ok = all(
        r.get("price") is not None or r.get("pe") is not None
        for rows in table2.values() for r in rows
    ) if table2_ok else False

    if table1_ok and table2_ok and table2_fields_ok and not errors:
        with open(OUT_LATEST, "w") as f:
            json.dump({
                "generated_at": generated_at,
                "table1": table1,
                "table2": table2,
            }, f, indent=2)
        with open(OUT_STATUS, "w") as f:
            json.dump({"generated_at": generated_at, "ok": True, "errors": []}, f, indent=2)
        print(f"OK - wrote {OUT_LATEST}")
    else:
        if table2_ok and not table2_fields_ok:
            errors.append(
                "Table 2 rows exist but every field is null - the per-stock "
                "page parser matched nothing. See data/debug_stock_page.html "
                "for a raw sample to recalibrate against."
            )
        with open(OUT_STATUS, "w") as f:
            json.dump({
                "generated_at": generated_at,
                "ok": False,
                "errors": errors,
                "table1_counts": {s: len(v) for s, v in table1.items()},
                "table2_counts": {s: len(v) for s, v in table2.items()},
            }, f, indent=2)
        print("FAILED - see data/status.json")
        print(json.dumps(errors, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        os.makedirs("data", exist_ok=True)
        with open(OUT_STATUS, "w") as f:
            json.dump({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "ok": False,
                "errors": [traceback.format_exc()],
            }, f, indent=2)
        raise
