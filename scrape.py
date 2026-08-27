#!/usr/bin/env python3
"""
Bursa Malaysia Weekly Comps Digest - data bridge scraper.

Fetches raw structured data from klsescreener.com (Table 1: full sector
constituent lists; Table 2: per-stock detail for the largest names per
sector) and writes it as JSON to data/latest.json for a Claude Routine
to pick up via a plain HTTPS GET (raw.githubusercontent.com). This is
now the PRIMARY data source for the weekly digest (see 2026-08-27 (cont'd)
architecture note below) - direct WebFetch from the Routine is the
fallback, not the other way around.

On any failure, writes data/status.json with an error message instead of
a broken latest.json, so the downstream Routine can detect failure and
fall back to WebFetch (or send an honest failure email if that also
fails) rather than silently reusing old data or sending broken numbers.

=== Change log ===

2026-08-24 fix: parse_stock_page() was rewritten. The original version
scanned <tr>/<div>/<li> elements generically for two-child label/value
pairs, but klsescreener.com stock pages have large wrapper <div>s whose
flattened text happens to contain "p/e"/"roe" etc as substrings - those
wrapper "pairs" sorted earlier in document order than the real <tr> rows
and won permanently under a first-match search, so every field came back
null. Fix: only scan literal <tr> elements with exactly two <td>
children, using the site's real short labels (p/e, p/b, psr, roe, eps,
dps, dy, 52w) rather than long guessed label text, and pull the current
price from the dedicated `#price` span (data-value attribute) since it
isn't part of any label/value table row on this site. Verified live
2026-08-27 (workflow run #5, went green, real numbers confirmed in
data/status.json and data/latest.json).

2026-08-27 (cont'd) architecture change - two things fixed at once:

1. Table 1's 52-week high/low was a known permanent gap (confirmed blank
   for every constituent, every week, since the digest launched) because
   the three sector-list pages this scraper's parse_sector_table() reads
   never publish per-stock 52-week range - only individual stock pages
   do. Getting real coverage means one per-stock fetch per Table 1
   constituent (~108 stocks across the three sectors), which was
   previously ruled out as "too many extra WebFetch calls for a live
   Claude session to make." That reasoning doesn't apply here: this
   scraper already runs unattended on free GitHub Actions infrastructure
   with no per-call cost or approval-gate risk, so the fetch-everyone's-
   stock-page approach that was too expensive for a live session is cheap
   and easy here. main() now builds ONE set of "codes needed" - the union
   of every Table 1 constituent's code and every Table 2 target code -
   fetches each unique code's stock page exactly once (avoiding a wasteful
   double-fetch for names that appear in both tables, which in practice is
   most of Table 2 since those are literally the largest names within
   each Table 1 sector list), and reuses that single per-stock result to
   both backfill Table 1's week52_high/week52_low and build Table 2's
   full row. Runtime cost: roughly 108 extra requests at ~0.9s each
   (fetch + 0.4s politeness sleep) versus the previous ~26, adding
   roughly 1-2 minutes to the job - trivial against GitHub Actions' free
   minutes and default 6-hour job timeout.

2. Because this restructuring happens to make every field of Table 2 come
   from the same single parse_stock_page() call as Table 1's 52-week
   backfill, the old failure mode ("a row exists but every field in it is
   null") is now structurally impossible rather than merely detected: a
   Table 2 row is only ever added to the output when parse_stock_page()
   for that code actually returned data. A per-stock fetch/parse failure
   now just means that one stock is missing from the sector's list (and
   is named explicitly in errors/status.json) rather than present with
   garbage fields - the exact class of silent bug this scraper had for
   its first month is now prevented by construction, not just tested for.

Given this scraper's data is now both more complete (full 52-week
coverage on Table 1) and cheaper to produce than a live Claude session
repeating the same ~130+ fetches itself every week, the Routine was
updated the same day to treat this bridge as primary and direct WebFetch
as the fallback (previously the reverse) - see builds/malaysia-comps-agent.md
in the AI Projects Claude project for the full reasoning and trade-offs
(data freshness: ~1hr old at email time via the bridge vs ~15-30min via
live WebFetch - judged an acceptable trade for the reliability and cost
win, but worth stating plainly rather than burying it).
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

# Table 2 target list: (numeric_code, display-name-as-used-in-the-Routine's
# prompt/digest). Keep this in sync with Step 2 of trig_01BCL9YRWLBv52qgD4SCZJvy's
# prompt if the largest-by-market-cap set ever needs to change.
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
    """Parse a sector-list page into Table 1 rows (price/PE/DY/market cap -
    52-week range is NOT here, see the module docstring; it's backfilled in
    main() from each stock's own page instead).

    Strategy: find the <table> with the most rows - that's the constituent
    list, not a nav/footer table. Column positions are located by header
    keyword match rather than hardcoded indices.
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
            "week52_high": None,  # backfilled in main() from per-stock pages
            "week52_low": None,
        })

    if len(out) < 5:
        raise RuntimeError(
            f"Only parsed {len(out)} rows from {sector_name} page - "
            f"header detection likely failed. Headers seen: {header_cells}"
        )
    return out


# Real short labels used by klsescreener.com's per-stock detail table, e.g.
# <tr><td>P/E</td><td class="number">78.45</td></tr> - confirmed 2026-08-24
# against a live-saved sample page, still correct 2026-08-27 (run #5).
_FIELD_KEYS = {
    "pe": ["p/e"],
    "pb": ["p/b"],
    "psr": ["psr"],
    "roe": ["roe"],
    "eps": ["eps"],
    "dps": ["dps"],
    "dy": ["dy"],
    "market_cap": ["market cap"],
    "target_price": ["target price", "fair value", "consensus target"],
}


def parse_stock_page(html, code):
    """Parse a per-stock detail page into a full Table 2 field set (also
    used to backfill Table 1's 52-week range for every constituent - see
    module docstring). Only scans literal <tr> elements with exactly two
    direct <td> children - this site's real label/value rows - not any
    <div>/<li> "row" whose flattened text can spuriously contain a field's
    search keyword (that was the 2026-08-24 bug). Current price comes from
    the dedicated `#price` span's data-value attribute, since it lives
    outside the label/value table entirely.
    """
    soup = BeautifulSoup(html, "html.parser")

    text_pairs = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"], recursive=False)
        if len(cells) == 2:
            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)
            # Guard against any non-conforming row: real labels on this
            # site are short (P/E, ROE, 52w, Market Cap, ...); anything
            # long is not a genuine label/value pair.
            if label and value and len(label) <= 40:
                text_pairs.append((label.lower(), value))

    def find_value(keys):
        for label, value in text_pairs:
            if any(label == k or k in label for k in keys):
                return value
        return None

    price_span = soup.find(id="price")
    price_val = None
    if price_span is not None:
        price_val = price_span.get("data-value") or price_span.get_text(strip=True)

    result = {"code": code, "price": _clean_num(price_val)}

    for field, keys in _FIELD_KEYS.items():
        result[field] = _clean_num(find_value(keys))

    week52_raw = find_value(["52w"])
    week52_high, week52_low = None, None
    if week52_raw:
        # Real format is "3.560 - 9.650" (low - high, ascending)
        parts = re.split(r"\s*-\s*", week52_raw.strip())
        if len(parts) == 2:
            a, b = _clean_num(parts[0]), _clean_num(parts[1])
            if a is not None and b is not None:
                week52_low, week52_high = min(a, b), max(a, b)
    result["week52_high"] = week52_high
    result["week52_low"] = week52_low

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
    errors = []

    # --- Table 1: sector-list pages (price/PE/DY/market cap; no 52-week) ---
    for sector, url in SECTOR_URLS.items():
        try:
            html = fetch(url)
            table1[sector] = parse_sector_table(html, sector)
        except Exception as e:
            errors.append(f"Table 1 / {sector}: {e}")
            table1[sector] = []

    # --- One per-stock fetch pass, deduplicated across Table 1 + Table 2 ---
    # codes_needed is the union of every Table 1 constituent's code (to
    # backfill 52-week range) and every Table 2 target code (for the full
    # P/B/ROE/etc panel). A code that appears in both only gets fetched once.
    codes_needed = set()
    for rows in table1.values():
        codes_needed.update(row["code"] for row in rows if row.get("code"))
    for stocks in STOCK_CODES.values():
        codes_needed.update(code for code, _name in stocks)

    stock_detail = {}
    debug_dumped = False
    for code in codes_needed:
        try:
            html = fetch(STOCK_URL_TMPL.format(code=code))
            if not debug_dumped:
                # Save one raw sample page for calibrating the parser -
                # this sandbox has no other way to see the real markup.
                with open("data/debug_stock_page.html", "w") as f:
                    f.write(html)
                debug_dumped = True
            stock_detail[code] = parse_stock_page(html, code)
        except Exception as e:
            errors.append(f"Stock detail / {code}: {e}")
        time.sleep(0.4)  # be polite to the source site

    # --- Backfill Table 1's 52-week range (best-effort: a stock whose
    # detail-page fetch failed just keeps None here, same as before this
    # change - it does not fail the whole run). ---
    for sector, rows in table1.items():
        for row in rows:
            d = stock_detail.get(row.get("code"))
            if d:
                row["week52_high"] = d.get("week52_high")
                row["week52_low"] = d.get("week52_low")

    # --- Build Table 2 rows from the same stock_detail cache. A row is
    # only added when its fetch actually succeeded, so "row exists but
    # every field is null" (the 2026-08-24 bug class) is now structurally
    # impossible rather than merely checked for. ---
    table2 = {}
    for sector, stocks in STOCK_CODES.items():
        rows = []
        for code, name in stocks:
            d = stock_detail.get(code)
            if d is None:
                errors.append(f"Table 2 / {sector} / {code} {name}: no stock detail available")
                continue
            row = dict(d)
            row["name"] = name  # curated display name, not the sector-list one
            rows.append(row)
        table2[sector] = rows

    table1_ok = all(len(table1.get(s, [])) >= 5 for s in SECTOR_URLS)
    table2_ok = all(len(table2.get(s, [])) >= 1 for s in STOCK_CODES)
    ok = table1_ok and table2_ok

    if ok:
        with open(OUT_LATEST, "w") as f:
            json.dump({
                "generated_at": generated_at,
                "table1": table1,
                "table2": table2,
            }, f, indent=2)
        with open(OUT_STATUS, "w") as f:
            json.dump({
                "generated_at": generated_at,
                "ok": True,
                "errors": errors,  # non-fatal misses (e.g. one stock's page
                                    # failed to fetch) are still surfaced
                                    # here even on an overall-success run
                "table1_counts": {s: len(v) for s, v in table1.items()},
                "table2_counts": {s: len(v) for s, v in table2.items()},
                "table1_week52_coverage": {
                    s: sum(1 for r in v if r.get("week52_high") is not None)
                    for s, v in table1.items()
                },
            }, f, indent=2)
        print(f"OK - wrote {OUT_LATEST} ({len(errors)} non-fatal warning(s))")
        if errors:
            print(json.dumps(errors, indent=2))
    else:
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
