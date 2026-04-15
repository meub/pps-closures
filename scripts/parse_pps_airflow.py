"""Parse PPS airflow PDFs into per-school ACH stats.

Input:  data/raw/pps_airflow_pdfs/{slug}.pdf (from fetch_pps_airflow.py)
Output: data/raw/pps_airflow_stats.json  — per-school aggregates

Metrics per school:
- rooms_tested
- ach_supply_median       (col 11: air changes/hour, supply air)
- ach_oa_median           (col 12: air changes/hour, outside air only)
- ach_e_hvac_only_median  (col 15: ASHRAE Total Effective ACH w/o portable filter)
- pct_rooms_below_3_ach_e (Lancet lower bound)
- pct_rooms_below_6_ach_e (Lancet upper bound)
- filter_status_upgraded  (MERV-13 upgrade in place at test time; None if ambiguous)

The airflow table has a consistent 17-column layout across all reports
(same contractor, same template):
  0 Room | 1 Served By | 2 Equipment Type |
  3 Length | 4 Width | 5 Area | 6 Height | 7 Volume |
  8 Total CFM Supply | 9 OA CFM Supply | 10 OA % |
  11 ACH (supply) | 12 ACH (OA) | 13 # Portable Filters |
  14 ACH_e with Portable | 15 ACH_e without Portable | 16 Notes
"""
import json
import re
import statistics as st
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = ROOT / "data/raw/pps_airflow_pdfs"
INDEX = ROOT / "data/raw/pps_airflow_index.json"
OUT = ROOT / "data/raw/pps_airflow_stats.json"


def numf(v):
    if v is None:
        return None
    s = str(v).replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def parse_rows(pdf):
    """Extract data rows from the airflow table.

    Room labels vary widely (e.g. 'A103', 'Conf. Rm A100', 'C110', 'Cafeteria
    B100'), so we identify data rows by checking that column 11 (ACH supply)
    parses as a number. Section headers like 'First Floor' / 'Second Floor'
    sit in column 0 with empty numeric columns and are filtered out.
    """
    rows = []
    for page in pdf.pages:
        for table in page.extract_tables() or []:
            if not table or not table[0]:
                continue
            header = " ".join(c or "" for c in table[0])
            if "Room" not in header or "Equipment" not in header:
                continue
            for r in table[2:]:
                cells = [c or "" for c in r]
                if len(cells) < 16:
                    continue
                if numf(cells[11]) is None:
                    continue
                rows.append(cells)
    return rows


FILTER_UPGRADED_RE = re.compile(r"filter\s*status\s*:?\s*(not\s+)?upgraded", re.IGNORECASE)


def parse_filter_status(pdf):
    """Returns True if MERV-13 upgrade was in place, False if not, None if unknown."""
    for page in pdf.pages[:12]:  # header info appears early
        try:
            text = page.extract_text() or ""
        except Exception:
            continue
        m = FILTER_UPGRADED_RE.search(text)
        if m:
            return m.group(1) is None  # "not upgraded" → False; bare "upgraded" → True
    return None


def summarize(rows):
    def col(i):
        return [numf(r[i]) for r in rows if len(r) > i]

    def clean(arr):
        return [x for x in arr if x is not None]

    supply = clean(col(11))
    oa = clean(col(12))
    e_hvac = clean(col(15))

    def pct_below(arr, t):
        if not arr:
            return None
        return round(sum(1 for x in arr if x < t) / len(arr), 4)

    def med(arr):
        return round(st.median(arr), 2) if arr else None

    return {
        "rooms_tested": len(rows),
        "ach_supply_median": med(supply),
        "ach_oa_median": med(oa),
        "ach_e_hvac_only_median": med(e_hvac),
        "pct_rooms_below_3_ach_e": pct_below(e_hvac, 3),
        "pct_rooms_below_6_ach_e": pct_below(e_hvac, 6),
    }


def main():
    idx = json.loads(INDEX.read_text())
    out = {}
    for i, entry in enumerate(idx, 1):
        slug = entry["slug"]
        pdf_path = PDF_DIR / f"{slug}.pdf"
        if not pdf_path.exists() or pdf_path.stat().st_size < 10_000:
            print(f"[{i}/{len(idx)}] {slug}: missing PDF, skipping")
            continue
        try:
            with pdfplumber.open(pdf_path) as pdf:
                rows = parse_rows(pdf)
                if not rows:
                    print(f"[{i}/{len(idx)}] {slug}: NO DATA ROWS")
                    continue
                stats = summarize(rows)
                stats["filter_status_upgraded"] = parse_filter_status(pdf)
        except Exception as e:
            print(f"[{i}/{len(idx)}] {slug}: ERROR {e}")
            continue
        stats["school_name"] = entry["school_name"]
        out[slug] = stats
        print(f"[{i}/{len(idx)}] {entry['school_name']}: "
              f"{stats['rooms_tested']} rooms, "
              f"ACH_e median={stats['ach_e_hvac_only_median']}, "
              f"below 3={stats['pct_rooms_below_3_ach_e']:.0%}"
              if stats['pct_rooms_below_3_ach_e'] is not None
              else f"[{i}/{len(idx)}] {entry['school_name']}: n/a")

    OUT.write_text(json.dumps(out, indent=2))
    print(f"\nWrote {OUT} ({len(out)} schools)")


if __name__ == "__main__":
    main()
