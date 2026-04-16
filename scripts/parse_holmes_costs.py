#!/usr/bin/env python3
"""Parse per-school ROM retrofit cost estimates from the Holmes 2024
Portland Public Schools Seismic Assessment PDF.

Extracts two dollar values from each school's cover page:
  - URM-ONLY ROM retrofit cost (subset retrofit of unreinforced-masonry areas)
  - COMPLETE ROM retrofit cost (total remaining exposure for the whole campus)

Both may be "None" when Holmes excludes the school (recent modernization or
in-design). "None" is written as 0 in the output so downstream sorts treat it
as "no remaining cost."

Input:  data/raw/holmes_2024_seismic.pdf (365-page combined report)
Output: data/raw/pps_holmes_2024_costs.json
  {
    "Abernethy": {"urm_only_cost_usd": 450000, "complete_cost_usd": 12367500,
                  "urm_on_database": false, "page": 4, "notes": null},
    ...
  }
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parent.parent
PDF = ROOT / "data/raw/holmes_2024_seismic.pdf"
OUT = ROOT / "data/raw/pps_holmes_2024_costs.json"


def parse_dollars(v: str | None) -> int | None:
    if v is None:
        return None
    if "None" in v:
        return 0
    m = re.search(r"\$([0-9,]+)", v)
    return int(m.group(1).replace(",", "")) if m else None


def extract_cover(text: str) -> dict | None:
    """Extract (name, urm_only, complete, urm_on_db, notes) from a cover page."""
    lines = text.split("\n")
    name = None
    for i, ln in enumerate(lines):
        if "Select school from" in ln and i + 1 < len(lines):
            name = lines[i + 1].strip()
            break
    if not name:
        return None

    urm_val = complete_val = None
    for i, ln in enumerate(lines):
        if "URM-ONLY ROM cost" in ln and i > 0:
            urm_val = lines[i - 1].strip()
        if "COMPLETE ROM cost" in ln and i > 0:
            complete_val = lines[i - 1].strip()

    # "URM Database : YES" / "URM Database : NO" — but pdfplumber sometimes
    # interleaves text so use a looser regex.
    urm_on_db = None
    if re.search(r"URM\s*Database\s*:\s*Y", text):
        urm_on_db = True
    elif re.search(r"URM\s*Database\s*:\s*N", text):
        urm_on_db = False

    notes = None
    m = re.search(r"(Retrofit and Rebuild Completed[^\n]*)", text)
    if m:
        notes = m.group(1).strip()

    return {
        "name": name,
        "urm_only_cost_usd": parse_dollars(urm_val),
        "complete_cost_usd": parse_dollars(complete_val),
        "urm_on_database": urm_on_db,
        "notes": notes,
    }


def main() -> None:
    if not PDF.exists():
        raise SystemExit(f"missing {PDF} — copy the Holmes 2024 PDF there first")

    records: dict[str, dict] = {}
    with pdfplumber.open(PDF) as pdf:
        for i in range(len(pdf.pages)):
            t = pdf.pages[i].extract_text() or ""
            if "Select school from" not in t or "URM-ONLY ROM cost" not in t:
                continue
            rec = extract_cover(t)
            if not rec:
                continue
            rec["page"] = i + 1
            name = rec.pop("name")
            # Some campuses appear twice (e.g. "Beverly Cleary" has Fernwood +
            # Hollyrood entries). Keep them both under their actual Holmes names.
            if name in records:
                # de-duplicate by page number
                name = f"{name} (p{rec['page']})"
            records[name] = rec

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, sort_keys=True)

    n = len(records)
    uncommitted = sum(1 for r in records.values() if r["complete_cost_usd"])
    done = sum(1 for r in records.values() if r["complete_cost_usd"] == 0)
    total = sum(r["complete_cost_usd"] or 0 for r in records.values())
    print(f"wrote {OUT} ({n} schools)")
    print(f"  complete-retrofit cost > 0: {uncommitted} schools")
    print(f"  complete-retrofit cost = 0 (done/excluded): {done} schools")
    print(f"  portfolio total complete-retrofit ROM: ${total:,}")


if __name__ == "__main__":
    main()
