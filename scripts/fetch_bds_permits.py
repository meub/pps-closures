#!/usr/bin/env python3
"""Fetch Portland residential building permits (2022-present) from the
City of Portland BDS open-data feature layer on PortlandMaps.

Endpoint:
  https://www.portlandmaps.com/od/rest/services/COP_OpenData_PlanningDevelopment/MapServer/89

Dataset description (from the service metadata):
  "All City of Portland permits that have created one or more new residential
   units since 1995. Derived from the Bureau of Development Services
   permitting database (TRACS). Geocoded to taxlot centroids or street address
   when taxlot information is not accurate or available."

This means the layer is ALREADY filtered to permits that add one or more
residential units -- new SFDs, duplexes, townhouses, multifamily, and major
alterations that add units, plus ADUs (flagged in IS_ADU). No further "exclude
commercial/sign/electrical" filtering is required.

Output:
  /Users/meuba/Code/school-research/data/raw/portland_bds_permits.csv
    columns: permit_id, issue_date, address, latitude, longitude, units,
             work_description, valuation
  /Users/meuba/Code/school-research/data/raw/bds_permits_raw.json
    raw ArcGIS JSON response pages (concatenated) for audit.
"""
from __future__ import annotations

import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SERVICE_URL = (
    "https://www.portlandmaps.com/od/rest/services/"
    "COP_OpenData_PlanningDevelopment/MapServer/89/query"
)
OUT_CSV = Path("/Users/meuba/Code/school-research/data/raw/portland_bds_permits.csv")
OUT_RAW = Path("/Users/meuba/Code/school-research/data/raw/bds_permits_raw.json")

UA = {"User-Agent": "Mozilla/5.0 (research; pps-closure-analysis)"}

# ArcGIS date literal; server interprets this in its local tz but we only
# care about day-level truncation for "since 2022".
WHERE = "ISSUEDATE >= DATE '2022-01-01'"

OUT_FIELDS = [
    "FOLDERNUMB",  # permit id (e.g. "18-129792-000-00-RS")
    "REV",         # secondary id; often null
    "ISSUEDATE",   # epoch ms
    "PROP_ADDRE",  # address
    "WORKDESC",    # "New Construction", "Alteration", etc.
    "NEW_UNITS",   # count of new residential units added
    "VALUATION",   # dollars
    "NEWCLASS",    # e.g. "New Construction", "Alteration"
    "NEWTYPE",     # e.g. "Single Family Dwelling", "Townhouse", "Apartment"
    "IS_ADU",      # "True"/"False"
    "STATUS",
]

PAGE_SIZE = 200  # server maxRecordCount is 200
SLEEP_BETWEEN_PAGES = 0.5


def http_get(url: str, timeout: int = 60) -> bytes:
    with urlopen(Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def count_records() -> int:
    url = SERVICE_URL + "?" + urlencode({
        "where": WHERE,
        "returnCountOnly": "true",
        "f": "json",
    })
    data = json.loads(http_get(url))
    return int(data.get("count", 0))


def fetch_page(offset: int) -> dict:
    url = SERVICE_URL + "?" + urlencode({
        "where": WHERE,
        "outFields": ",".join(OUT_FIELDS),
        "outSR": "4326",
        "orderByFields": "ISSUEDATE ASC",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "returnGeometry": "true",
        "f": "json",
    })
    return json.loads(http_get(url))


def epoch_ms_to_iso(ms) -> str | None:
    if ms is None:
        return None
    try:
        return (
            datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
            .date()
            .isoformat()
        )
    except Exception:
        return None


def to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def main() -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    total = count_records()
    print(f"[count] {total} residential permits with ISSUEDATE >= 2022-01-01")

    pages: list[dict] = []
    features: list[dict] = []
    offset = 0
    while True:
        page = fetch_page(offset)
        feats = page.get("features") or []
        if not feats:
            # server may return empty once we're past the end
            pages.append(page)
            break
        pages.append(page)
        features.extend(feats)
        print(f"[page] offset={offset} got={len(feats)} cumulative={len(features)}")
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_PAGES)

    # Save raw audit dump (list of page responses).
    with OUT_RAW.open("w", encoding="utf-8") as f:
        json.dump({"query": WHERE, "pages": pages}, f)
    print(f"[raw] wrote {OUT_RAW} ({len(pages)} pages)")

    # Transform -> CSV rows.
    fields = [
        "permit_id",
        "issue_date",
        "address",
        "latitude",
        "longitude",
        "units",
        "work_description",
        "valuation",
    ]

    dropped_no_geom = 0
    dropped_no_units = 0
    rows_out: list[dict] = []
    for feat in features:
        attrs = feat.get("attributes") or {}
        geom = feat.get("geometry") or {}
        lat = to_float(geom.get("y"))
        lon = to_float(geom.get("x"))
        if lat is None or lon is None:
            dropped_no_geom += 1
            continue
        units = to_float(attrs.get("NEW_UNITS"))
        # The layer is already restricted to permits adding residential units,
        # but defensively drop rows where units is missing/zero so the
        # densification indicator isn't biased by zero-unit noise.
        if units is None or units <= 0:
            dropped_no_units += 1
            continue
        work_desc_parts = [
            str(attrs.get("WORKDESC") or "").strip(),
            str(attrs.get("NEWTYPE") or "").strip(),
        ]
        if str(attrs.get("IS_ADU") or "").lower() == "true":
            work_desc_parts.append("ADU")
        work_description = " | ".join(p for p in work_desc_parts if p)

        permit_id = (attrs.get("FOLDERNUMB") or attrs.get("REV") or "").strip()
        rows_out.append({
            "permit_id": permit_id,
            "issue_date": epoch_ms_to_iso(attrs.get("ISSUEDATE")),
            "address": (attrs.get("PROP_ADDRE") or "").strip(),
            "latitude": round(lat, 7),
            "longitude": round(lon, 7),
            "units": int(units) if units.is_integer() else units,
            "work_description": work_description,
            "valuation": to_float(attrs.get("VALUATION")),
        })

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows_out:
            w.writerow(r)
    print(f"[csv] wrote {len(rows_out)} rows -> {OUT_CSV}")
    print(f"[csv] dropped {dropped_no_geom} rows missing geometry")
    print(f"[csv] dropped {dropped_no_units} rows missing/zero NEW_UNITS")

    if rows_out:
        dates = [r["issue_date"] for r in rows_out if r["issue_date"]]
        if dates:
            print(f"[csv] issue_date range: {min(dates)} .. {max(dates)}")
        total_units = sum(
            float(r["units"]) for r in rows_out if r.get("units") is not None
        )
        print(f"[csv] total new units in file: {int(total_units)}")


if __name__ == "__main__":
    main()
