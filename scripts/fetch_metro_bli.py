#!/usr/bin/env python3
"""Fetch the Building Land Inventory (BLI) Housing-Employment Allocation grid
from the City of Portland open-data ArcGIS service.

Endpoint:
  https://www.portlandmaps.com/od/rest/services/COP_OpenData_PlanningDevelopment/MapServer/88

Dataset (per metadata): a grid-cell layer assigning a share of Metro's
2045 BLI allocation (housing units + jobs) to each cell. The key field for
future school-enrollment context is `Forecast_Units_Prop` — projected
new residential units by grid cell by ~2035.

Output:
  data/raw/metro_bli_housing_allocation.geojson
    FeatureCollection of polygons with attributes:
      Grid_ID, Total_Existing_Units,
      Forecast_SFR_Units_Prop, Forecast_MFR_Units_Prop,
      Forecast_Units_Prop, Forecast_Jobs_Prop
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data/raw/metro_bli_housing_allocation.geojson"

SERVICE_URL = (
    "https://www.portlandmaps.com/od/rest/services/"
    "COP_OpenData_PlanningDevelopment/MapServer/88/query"
)
UA = {"User-Agent": "Mozilla/5.0 (research; pps-closure-analysis)"}

OUT_FIELDS = [
    "Grid_ID",
    "Total_Existing_Units",
    "Forecast_SFR_Units_Prop",
    "Forecast_MFR_Units_Prop",
    "Forecast_Units_Prop",
    "Forecast_Jobs_Prop",
]
PAGE_SIZE = 200
SLEEP = 0.4


def http_get(url: str, timeout: int = 90) -> bytes:
    with urlopen(Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def count_records() -> int:
    url = SERVICE_URL + "?" + urlencode({
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    })
    return int(json.loads(http_get(url)).get("count", 0))


def fetch_page(offset: int) -> dict:
    url = SERVICE_URL + "?" + urlencode({
        "where": "1=1",
        "outFields": ",".join(OUT_FIELDS),
        "outSR": "4326",
        "orderByFields": "Grid_ID ASC",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "returnGeometry": "true",
        "f": "geojson",
    })
    return json.loads(http_get(url))


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    total = count_records()
    print(f"[count] {total} BLI grid cells")

    features: list[dict] = []
    offset = 0
    while True:
        page = fetch_page(offset)
        feats = page.get("features") or []
        if not feats:
            break
        features.extend(feats)
        print(f"[page] offset={offset} got={len(feats)} cumulative={len(features)}")
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(SLEEP)

    fc = {"type": "FeatureCollection", "features": features}
    OUT.write_text(json.dumps(fc))
    print(f"[out] wrote {len(features)} features -> {OUT}")

    if features:
        units = sum(
            (f["properties"].get("Forecast_Units_Prop") or 0) for f in features
        )
        print(f"[stat] sum Forecast_Units_Prop = {units:,.0f} projected new units")


if __name__ == "__main__":
    main()
