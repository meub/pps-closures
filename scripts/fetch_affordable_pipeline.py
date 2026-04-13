#!/usr/bin/env python3
"""Fetch affordable housing pipeline data for Portland, OR.

Primary source: Oregon Affordable Housing Inventory (OAHI) on data.oregon.gov.
Secondary: Metro RLIS Affordable Housing FeatureServer (for Portland records
built in 2015+ that may not yet be in OAHI).

Output: /Users/meuba/Code/school-research/data/raw/portland_affordable_pipeline.csv
"""
from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OUT = Path("/Users/meuba/Code/school-research/data/raw/portland_affordable_pipeline.csv")
UA = {"User-Agent": "Mozilla/5.0 (research; school-affordable-housing-linker)"}

# Portland city bbox (rough) — used as a secondary filter for records whose
# "city" field might say e.g. PORTLAND but lat/lon is outside. Also used when
# city is blank.
PORTLAND_BBOX = (45.43, 45.66, -122.85, -122.44)  # (lat_min, lat_max, lon_min, lon_max)


def http_get(url: str, timeout: int = 120) -> bytes:
    with urlopen(Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def parse_int(v) -> int | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "N/A", "NA", "null"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def parse_float(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "-", "N/A", "NA", "null"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def in_portland_bbox(lat, lon) -> bool:
    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return False
    lat_min, lat_max, lon_min, lon_max = PORTLAND_BBOX
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


# ---------------------------------------------------------------------------
# Source 1: OAHI (Oregon Affordable Housing Inventory) via data.oregon.gov
# ---------------------------------------------------------------------------
OAHI_URL = "https://data.oregon.gov/api/views/p9yn-ftai/rows.csv?accessType=DOWNLOAD"

POINT_RE = re.compile(r"POINT\s*\(\s*(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s*\)", re.I)


def parse_geocode(geocode: str) -> tuple[float | None, float | None]:
    if not geocode:
        return None, None
    m = POINT_RE.search(geocode)
    if not m:
        return None, None
    lon = float(m.group(1))
    lat = float(m.group(2))
    return lat, lon


STATUS_MAP = {
    "active": "completed",
    "in development": "in_development",
    "closed": "closed",
    "inactive": "closed",
}


def fetch_oahi() -> list[dict]:
    print(f"[oahi] downloading {OAHI_URL}")
    text = http_get(OAHI_URL).decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows_all = list(reader)
    print(f"[oahi] total statewide rows: {len(rows_all)}")
    out: list[dict] = []
    for r in rows_all:
        city = (r.get("City") or "").strip()
        if city.upper() != "PORTLAND":
            continue
        lat, lon = parse_geocode(r.get("Geocode") or "")
        # Skip anything clearly outside Portland bbox
        if lat is not None and lon is not None and not in_portland_bbox(lat, lon):
            continue
        total_units = parse_int(r.get("Total Units"))
        br2 = parse_int(r.get("Total_2_BR_Units")) or 0
        br3 = parse_int(r.get("Total_3_BR_Units")) or 0
        br4 = parse_int(r.get("Total_4Plus_BR_Units")) or 0
        family = br2 + br3 + br4
        # Deep-affordable AMI-based unit count (<=60% AMI).
        ami30 = parse_int(r.get("Total_30_AMI_Units")) or 0
        ami40 = parse_int(r.get("Total_40_AMI_Units")) or 0
        ami50 = parse_int(r.get("Total_50_AMI_Units")) or 0
        ami60 = parse_int(r.get("Total_60_AMI_Units")) or 0
        ami80 = parse_int(r.get("Total_80_AMI_Units")) or 0
        mkt = parse_int(r.get("Market_Rate_Units")) or 0
        affordable_units = ami30 + ami40 + ami50 + ami60 + ami80
        if affordable_units == 0 and total_units is not None and mkt == 0:
            # If no AMI breakdown but all units are regulated, fall back to total.
            affordable_units = total_units
        status_raw = (r.get("Status") or "").strip()
        status = STATUS_MAP.get(status_raw.lower(), status_raw.lower() or "unknown")
        if status == "completed":
            status = "in_service"
        expected_year = parse_int(r.get("Year Built")) or parse_int(r.get("Rehab Year"))
        # For "in_development" rows, try to infer expected year from closing date
        if status == "in_development" and not expected_year:
            fc = (r.get("Financial_Closing_Date") or "").strip()
            m = re.search(r"(\d{4})", fc)
            if m:
                expected_year = int(m.group(1)) + 2  # typical construction lag
        out.append({
            "project_name": (r.get("Property Name") or "").strip(),
            "address": (r.get("Address") or "").strip(),
            "city": "Portland",
            "zip": (r.get("Zip Code") or "").strip(),
            "latitude": lat,
            "longitude": lon,
            "total_units": total_units,
            "affordable_units": affordable_units or None,
            "family_units_estimate": family or None,
            "status": status,
            "expected_year": expected_year,
            "source": "OAHI (data.oregon.gov p9yn-ftai)",
        })
    print(f"[oahi] kept {len(out)} Portland rows")
    return out


# ---------------------------------------------------------------------------
# Source 2: Metro RLIS Affordable Housing FeatureServer (backfill recent 2015+)
# ---------------------------------------------------------------------------
METRO_BASE = (
    "https://services2.arcgis.com/McQ0OlIABe29rJJy/arcgis/rest/services/"
    "Affordable_Housing/FeatureServer"
)


def fetch_metro() -> list[dict]:
    # Query Portland records only, with Year_Built >= 2015 to focus on recent.
    where = "GIS_City = 'PORTLAND'"
    url = METRO_BASE + "/0/query?" + urlencode({
        "where": where,
        "outFields": ",".join([
            "Metro_ID", "Proj_Name", "Proj_Addr", "City", "ZIP", "GIS_City",
            "Proj_Type", "Const_Type", "Year_Built", "Year_Rehab",
            "Units", "Reg_Units", "UReg_Units", "HUD_Assisted_Units",
        ]),
        "outSR": "4326",
        "f": "geojson",
        "resultRecordCount": 4000,
    })
    print(f"[metro] querying {url[:90]}...")
    data = json.loads(http_get(url))
    feats = data.get("features") or []
    print(f"[metro] {len(feats)} Portland features")
    out: list[dict] = []
    for f in feats:
        props = f.get("properties") or {}
        geom = f.get("geometry") or {}
        lon = lat = None
        if geom.get("type") == "Point":
            coords = geom.get("coordinates") or []
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
        year = parse_int(props.get("Year_Built")) or parse_int(props.get("Year_Rehab"))
        status = "in_service"  # Metro inventory is of existing housing
        out.append({
            "project_name": (props.get("Proj_Name") or "").strip(),
            "address": (props.get("Proj_Addr") or "").strip(),
            "city": "Portland",
            "zip": (props.get("ZIP") or "").strip(),
            "latitude": lat,
            "longitude": lon,
            "total_units": parse_int(props.get("Units")),
            "affordable_units": parse_int(props.get("Reg_Units")),
            "family_units_estimate": None,  # Metro unit-table has BR breakdown but requires join
            "status": status,
            "expected_year": year,
            "source": "Metro RLIS Affordable Housing",
        })
    return out


def metro_unit_breakdown() -> dict[int, dict]:
    """Join metro unit table for BR breakdown per Metro_ID."""
    url = METRO_BASE + "/1/query?" + urlencode({
        "where": "1=1", "outFields": "Metro_ID,Number_Units,Unit_Type,Regulated_Type",
        "f": "json", "resultRecordCount": 20000,
    })
    try:
        data = json.loads(http_get(url))
    except Exception as e:
        print(f"[metro-units] failed: {e}")
        return {}
    agg: dict[int, dict] = {}
    for feat in data.get("features", []):
        a = feat.get("attributes") or {}
        mid = a.get("Metro_ID")
        if mid is None:
            continue
        e = agg.setdefault(mid, {"family": 0, "total": 0})
        n = parse_int(a.get("Number_Units")) or 0
        ut = (a.get("Unit_Type") or "").upper()
        e["total"] += n
        if ut in ("2BR", "3BR", "4BR", "4+BR", "5BR", "5+BR"):
            e["family"] += n
    print(f"[metro-units] aggregated {len(agg)} projects")
    return agg


# ---------------------------------------------------------------------------
# Merge + write
# ---------------------------------------------------------------------------
def dedupe(rows: list[dict]) -> list[dict]:
    """Dedupe by normalized (name, address). OAHI rows win over Metro."""
    def norm(s):
        return re.sub(r"\s+", " ", (s or "").strip().lower())
    best: dict[tuple, dict] = {}
    for r in rows:
        key = (norm(r.get("project_name")), norm(r.get("address")))
        if key == ("", ""):
            continue
        if key not in best:
            best[key] = r
        else:
            # Prefer OAHI
            existing_src = best[key].get("source", "")
            new_src = r.get("source", "")
            if "OAHI" in new_src and "OAHI" not in existing_src:
                best[key] = r
    return list(best.values())


def write_csv(rows: list[dict]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "project_name", "address", "city", "zip", "latitude", "longitude",
        "total_units", "affordable_units", "family_units_estimate",
        "status", "expected_year", "source",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})
    print(f"Wrote {len(rows)} rows -> {OUT}")


def main():
    rows: list[dict] = []
    try:
        rows.extend(fetch_oahi())
    except Exception as e:
        print(f"[oahi] fatal: {e}")

    try:
        metro_rows = fetch_metro()
        # Attach BR breakdown
        breakdown = metro_unit_breakdown()
        # Map by Metro_ID -- but our metro_rows lost the Metro_ID field, add back by re-query if needed
        # Simpler: re-fetch with Metro_ID so we can attach
        # Re-fetch + map
        url = METRO_BASE + "/0/query?" + urlencode({
            "where": "GIS_City = 'PORTLAND'",
            "outFields": "Metro_ID,Proj_Name,Proj_Addr,ZIP,Units,Reg_Units,Year_Built,Year_Rehab",
            "outSR": "4326",
            "f": "geojson",
            "resultRecordCount": 4000,
        })
        data = json.loads(http_get(url))
        id_by_key: dict[tuple, int] = {}
        for f in data.get("features", []):
            p = f.get("properties") or {}
            k = ((p.get("Proj_Name") or "").strip().lower(), (p.get("Proj_Addr") or "").strip().lower())
            mid = p.get("Metro_ID")
            if mid is not None:
                id_by_key[k] = mid
        for r in metro_rows:
            k = ((r.get("project_name") or "").strip().lower(), (r.get("address") or "").strip().lower())
            mid = id_by_key.get(k)
            if mid and mid in breakdown:
                fam = breakdown[mid].get("family") or 0
                if fam:
                    r["family_units_estimate"] = fam
        rows.extend(metro_rows)
    except Exception as e:
        print(f"[metro] fatal: {e}")

    rows = dedupe(rows)
    write_csv(rows)

    # quick summary
    from collections import Counter
    ctr = Counter(r.get("status") for r in rows)
    print("\nStatus breakdown:")
    for s, n in ctr.most_common():
        print(f"  {s}: {n}")
    with_ll = sum(1 for r in rows if r.get("latitude") and r.get("longitude"))
    with_fam = sum(1 for r in rows if r.get("family_units_estimate"))
    print(f"Rows with lat/lon: {with_ll}/{len(rows)}")
    print(f"Rows with family_units_estimate: {with_fam}/{len(rows)}")
    yrs = [r["expected_year"] for r in rows if r.get("expected_year")]
    if yrs:
        print(f"Year range: min={min(yrs)} max={max(yrs)}")


if __name__ == "__main__":
    main()
