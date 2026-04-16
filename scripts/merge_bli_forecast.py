"""Merge Metro BLI (2045) housing-allocation grid cells into the master CSV,
attributing `Forecast_Units_Prop` to each school's catchment by area-weighted
overlap.

For each grid-cell polygon that intersects a school's catchment polygon,
the fraction `intersection.area / cell.area` of the cell's
`Forecast_Units_Prop` is added to that school's total. Schools without a
catchment (alt programs, focus options, embedded programs) fall back to a
1-mile buffer around their point location.

Adds to data/pps_schools.csv:
  bli_forecast_units_within_catchment  (projected new units by ~2035)
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from shapely.geometry import Point, shape
from shapely.prepared import prep
from shapely.strtree import STRtree

from boundary_join import BoundaryIndex, haversine_miles

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data/pps_schools.csv"
BLI = ROOT / "data/raw/metro_bli_housing_allocation.geojson"
RADIUS_MILES = 1.0
# Rough WGS84 degree → miles; good enough to build a 1-mile buffer at PDX.
DEG_PER_MILE_LAT = 1.0 / 69.0
DEG_PER_MILE_LON = 1.0 / 53.0  # at ~45°N


def load_cells():
    fc = json.loads(BLI.read_text())
    geoms = []
    units = []
    for feat in fc["features"]:
        g = shape(feat["geometry"])
        if not g.is_valid:
            g = g.buffer(0)
        geoms.append(g)
        props = feat.get("properties") or {}
        units.append(float(props.get("Forecast_Units_Prop") or 0.0))
    return geoms, units


def area_weighted_sum(target_poly, tree: STRtree, geoms, units) -> float:
    idxs = tree.query(target_poly)
    total = 0.0
    for i in idxs:
        cell = geoms[int(i)]
        u = units[int(i)]
        if u <= 0 or cell.area <= 0:
            continue
        inter = cell.intersection(target_poly)
        if inter.is_empty:
            continue
        total += u * (inter.area / cell.area)
    return total


def main():
    schools = pd.read_csv(MASTER)
    geoms, units = load_cells()
    tree = STRtree(geoms)
    print(f"BLI: {len(geoms)} cells, {sum(units):,.0f} Forecast_Units_Prop total")

    boundaries = BoundaryIndex()
    fallback_schools = []
    unmapped = []

    def aggregate(row):
        name = row["school_name"]
        level = row.get("level")
        lat = row["latitude"]
        lon = row["longitude"]
        poly = boundaries.polygon_for(name, level)
        if poly == "UNMAPPED":
            unmapped.append(name)
            poly = None
        if poly is not None:
            return round(area_weighted_sum(poly, tree, geoms, units), 1)
        if pd.isna(lat) or pd.isna(lon):
            return 0.0
        fallback_schools.append(name)
        # Build an approximate 1-mile buffer as a lat/lon ellipse around the point.
        dy = RADIUS_MILES * DEG_PER_MILE_LAT
        dx = RADIUS_MILES * DEG_PER_MILE_LON
        circle = Point(lon, lat).buffer(1.0)  # unit circle in degree space
        # scale the unit circle to the ellipse radii
        from shapely.affinity import scale
        buf = scale(circle, xfact=dx, yfact=dy, origin=(lon, lat))
        return round(area_weighted_sum(buf, tree, geoms, units), 1)

    schools["bli_forecast_units_within_catchment"] = schools.apply(aggregate, axis=1)
    schools.to_csv(MASTER, index=False)
    print(f"Wrote {len(schools)} rows to {MASTER}")

    if unmapped:
        print(f"WARN: unmapped schools: {unmapped}")
    if fallback_schools:
        print(f"Fallback (1-mi radius): {fallback_schools}")

    cand_mask = schools["is_closure_candidate"].fillna(False).astype(bool)
    cand = schools[cand_mask].copy().sort_values(
        "bli_forecast_units_within_catchment", ascending=False
    )
    print()
    print(cand[[
        "school_name",
        "enrollment_2025_26",
        "bli_forecast_units_within_catchment",
        "pipeline_family_units_within_1mi",
        "permits_units_within_1mi_since_2022",
    ]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
