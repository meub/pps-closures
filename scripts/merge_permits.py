"""Merge Portland BDS residential permits (2022+) into master using PPS
attendance polygons (with a 1-mile haversine fallback for schools without a
catchment).

Adds: permits_units_within_1mi_since_2022, n_permits_within_1mi_since_2022.
Source CSV: Portland BDS via PortlandMaps ArcGIS MapServer/89.

The "within_1mi" column names are retained for compatibility with existing
consumers; the actual geography used is the school's PPS attendance area where
one exists, or a 1-mile radius fallback otherwise.
"""
import pandas as pd
from pathlib import Path

from boundary_join import BoundaryIndex, build_point_filter, haversine_miles

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data/pps_schools.csv"
PERMITS = ROOT / "data/raw/portland_bds_permits.csv"
RADIUS_MILES = 1.0


def main():
    schools = pd.read_csv(MASTER)
    permits = pd.read_csv(PERMITS).dropna(subset=["latitude", "longitude", "units"])
    permits = permits[permits["units"] > 0]
    print(f"Permits: {len(permits)} rows, {int(permits['units'].sum())} units, "
          f"{permits['issue_date'].min()}..{permits['issue_date'].max()}")

    boundaries = BoundaryIndex()
    unmapped = []

    def aggregate(row):
        lat = row["latitude"]
        lon = row["longitude"]
        if pd.isna(lat):
            return 0, 0
        name = row["school_name"]
        level = row.get("level")
        poly = boundaries.polygon_for(name, level)
        if poly == "UNMAPPED":
            if name not in unmapped:
                unmapped.append(name)
            poly = None
        if poly is not None:
            inside = build_point_filter(poly)
            mask = permits.apply(lambda r: inside(r["latitude"], r["longitude"]), axis=1)
            units = int(permits.loc[mask, "units"].sum())
            return units, int(mask.sum())
        units = 0
        n = 0
        for _, p in permits.iterrows():
            if haversine_miles(lat, lon, p["latitude"], p["longitude"]) <= RADIUS_MILES:
                units += p["units"]
                n += 1
        return int(units), n

    def haversine_units_only(row):
        lat = row["latitude"]; lon = row["longitude"]
        if pd.isna(lat):
            return 0
        units = 0
        for _, p in permits.iterrows():
            if haversine_miles(lat, lon, p["latitude"], p["longitude"]) <= RADIUS_MILES:
                units += p["units"]
        return int(units)

    def method_for(row):
        poly = boundaries.polygon_for(row["school_name"], row.get("level"))
        return "fallback" if poly in (None, "UNMAPPED") else "polygon"

    schools["_agg_method"] = schools.apply(method_for, axis=1)

    cand_mask = schools["is_closure_candidate"].fillna(False).astype(bool)
    old_counts = {r["school_name"]: haversine_units_only(r)
                  for _, r in schools[cand_mask].iterrows()}

    results = schools.apply(lambda r: pd.Series(aggregate(r)), axis=1)
    schools["permits_units_within_1mi_since_2022"] = results[0].astype("Int64")
    schools["n_permits_within_1mi_since_2022"] = results[1].astype("Int64")

    method_counts = schools["_agg_method"].value_counts().to_dict()
    fallback_names = schools.loc[schools["_agg_method"] == "fallback", "school_name"].tolist()
    matched_names = schools.loc[schools["_agg_method"] == "polygon", "school_name"].tolist()
    schools = schools.drop(columns=["_agg_method"])
    schools.to_csv(MASTER, index=False)
    print(f"Wrote {len(schools)} rows to {MASTER}")
    print()
    print(f"Aggregation method: {method_counts}")
    if unmapped:
        print(f"WARNING: {len(unmapped)} school(s) missing from BOUNDARY_NAME_MAP:")
        for n in unmapped:
            print(f"  - {n!r}")
    print(f"Polygon-matched ({len(matched_names)})")
    print(f"Fallback (1-mi radius) ({len(fallback_names)}): {fallback_names}")
    print()
    print("Closure candidates — permits (old 1-mi -> new catchment):")
    cand = schools[cand_mask].sort_values("closure_rank")
    for _, r in cand.iterrows():
        old = old_counts.get(r["school_name"], 0)
        new = int(r["permits_units_within_1mi_since_2022"] or 0)
        print(f"  #{int(r['closure_rank'] or 0):>2} {r['school_name']:<40} "
              f"1mi={old:>5}  catchment={new:>5}  Δ={new - old:+d}")
    print()
    print(cand[[
        "school_name", "closure_rank", "enrollment_2025_26",
        "permits_units_within_1mi_since_2022",
        "n_permits_within_1mi_since_2022",
        "pipeline_family_units_within_1mi",
    ]].to_string(index=False))


if __name__ == "__main__":
    main()
