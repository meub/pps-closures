"""Merge affordable housing pipeline counts into master using PPS attendance
polygons (with a 1-mile haversine fallback for schools without a catchment).

Adds four columns: affordable_units_within_1mi (existing inventory),
pipeline_affordable_units_within_1mi (in-development 2020+),
pipeline_family_units_within_1mi, and n_pipeline_projects_within_1mi.

The "within_1mi" column names are retained for compatibility with existing
consumers (web export, charts). The underlying aggregation now reflects each
school's PPS attendance area rather than a fixed radius, except for schools
that have no published catchment (alternative/embedded programs, focus-option
high schools), where a 1-mile radius is used as a fallback.
"""
import pandas as pd
from pathlib import Path

from boundary_join import BoundaryIndex, build_point_filter, haversine_miles

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data/pps_schools.csv"
HOUSING = ROOT / "data/raw/portland_affordable_pipeline.csv"
RADIUS_MILES = 1.0


def main():
    schools = pd.read_csv(MASTER)
    housing = pd.read_csv(HOUSING)
    housing = housing.dropna(subset=["latitude", "longitude"])

    in_service = housing[housing["status"] == "in_service"]
    pipeline_statuses = ["in_development", "permitted", "funded", "proposed"]
    pipeline = housing[housing["status"].isin(pipeline_statuses)]
    print(f"Pipeline breakdown: {pipeline['status'].value_counts().to_dict()}")

    boundaries = BoundaryIndex()
    unmapped = []
    fallback_schools = []
    matched_schools = []
    old_counts_1mi: dict[str, int] = {}

    def sum_col(rows, col):
        total = 0.0
        for v in rows[col]:
            if pd.notna(v):
                total += v
        return total

    def aggregate(row, subset, col):
        """Return the sum of `col` over subset that falls inside this school's
        catchment (with a 1-mile radius fallback)."""
        lat = row["latitude"]
        lon = row["longitude"]
        if pd.isna(lat):
            return 0.0
        name = row["school_name"]
        level = row.get("level")
        poly = boundaries.polygon_for(name, level)
        if poly == "UNMAPPED":
            if name not in unmapped:
                unmapped.append(name)
            poly = None
        if poly is not None:
            inside = build_point_filter(poly)
            mask = subset.apply(lambda r: inside(r["latitude"], r["longitude"]), axis=1)
            return float(subset.loc[mask, col].fillna(0).sum()) if mask.any() else 0.0
        # Fallback: 1-mile haversine radius.
        total = 0.0
        for _, h in subset.iterrows():
            if haversine_miles(lat, lon, h["latitude"], h["longitude"]) <= RADIUS_MILES:
                v = h[col]
                if pd.notna(v):
                    total += v
        return total

    def count_projects(row, subset):
        lat = row["latitude"]
        lon = row["longitude"]
        if pd.isna(lat):
            return 0
        name = row["school_name"]
        level = row.get("level")
        poly = boundaries.polygon_for(name, level)
        if poly == "UNMAPPED":
            poly = None
        if poly is not None:
            inside = build_point_filter(poly)
            mask = subset.apply(lambda r: inside(r["latitude"], r["longitude"]), axis=1)
            return int(mask.sum())
        n = 0
        for _, h in subset.iterrows():
            if haversine_miles(lat, lon, h["latitude"], h["longitude"]) <= RADIUS_MILES:
                n += 1
        return n

    def haversine_units(row, subset, col):
        """Always-haversine count for the old/new comparison diagnostic."""
        lat = row["latitude"]; lon = row["longitude"]
        if pd.isna(lat):
            return 0
        total = 0.0
        for _, h in subset.iterrows():
            if haversine_miles(lat, lon, h["latitude"], h["longitude"]) <= RADIUS_MILES:
                v = h[col]
                if pd.notna(v):
                    total += v
        return total

    # Track per-school method for the summary.
    def method_for(row):
        name = row["school_name"]
        level = row.get("level")
        poly = boundaries.polygon_for(name, level)
        if poly == "UNMAPPED" or poly is None:
            return "fallback"
        return "polygon"

    schools["_agg_method"] = schools.apply(method_for, axis=1)

    # Save old 1-mi counts on closure candidates before we overwrite, for diagnostic.
    cand_mask = schools["is_closure_candidate"].fillna(False).astype(bool)
    for _, r in schools[cand_mask].iterrows():
        old_counts_1mi[r["school_name"]] = int(
            haversine_units(r, in_service, "affordable_units")
        )

    schools["affordable_units_within_1mi"] = schools.apply(
        lambda r: aggregate(r, in_service, "affordable_units"), axis=1
    ).astype("Int64")
    schools["pipeline_affordable_units_within_1mi"] = schools.apply(
        lambda r: aggregate(r, pipeline, "affordable_units"), axis=1
    ).astype("Int64")
    schools["pipeline_family_units_within_1mi"] = schools.apply(
        lambda r: aggregate(r, pipeline, "family_units_estimate"), axis=1
    ).astype("Int64")
    schools["n_pipeline_projects_within_1mi"] = schools.apply(
        lambda r: count_projects(r, pipeline), axis=1
    ).astype("Int64")

    # Summarize polygon vs fallback attribution.
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
    print(f"Polygon-matched ({len(matched_names)}): {matched_names}")
    print(f"Fallback (1-mi radius) ({len(fallback_names)}): {fallback_names}")
    print()
    print("Closure candidates with housing pipeline context (old 1-mi -> new catchment):")
    cand = schools[cand_mask].sort_values("closure_rank")
    for _, r in cand.iterrows():
        old = old_counts_1mi.get(r["school_name"], 0)
        new = int(r["affordable_units_within_1mi"] or 0)
        diff = new - old
        print(f"  #{int(r['closure_rank'] or 0):>2} {r['school_name']:<40} "
              f"1mi={old:>5}  catchment={new:>5}  Δ={diff:+d}")
    print()
    print(cand[[
        "school_name", "closure_rank", "enrollment_2025_26",
        "affordable_units_within_1mi",
        "pipeline_affordable_units_within_1mi",
        "pipeline_family_units_within_1mi",
        "n_pipeline_projects_within_1mi",
    ]].to_string(index=False))


if __name__ == "__main__":
    main()
