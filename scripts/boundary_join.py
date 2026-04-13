"""Shared helper for point-in-polygon aggregation against PPS attendance areas.

Loads the three GeoJSON files produced by fetch_boundaries.py and, given a
master CSV row, returns the polygon shapes matching that school's catchment
(elementary/K-8 use the grade-1 layer; middle schools use the grade-6 layer;
high schools use the grade-10 layer).

Names in the boundary layer are short (e.g., "Arleta"); the master CSV uses
long names (e.g., "Arleta Elementary School"). BOUNDARY_NAME_MAP bridges the
two. Schools that have no catchment polygon (alternative programs, embedded
programs, schools opened post-dataset) are returned as None so callers can
fall back to a haversine radius.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from shapely.geometry import shape, Point
from shapely.prepared import prep

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data/raw"

# Master school_name -> boundary polygon name at the relevant level.
# Most names differ only by level suffix ("Arleta Elementary School" vs
# "Arleta"); a few are structurally different (McDaniel rename, combined
# Boise-Eliot/Humboldt polygon, etc).
BOUNDARY_NAME_MAP = {
    # === elementary / K-8 ===
    "Abernethy Elementary School": "Abernethy",
    "Ainsworth Elementary School": "Ainsworth",
    "Alameda Elementary School": "Alameda",
    "Arleta Elementary School": "Arleta",
    "Astor Elementary School": "Astor",
    "Atkinson Elementary School": "Atkinson",
    "Beach Elementary School": "Beach",
    "Beverly Cleary School": "Beverly Cleary",
    "Beverly Cleary School ": "Beverly Cleary",  # trailing space in master
    "Boise-Eliot Elementary School": "Boise-Eliot/Humboldt",
    "Bridger Creative Science School": "Bridger",
    "Bridlemile Elementary School": "Bridlemile",
    "Buckman Elementary School": "Buckman",
    "Capitol Hill Elementary School": "Capitol Hill",
    "César Chávez K-8 School": "Cesar Chavez",
    "Chapman Elementary School": "Chapman",
    "Chief Joseph Elementary School": "Chief Joseph",
    "Clark Elementary School": "Clark",
    "Creston Elementary School": "Creston",
    "Dr. Martin Luther King Jr. School": "MLK Jr",
    "Duniway Elementary School": "Duniway",
    "Faubion Elementary School": "Faubion",
    "Forest Park Elementary School": "Forest Park",
    "Glencoe Elementary School": "Glencoe",
    "Grout Elementary School": "Grout",
    "Hayhurst Elementary School": "Hayhurst",
    "Irvington Elementary School": "Irvington",
    "James John Elementary School": "James John",
    "Kelly Elementary School": "Kelly",
    "Laurelhurst Elementary School": "Laurelhurst",
    "Lee Elementary School": "Lee",
    "Lent Elementary School": "Lent",
    "Lewis Elementary School": "Lewis",
    "Llewellyn Elementary School": "Llewellyn",
    "Maplewood Elementary School": "Maplewood",
    "Markham Elementary School": "Markham",
    "Marysville Elementary School": "Marysville",
    "Peninsula Elementary School": "Peninsula",
    "Richmond Elementary School": "Richmond",  # may be absent (immersion lottery)
    "Rieke Elementary School": "Rieke",
    "Rigler Elementary School": "Rigler",
    "Rosa Parks Elementary School": "Rosa Parks",
    "Rose City Park": "Rose City Park",
    "Sabin Elementary School": "Sabin",
    "Scott Elementary School": "Scott",
    "Sitton Elementary School": "Sitton",
    "Skyline Elementary School": "Skyline",
    "Stephenson Elementary School": "Stephenson",
    "Sunnyside Environmental School": "Sunnyside",
    "Vernon Elementary School": "Vernon",
    "Vestal Elementary School": "Vestal",
    "Whitman Elementary School": "Whitman",
    "Winterhaven School": "Winterhaven",  # focus-option — likely no polygon
    "Woodlawn Elementary School": "Woodlawn",
    "Woodmere Elementary School": "Woodmere",
    "Woodstock Elementary School": "Woodstock",
    # === middle ===
    "Beaumont Middle School": "Beaumont",
    "George Middle School": "George",
    "Gray Middle School": "Gray",
    "Harriet Tubman Middle School": "Tubman",
    "Harrison Park School": "Harrison Park",
    "Hosford Middle School": "Hosford",
    "Jackson Middle School": "Jackson",
    "Kellogg Middle School": "Kellogg",
    "Lane Middle School": "Lane",
    "Mt Tabor Middle School": "Mt Tabor",
    "Ockley Green Middle School": "Ockley Green",
    "Roseway Heights School": "Roseway Heights",
    "Sellwood Middle School": "Sellwood",
    "West Sylvan Middle School": "West Sylvan",
    "da Vinci Middle School": None,  # focus-option middle school, no catchment
    # === high ===
    "Benson Polytechnic High School": None,  # focus-option, no base catchment
    "Cleveland High School": "Cleveland",
    "Franklin High School": "Franklin",
    "Grant High School": "Grant",
    "Ida B. Wells-Barnett High School": "Ida B. Wells",
    "Jefferson High School": None,  # overlay on Grant/McDaniel/Roosevelt; use fallback
    "Leodis V. McDaniel High School": "McDaniel",
    "Lincoln High School": "Lincoln",
    "Roosevelt High School": "Roosevelt",
    # === alternative / embedded — no catchment ===
    "ACCESS Academy": None,
    "Alliance High School": None,
    "Metropolitan Learning Center": None,
    "Odyssey Program (K-8)": None,
}


def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _load_level(level: str) -> dict:
    path = RAW / f"pps_boundaries_{level}.geojson"
    data = json.loads(path.read_text())
    out = {}
    for feat in data["features"]:
        name = feat["properties"]["school_name"]
        geom = shape(feat["geometry"])
        out[name] = geom
    return out


class BoundaryIndex:
    """Lazy-loaded per-level polygon index with school-name lookup."""

    def __init__(self):
        self._levels: dict[str, dict[str, object]] = {}

    def _level_for(self, master_level: str | None) -> str | None:
        if master_level in ("middle",):
            return "middle"
        if master_level in ("high",):
            return "high"
        if master_level in ("elementary", "k8", "other"):
            return "elementary"
        return None  # alternative / unknown

    def _polygons(self, level: str):
        if level not in self._levels:
            self._levels[level] = _load_level(level)
        return self._levels[level]

    def polygon_for(self, school_name: str, master_level: str | None):
        """Return the shapely geometry for this school's catchment, or None."""
        lvl = self._level_for(master_level)
        if lvl is None:
            return None
        mapped = BOUNDARY_NAME_MAP.get(school_name, BOUNDARY_NAME_MAP.get(school_name.strip()))
        if mapped is None:
            # Either unmapped or explicitly mapped to None.
            if school_name not in BOUNDARY_NAME_MAP and school_name.strip() not in BOUNDARY_NAME_MAP:
                return "UNMAPPED"
            return None
        polys = self._polygons(lvl)
        return polys.get(mapped)


def build_point_filter(polygon):
    """Return a function(lat, lon) -> bool for fast membership testing."""
    if polygon is None:
        return None
    prepared = prep(polygon)
    def inside(lat, lon):
        return prepared.contains(Point(lon, lat))
    return inside
