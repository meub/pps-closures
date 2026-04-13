"""Download PPS attendance-area polygons from the City of Portland
School_Boundaries FeatureServer and dissolve them into per-school polygons
at three levels: elementary, middle, and high school.

Source:
  https://services.arcgis.com/quVN97tn06YNGj9s/arcgis/rest/services/School_Boundaries/FeatureServer/0

The source layer (School_Attendance_Areas) stores one polygon per "cell" with
three assignment columns: Grade_1_Choice1_Name (elementary/K-8),
Grade_6_Choice1_Name (middle), Grade_10_Choice1_Name (high). Many cells
share the same school, so we dissolve/union them per school.

Output GeoJSON files (EPSG:4326):
  data/raw/pps_boundaries_elementary.geojson
  data/raw/pps_boundaries_middle.geojson
  data/raw/pps_boundaries_high.geojson
"""
import json
import urllib.request
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data/raw"
SOURCE = (
    "https://services.arcgis.com/quVN97tn06YNGj9s/arcgis/rest/services/"
    "School_Boundaries/FeatureServer/0/query"
    "?where=Unified_SD_Name%3D%27Portland+SD+1J%27"
    "&outFields=*&returnGeometry=true&outSR=4326&f=geojson"
)

LEVEL_FIELDS = {
    "elementary": "Grade_1_Choice1_Name",
    "middle": "Grade_6_Choice1_Name",
    "high": "Grade_10_Choice1_Name",
}


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(SOURCE) as r:
        fc = json.loads(r.read())
    feats = fc.get("features", [])
    print(f"Source: {len(feats)} cells for Portland SD 1J")

    for level, field in LEVEL_FIELDS.items():
        # Group cell polygons by school name.
        by_school = {}
        for f in feats:
            name = f["properties"].get(field)
            if not name:
                continue
            geom = shape(f["geometry"])
            by_school.setdefault(name, []).append(geom)

        # Dissolve each school's cells into a single (multi)polygon.
        out_feats = []
        for name, geoms in sorted(by_school.items()):
            merged = unary_union(geoms)
            out_feats.append({
                "type": "Feature",
                "properties": {"school_name": name, "level": level},
                "geometry": mapping(merged),
            })

        out = {"type": "FeatureCollection", "features": out_feats}
        path = RAW / f"pps_boundaries_{level}.geojson"
        path.write_text(json.dumps(out))
        print(f"  {level}: {len(out_feats)} schools -> {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
