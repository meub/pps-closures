"""Re-geocode the new pipeline rows via Nominatim.

The sub-agent used ZIP-centroid fallback for its 52 added rows. We can do
better — throttle to 1 req/sec per Nominatim ToS and fall back to existing
coordinates if a query returns no result.
"""
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "data/raw/portland_affordable_pipeline.csv"
BACKUP = ROOT / "data/raw/portland_affordable_pipeline.backup.csv"
CACHE = ROOT / "data/raw/.geocode_cache.json"

HEADERS = {"User-Agent": "school-research/1.0 (PPS closure analysis)"}
SKIP_ADDRESSES = {"TBD", "Various", "Scattered Sites"}


def nominatim(addr):
    q = urllib.parse.urlencode({
        "q": f"{addr}, Portland, OR",
        "format": "json", "limit": 1, "countrycodes": "us",
    })
    url = f"https://nominatim.openstreetmap.org/search?{q}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        if d:
            return float(d[0]["lat"]), float(d[0]["lon"])
    except Exception as e:
        print(f"  geocode err: {e}")
    return None


def main():
    df = pd.read_csv(CSV)
    backup = pd.read_csv(BACKUP)
    new_mask = ~df["project_name"].isin(backup["project_name"])
    print(f"Re-geocoding {new_mask.sum()} new rows...")

    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    updated = 0
    for idx in df.index[new_mask]:
        addr = str(df.at[idx, "address"]).strip()
        if not addr or addr in SKIP_ADDRESSES or addr.lower() == "nan":
            continue
        if addr in cache:
            lat, lon = cache[addr]
        else:
            result = nominatim(addr)
            if result is None:
                cache[addr] = [None, None]
                print(f"  MISS  {addr}")
                time.sleep(1.1)
                continue
            lat, lon = result
            cache[addr] = [lat, lon]
            print(f"  OK    {addr}  -> {lat:.5f}, {lon:.5f}")
            time.sleep(1.1)

        if lat is None:
            continue
        df.at[idx, "latitude"] = lat
        df.at[idx, "longitude"] = lon
        updated += 1

    CACHE.write_text(json.dumps(cache, indent=2))
    df.to_csv(CSV, index=False)
    print(f"\nRe-geocoded {updated} rows. Cache saved to {CACHE}.")


if __name__ == "__main__":
    main()
