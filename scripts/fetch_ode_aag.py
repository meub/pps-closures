"""Download ODE's statewide At-A-Glance Schools source file (CSV).

Contains the metrics displayed on each school's At-A-Glance profile: regular
attenders %, experienced teachers %, teacher retention rate, class size,
wraparound staff counts, proficiency rates, on-track, grad rates.

Year ID map (from /data/ReportCard/Media/GetYears): 26 = 2024-25, 25 = 2023-24.
"""
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data/raw/ode_aag_schools_2425.csv"

YEAR_ID = 26  # 2024-25
URL = (
    "https://www.ode.state.or.us/data/ReportCard/Media/DownloadFile"
    f"?schlYr={YEAR_ID}&fldr=stateData&flNm=AAGmediaSchoolsAggregate"
)


def main():
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes(r.content)
    print(f"Wrote {len(r.content):,} bytes to {OUT}")


if __name__ == "__main__":
    main()
