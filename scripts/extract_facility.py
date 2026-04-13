"""Extract per-school facility data (year built, sq ft, construction type)
from the 2009 KPFF seismic report PDF. This is the most complete publicly
available table of PPS buildings with year_built and square_footage.

Caveat: the 2009 data pre-dates recent bond work — schools modernized since
2012 may have different square footage. Year built remains correct."""
import pdfplumber, re, csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PDF = ROOT / "data/raw/pps_seismic_report_2009.pdf"
OUT = ROOT / "data/pps_facility_2009.csv"

pattern = re.compile(
    r'^([YN])\s+(.+?)\s+([A-Z0-9\-/ ]+?)\s+(\d+)\s+(\d{4})\s+(\d{3,7})\s+(Yes|No)\s+([A-Za-z/]+)',
    re.MULTILINE,
)


def main():
    with pdfplumber.open(PDF) as pdf:
        text = '\n'.join(pdf.pages[i].extract_text() or '' for i in range(17, 21))
    rows = pattern.findall(text)
    with open(OUT, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow([
            "school_name_2009", "grade_range_2009", "floors",
            "year_built", "square_feet_2009", "construction_type",
            "has_additions", "asce_evaluated",
        ])
        for asce, name, grade, floors, year, sqft, adds, ctype in rows:
            w.writerow([
                name.strip(), grade.strip(), floors, year, sqft,
                ctype, adds, asce,
            ])
    print(f"Wrote {len(rows)} rows to {OUT}")


if __name__ == "__main__":
    main()
