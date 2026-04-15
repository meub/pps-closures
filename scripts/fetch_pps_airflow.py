"""Fetch PPS's per-school airflow test PDFs.

PPS publishes one PDF per school with a 2021-vintage NEBB-certified
airflow survey (contractor: Amerseco + Neudorfer Engineers). The list
lives in an HTML table on pps.net; each row links two PDFs: an IAQ
test and an airflow test. Only the airflow PDF is needed for ACH.

This script scrapes the table, saves the school→URL index, and
downloads every airflow PDF to data/raw/pps_airflow_pdfs/. PDFs are
large (~3 MB each, ~270 MB total) and gitignored.
"""
import json
import re
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX_URL = "https://www.pps.net/departments/risk-management/healthy-schools/indoor-air-quality"
BASE = "https://www.pps.net"
OUT_DIR = ROOT / "data/raw/pps_airflow_pdfs"
OUT_INDEX = ROOT / "data/raw/pps_airflow_index.json"
UA = "Mozilla/5.0 (compatible; school-research scraper; contact: alexmeub@gmail.com)"


def slugify(name):
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return s


def fetch_index():
    req = urllib.request.Request(INDEX_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req) as r:
        html = r.read().decode("utf-8", errors="replace")
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
    if not tables:
        raise RuntimeError("no table found on IAQ page")
    t = tables[0]
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.DOTALL)
    out = []
    for row in rows[1:]:  # skip header
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
        if len(cells) < 3:
            continue
        name = re.sub(r"<[^>]+>", " ", cells[0])
        name = re.sub(r"&nbsp;", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        hrefs = re.findall(r'href="([^"]+)"', row)
        # Convention: first link = IAQ test, second = airflow test. Some
        # rows have only one PDF (airflow N/A), so skip those.
        if len(hrefs) < 2:
            continue
        iaq, airflow = hrefs[0], hrefs[1]
        out.append({
            "school_name": name,
            "slug": slugify(name),
            "iaq_pdf_url": BASE + iaq if iaq.startswith("/") else iaq,
            "airflow_pdf_url": BASE + airflow if airflow.startswith("/") else airflow,
        })
    return out


def download(url, dest):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req) as r, open(dest, "wb") as f:
        while True:
            chunk = r.read(64 * 1024)
            if not chunk:
                break
            f.write(chunk)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Fetching index from {INDEX_URL} ...")
    idx = fetch_index()
    print(f"  {len(idx)} schools with airflow PDFs")
    OUT_INDEX.write_text(json.dumps(idx, indent=2))
    print(f"Wrote {OUT_INDEX}")

    for i, row in enumerate(idx, 1):
        dest = OUT_DIR / f"{row['slug']}.pdf"
        if dest.exists() and dest.stat().st_size > 10_000:
            continue
        print(f"[{i}/{len(idx)}] {row['school_name']} → {dest.name}")
        try:
            download(row["airflow_pdf_url"], dest)
            time.sleep(0.3)  # polite pacing
        except Exception as e:
            print(f"  FAILED: {e}")
    print("Done.")


if __name__ == "__main__":
    main()
