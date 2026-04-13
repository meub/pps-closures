"""Export master CSV and column metadata to web/data.json for the static site."""
import json
import math
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "data/pps_schools.csv"
OUT_DATA = ROOT / "web/data.json"

# Column metadata: label, description, source, format.
# Format values: int, pct_0_100, pct_0_1, usd, text, bool, year, ratio.
META = {
    "school_name": {"label": "School", "desc": "School name (PPS Directory 2025-26).", "source": "PPS Directory", "fmt": "text"},
    "level": {"label": "Level", "desc": "Grade band served: elementary, k8, middle, high, alternative.", "source": "PPS Directory + ODE", "fmt": "text"},
    "is_closure_candidate": {"label": "Likely closure candidate", "desc": "One of the 15 schools Willamette Week ranked as most likely to be closed (2026-03-18). PPS has not released its own shortlist — that is expected Nov 2026.", "source": "Willamette Week", "fmt": "bool"},
    "closure_rank": {"label": "WW rank", "desc": "Willamette Week's published rank (1 = smallest), based solely on enrollment numbers. Not a PPS rank.", "source": "Willamette Week", "fmt": "int"},
    "enrollment_2025_26": {"label": "Enrollment 25-26", "desc": "Total students, fall 2025.", "source": "Oregon ODE Fall Membership", "fmt": "int"},
    "enrollment_2024_25": {"label": "Enrollment 24-25", "desc": "Total students, fall 2024.", "source": "Oregon ODE Fall Membership", "fmt": "int"},
    "enrollment_pct_change": {"label": "Enrollment Δ% YoY", "desc": "Year-over-year enrollment change (2024-25 → 2025-26).", "source": "Derived from ODE", "fmt": "pct_0_1"},
    "students_per_sqft": {"label": "Students / sqft", "desc": "Building crowding: 2025-26 enrollment ÷ 2009 KPFF square footage. Lower = more underused space.", "source": "Derived: ODE + KPFF 2009", "fmt": "ratio"},
    "year_built": {"label": "Year built", "desc": "Year the main building was constructed.", "source": "KPFF Seismic Report 2009", "fmt": "year"},
    "square_feet": {"label": "Square feet", "desc": "Building square footage (2009 inventory — may predate recent bond expansions).", "source": "KPFF Seismic Report 2009", "fmt": "int"},
    "construction_type_2009": {"label": "Construction", "desc": "Structural type: URM (unreinforced masonry), LRCW (reinforced concrete wall), Wood, Concrete, Steel, Masonry.", "source": "KPFF 2009", "fmt": "text"},
    "is_urm_building": {"label": "URM?", "desc": "Unreinforced masonry — highest seismic risk category.", "source": "Holmes Engineering 2024, via WW 2025-05-10", "fmt": "bool"},
    "urm_retrofit_cost_usd": {"label": "URM retrofit $", "desc": "Estimated cost to retrofit this URM building (only populated for URM schools).", "source": "Holmes Engineering 2024", "fmt": "usd"},
    "seismic_retrofit_status": {"label": "Seismic retrofit", "desc": "'full' = full modernization, 'targeted' = roof/partial retrofit done, 'planned_*' = 2025 bond scheduled, blank = none.", "source": "PPS bond.pps.net/seismic-improvements", "fmt": "text"},
    "is_title_i": {"label": "Title I?", "desc": "Receives federal Title I-A schoolwide funding (high-poverty designation).", "source": "PPS Funded Programs 2025-26", "fmt": "bool"},
    "pct_ela_prof_2425": {"label": "% ELA prof 24-25", "desc": "Percent of students meeting/exceeding on Oregon ELA state test (all grades, all students).", "source": "Oregon OSAS 2024-25", "fmt": "pct_0_100"},
    "pct_math_prof_2425": {"label": "% Math prof 24-25", "desc": "Percent of students meeting/exceeding on Oregon Math state test (all grades, all students).", "source": "Oregon OSAS 2024-25", "fmt": "pct_0_100"},
    "pct_ela_prof_2324": {"label": "% ELA prof 23-24", "desc": "Prior-year ELA proficiency.", "source": "Oregon OSAS 2023-24", "fmt": "pct_0_100"},
    "pct_math_prof_2324": {"label": "% Math prof 23-24", "desc": "Prior-year Math proficiency.", "source": "Oregon OSAS 2023-24", "fmt": "pct_0_100"},
    "crdc_chronic_absent_2020": {"label": "Chronic absent (CRDC)", "desc": "Count of students chronically absent (missed ≥15 days). Based on 2020 COVID year — likely suppressed.", "source": "US Dept of Ed CRDC 2020", "fmt": "int"},
    "crdc_lep_2020": {"label": "LEP students", "desc": "English Learner (Limited English Proficient) count.", "source": "US Dept of Ed CRDC 2020", "fmt": "int"},
    "crdc_idea_2020": {"label": "IDEA (SPED) students", "desc": "Students on IEPs under IDEA (special education).", "source": "US Dept of Ed CRDC 2020", "fmt": "int"},
    "frl_free_lunch": {"label": "Free lunch (count)", "desc": "Students eligible for free meals — raw count.", "source": "NCES CCD 2022", "fmt": "int"},
    "frl_reduced_lunch": {"label": "Reduced lunch (count)", "desc": "Students eligible for reduced-price meals — raw count.", "source": "NCES CCD 2022", "fmt": "int"},
    "pct_free_lunch": {"label": "% Free lunch", "desc": "Share of students on free meals (free ÷ 2022 enrollment) — proxy for poverty.", "source": "Derived: NCES CCD 2022", "fmt": "pct_0_1"},
    "pct_frl": {"label": "% FRL", "desc": "Share of students eligible for free OR reduced-price meals — standard poverty proxy.", "source": "Derived: NCES CCD 2022", "fmt": "pct_0_1"},
    "pct_direct_cert": {"label": "% Direct cert", "desc": "Share directly certified for free meals via SNAP/TANF/foster — proxy for deep poverty.", "source": "Derived: NCES CCD 2022", "fmt": "pct_0_1"},
    "pct_lep": {"label": "% English learners", "desc": "Share of students classified LEP.", "source": "Derived: CRDC 2020 ÷ current enrollment", "fmt": "pct_0_1"},
    "pct_idea": {"label": "% SPED (IDEA)", "desc": "Share of students on IEPs.", "source": "Derived: CRDC 2020 ÷ current enrollment", "fmt": "pct_0_1"},
    "pct_asian": {"label": "% Asian", "desc": "Share of students identifying as Asian, 2025-26.", "source": "Oregon ODE", "fmt": "pct_0_1"},
    "pct_black": {"label": "% Black", "desc": "Share of students identifying as Black/African American, 2025-26.", "source": "Oregon ODE", "fmt": "pct_0_1"},
    "pct_hispanic": {"label": "% Hispanic", "desc": "Share of students identifying as Hispanic/Latino, 2025-26.", "source": "Oregon ODE", "fmt": "pct_0_1"},
    "pct_white": {"label": "% White", "desc": "Share of students identifying as White, 2025-26.", "source": "Oregon ODE", "fmt": "pct_0_1"},
    "pct_multiracial": {"label": "% Multiracial", "desc": "Share identifying as two or more races, 2025-26.", "source": "Oregon ODE", "fmt": "pct_0_1"},
    "pct_bipoc": {"label": "% BIPOC", "desc": "Share of students identifying as any race/ethnicity other than White (1 − % White), 2025-26.", "source": "Derived from Oregon ODE", "fmt": "pct_0_1"},
    "affordable_units_within_1mi": {"label": "Afford. units in catchment", "desc": "Total existing affordable housing units inside the school's PPS attendance area (or a 1-mile radius for schools without a published catchment).", "source": "OAHI + Metro RLIS", "fmt": "int"},
    "pipeline_affordable_units_within_1mi": {"label": "Pipeline afford. units", "desc": "Affordable units in projects currently in development inside the school's PPS attendance area (2023–2027).", "source": "OAHI", "fmt": "int"},
    "pipeline_family_units_within_1mi": {"label": "Pipeline family units", "desc": "2+BR pipeline units inside the school's PPS attendance area — proxy for future families with kids.", "source": "OAHI", "fmt": "int"},
    "n_pipeline_projects_within_1mi": {"label": "Pipeline projects", "desc": "Number of affordable housing projects in development inside the school's PPS attendance area.", "source": "OAHI", "fmt": "int"},
    "permits_units_within_1mi_since_2022": {"label": "Permitted units (2022+)", "desc": "New residential units on building permits issued since 2022-01-01 inside the school's PPS attendance area — single-family, ADUs, and multifamily (all tenures). Schools without a published catchment fall back to a 1-mile radius.", "source": "Portland BDS via PortlandMaps", "fmt": "int"},
    "n_permits_within_1mi_since_2022": {"label": "Permits (2022+)", "desc": "Number of residential building permits issued since 2022-01-01 inside the school's PPS attendance area. Permits = approved to build; not all reach completion.", "source": "Portland BDS via PortlandMaps", "fmt": "int"},
    "street_address": {"label": "Address", "desc": "Street address of the building.", "source": "NCES CCD + manual", "fmt": "text"},
    "latitude": {"label": "Latitude", "desc": "Geocoded latitude.", "source": "NCES CCD", "fmt": "text"},
    "longitude": {"label": "Longitude", "desc": "Geocoded longitude.", "source": "NCES CCD", "fmt": "text"},
}

# Columns to surface in the default table (order matters).
TABLE_COLS = [
    "school_name", "level", "closure_rank",
    "enrollment_2025_26", "enrollment_pct_change", "students_per_sqft",
    "year_built", "square_feet", "pct_ela_prof_2425", "pct_math_prof_2425",
    "is_urm_building", "seismic_retrofit_status", "is_title_i",
    "pipeline_family_units_within_1mi", "affordable_units_within_1mi",
    "permits_units_within_1mi_since_2022",
]

# Pre-defined scatter plots.
SCATTERS = [
    {
        "id": "enrollment_vs_sqft",
        "title": "Enrollment vs. building crowding",
        "x": "enrollment_2025_26",
        "y": "students_per_sqft",
        "subtitle": "Closure candidates cluster in the bottom-left (small + underused buildings).",
    },
    {
        "id": "math_vs_frl",
        "title": "Math proficiency vs. % low-income",
        "x": "pct_frl",
        "y": "pct_math_prof_2425",
        "subtitle": "Classic income-achievement gradient. Schools above the trend line outperform expectations; below, underperform.",
        "trendline": True,
    },
    {
        "id": "enrollment_trend_vs_permits",
        "title": "Enrollment change vs. nearby residential permits (2022+)",
        "x": "permits_units_within_1mi_since_2022",
        "y": "enrollment_pct_change",
        "subtitle": "Schools losing students while their neighborhood is actively being built warrant a closer look before closure.",
    },
    {
        "id": "year_vs_students",
        "title": "Year built vs. enrollment",
        "x": "year_built",
        "y": "enrollment_2025_26",
        "subtitle": "Many candidates are mid-century buildings with low current enrollment.",
    },
    {
        "id": "enrollment_vs_bipoc",
        "title": "Enrollment vs. % BIPOC students",
        "x": "enrollment_2025_26",
        "y": "pct_bipoc",
        "subtitle": "Do the 15 smallest-enrollment schools serve a disproportionately BIPOC student body compared with the rest of PPS?",
    },
    {
        "id": "urm_cost_vs_enrollment",
        "title": "URM retrofit cost vs. enrollment",
        "x": "enrollment_2025_26",
        "y": "urm_retrofit_cost_usd",
        "subtitle": "URM buildings only. The slope is cost-per-student to save; schools well above the trend are expensive to retrofit per retained seat.",
        "trendline": True,
    },
]


def clean_val(v):
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if pd.isna(v):
        return None
    return v


def main():
    df = pd.read_csv(MASTER)
    # PPS's closure announcement covers only elementary, K-8, middle, and
    # alternative schools (not high schools). Drop high schools from the
    # dashboard so every view reflects the in-scope set.
    df = df[df["level"] != "high"].reset_index(drop=True)
    df["pct_bipoc"] = 1 - df["pct_white"]
    schools = []
    for _, row in df.iterrows():
        schools.append({c: clean_val(row[c]) for c in df.columns})

    payload = {
        "schools": schools,
        "meta": META,
        "table_cols": TABLE_COLS,
        "scatters": SCATTERS,
        "n_schools": len(schools),
        "n_candidates": int(df["is_closure_candidate"].sum()),
    }
    OUT_DATA.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Wrote {len(schools)} schools to {OUT_DATA}")


if __name__ == "__main__":
    main()
