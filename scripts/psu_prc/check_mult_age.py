import openpyxl
p = '/Users/meuba/Code/school-research/data/raw/psu_prc/age_sex_county_forecasts.xlsx'
wb = openpyxl.load_workbook(p, data_only=True)
ws = wb['age_specific_combined_results']
# Filter Multnomah 2020, 2025, 2045 Female age 5-9
seen_years_mult = set()
seen_years_clack = set()
seen_years_wash = set()
rows_found = 0
for row in ws.iter_rows(values_only=True):
    year, county, sex, age5, pop, source = row
    if county == 'Multnomah':
        seen_years_mult.add(year)
        if year in (2025, 2035, 2045) and age5 in ('5-9','10-14','15-19'):
            print(row)
            rows_found += 1
    elif county == 'Clackamas':
        seen_years_clack.add(year)
    elif county == 'Washington':
        seen_years_wash.add(year)
print('\nMultnomah years:', sorted(y for y in seen_years_mult if y is not None))
print('Clackamas years:', sorted(y for y in seen_years_clack if y is not None))
print('Washington years:', sorted(y for y in seen_years_wash if y is not None))
print('rows_found:', rows_found)
