import openpyxl
p = '/Users/meuba/Code/school-research/data/raw/psu_prc/age_sex_county_forecasts.xlsx'
wb = openpyxl.load_workbook(p, data_only=True)
ws = wb['age_specific_combined_results']
counties = set()
for row in ws.iter_rows(values_only=True):
    if row[1]:
        counties.add(row[1])
print(sorted(counties))
