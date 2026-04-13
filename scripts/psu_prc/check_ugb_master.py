import openpyxl
p = '/Users/meuba/Code/school-research/data/raw/psu_prc/ugb_forecasts_all_areas.xlsx'
wb = openpyxl.load_workbook(p, data_only=True)
ws = wb['updated_combined_2024']
# Look for rows containing Multnomah/Clackamas/Washington or Portland/Metro
rows = list(ws.iter_rows(values_only=True))
print('header row 1:', rows[0])
print('header row 2:', rows[1])
print()
# Print all rows where col 2 (County) is Multnomah/Clackamas/Washington or contains Portland
target_counties = {'Multnomah County','Clackamas County','Washington County'}
count = 0
for row in rows[2:]:
    county = row[2]
    ugb = row[3]
    if county in target_counties or (county and 'Portland' in str(county)) or (ugb and 'Portland' in str(ugb)):
        count += 1
        print(row)
print('\nTotal matching rows:', count)

# Also look at unique counties in the sheet
counties = set()
for row in rows[2:]:
    if row[2]:
        counties.add(row[2])
print('\nAll counties present:', sorted(counties))
