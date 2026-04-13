import csv
from collections import defaultdict
p = '/Users/meuba/Code/school-research/data/raw/psu_prc_forecasts.csv'
rows = list(csv.DictReader(open(p)))
print('total rows:', len(rows))
# School-age bands (5-9, 10-14, 15-19) totals for Washington in 2025, 2035, 2045
agg = defaultdict(float)
for r in rows:
    if r['area_type']=='county' and r['age_group'] in ('5-9','10-14','15-19') and int(r['year']) in (2025,2035,2045):
        agg[(r['county'], int(r['year']), r['age_group'])] += float(r['total_population'])
for k in sorted(agg):
    print(k, round(agg[k],1))

# UGB sample: Sandy
print('\nSandy UGB (2025-2045):')
for r in rows:
    if r['area_name']=='Sandy' and r['area_type']=='ugb_sub_metro' and 2025<=int(r['year'])<=2045:
        print(' ', r['year'], round(float(r['total_population']),1))

# Unique area_names
names = sorted({(r['county'], r['area_type'], r['area_name']) for r in rows})
print('\nArea list:')
for n in names:
    print(' ', n)
