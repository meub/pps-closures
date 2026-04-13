import zipfile, os
base = '/Users/meuba/Code/school-research/data/raw/psu_prc/'
for fn in ['multnomah_forecast_2024.xlsx','washington_forecast_2024.xlsx','clackamas_forecast_2024.xlsx','age_sex_county_forecasts.xlsx','ugb_forecasts_all_areas.xlsx']:
    p = base + fn
    sz = os.path.getsize(p)
    try:
        z = zipfile.ZipFile(p)
        print(fn, sz, 'OK xlsx, entries=', len(z.namelist()))
    except Exception as e:
        print(fn, sz, 'NOT xlsx:', e)
        with open(p, 'rb') as f:
            data = f.read(400)
            print('  head:', data[:400])
