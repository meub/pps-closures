import pdfplumber
p = '/Users/meuba/Code/school-research/data/raw/psu_prc/multnomah_county_report_2024.pdf'
with pdfplumber.open(p) as pdf:
    print('pages:', len(pdf.pages))
    for i, page in enumerate(pdf.pages):
        text = page.extract_text() or ''
        if any(k in text for k in ('Age','AGE','0-4','5-9','15-19','Forecast','Population','Total','Year')):
            print(f'\n=== page {i} ===')
            print(text[:2000])
