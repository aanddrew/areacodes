import pandas as pd
import sqlite3



files_to_load = [
    {
        'file': 'raw_zip_codes_usps',
        'file': 'raw/ZIP_Locale_Detail.xls',
        'table': 'raw_zip_codes_usps',
    },
    {
        'file': 'raw_cities_simplemaps',
        'file': 'raw/simplemaps_worldcities_basicv1.77/worldcities.xlsx',
        'table': 'raw_cities_simplemaps',
    }
]

conn = sqlite3.connect('mnemono.db')
for file in files_to_load:
    df = None
    if file['file'].endswith('.xls'):
        df = pd.read_excel(file['file'], dtype=str)
    elif file['file'].endswith('.xlsx'):
        df = pd.read_excel(file['file'], dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {file['file']}")

    if df is not None:
        df.to_sql(file['table'], conn, if_exists='replace', index=False)


conn.close()