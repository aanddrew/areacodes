import pandas as pd
import sqlite3


# save to sqlite db
conn = sqlite3.connect('raw_mnemonic.db')

# https://postalpro.usps.com/ZIP_Locale_Detail
df_zip = pd.read_excel('ZIP_Locale_Detail.xls', dtype=str)
df_zip.to_sql('raw_zip_codes_usps', conn, if_exists='replace', index=False)
df_cities = pd.read_excel('simplemaps_worldcities_basicv1.77/worldcities.xlsx', dtype=str)
df_cities.to_sql('raw_cities_simplemaps', conn, if_exists='replace', index=False)

df_zip = pd.read_sql_query('SELECT * FROM raw_zip_codes_usps', conn)
conn.close()

