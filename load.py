import pandas as pd
import re
import sqlite3
import itertools as it

con = sqlite3.connect("words.db")

words_df = pd.read_csv('unigram_freq.csv')
words_df.to_sql('words', con, if_exists="replace", index=False)

areacodes_df = pd.read_csv('AreaCodes.csv')
areacodes_df.to_sql('area_codes', con, if_exists="replace", index=False)

cities_df = pd.read_csv('simplemaps_worldcities_basicv1.77/worldcities.csv')
cities_df.to_sql('cities', con, if_exists="replace", index=False)

digraphs = [
    {'character': '#', 'digraph': 'th'},
    {'character': '(', 'digraph': 'ch'},
    {'character': '$', 'digraph': 'sh'},
    {'character': '!', 'digraph': 'ck'},
    {'character': '@', 'digraph': 'ph'},
]

digit_letters = [
    {'digit': '0', 'letter': 's'},
    {'digit': '0', 'letter': 'z'},
    {'digit': '1', 'letter': 't'},
    {'digit': '1', 'letter': 'd'},
    {'digit': '1', 'letter': '#'},
    {'digit': '2', 'letter': 'n'},
    {'digit': '3', 'letter': 'm'},
    {'digit': '4', 'letter': 'r'},
    {'digit': '5', 'letter': 'l'},
    {'digit': '6', 'letter': 'j'},
    {'digit': '6', 'letter': '('},
    {'digit': '6', 'letter': '$'},
    {'digit': '7', 'letter': 'c'},
    {'digit': '7', 'letter': 'k'},
    {'digit': '7', 'letter': 'g'},
    {'digit': '7', 'letter': 'q'},
    {'digit': '7', 'letter': '!'},
    {'digit': '8', 'letter': 'v'},
    {'digit': '8', 'letter': 'f'},
    {'digit': '8', 'letter': '@'},
    {'digit': '9', 'letter': 'p'},
    {'digit': '9', 'letter': 'b'},
]

digit_df = pd.DataFrame(digit_letters)
digit_df.to_sql('digit_letters', con, if_exists="replace", index=False)

df = pd.read_sql("SELECT word, count FROM words;", con)
def clean_word(row):
    if row['word'] is None:
        return ''

    no_vowels = re.sub('[aeiouwhyx]', '', row['word'])
    no_dupes = ''
    i = 0
    while i < len(no_vowels):
        current_letter = no_vowels[i]
        while i < len(no_vowels) and no_vowels[i] == current_letter:
            i += 1
        no_dupes += current_letter
    
    for digraph in digraphs:
        no_dupes = no_dupes.replace(digraph['digraph'], digraph['character'])
    return no_dupes

df['cleaned'] = df.apply(clean_word, axis=1)
df.to_sql('words', con, if_exists="replace", index=False)
print(df)

def get_digit_letters(digit):
    letters = []
    for dl in digit_letters:
        if dl['digit'] == digit:
            letters.append(dl['letter'])
    return letters

def number_to_phonics(number, prefixes=[]):
    number = str(number)
    if len(number) == 0:
        return ['']
    elif len(number) == 1:
        return get_digit_letters(number[0])

    digit = number[0]
    prefixes = get_digit_letters(number[0])
    postfixes = number_to_phonics(number[1:], prefixes)
    possibilities = []
    for prefix in prefixes:
        for postfix in postfixes:
            possibilities.append(prefix + postfix)

    return possibilities

def get_area_code_phonics(row):
    return number_to_phonics(row['area_code'])

df_area_codes = pd.read_sql("SELECT area_code FROM area_codes;", con)
df_area_codes['phonics'] = df_area_codes.apply(get_area_code_phonics, axis=1)
df_area_codes = df_area_codes.explode('phonics')
df_area_codes.to_sql('area_code_phonics', con, if_exists="replace", index=False)

# print(pd.read_sql("SELECT COUNT(*) from words WHERE count > 1000000;", con))

df_area_code_cities = pd.read_sql(
"""
    SELECT area_code, state, main_city, c.lat, c.lng
    FROM area_codes ac
    LEFT JOIN cities c ON ac.main_city = c.city_ascii AND c.admin_name = ac.state
    WHERE AC.STATE NOT IN (
    'Trinidad and Tobago', 
    'Saint Kitts and Nevis', 
    'Jamaica', 
    'Toll free', 
    'Grenada', 
    'Guam', 
    'Montserrat', 
    'Dominican Republic',
    'Anguilla',
    'Antigua and Barbuda',
    'American Samoa',
    'Barbados',
    'Bermuda',
    'British Virgin Islands',
    'Cayman Islands',
    'Dominica',
    'Bahamas',
    'Yukon-Northwest Territories-Nunavut',
    'Turks and Caicos',
    'US Virgin Islands',
    'Nova Scotia-Prince Edward Island',
    'Newfoundland and Labrador',
    'Sint Maarten',
    'Saint Vincent',
    'Saint Lucia',
    'Northern Mariana'
    );
""", con)

regions = [
    {
        'region_name': 'socal', 
        'min_lat': 31.102211,
        'min_lng': -122.5721339,
        'max_lat': 35.655639, 
        'max_lng': -115.732556
    }
]

df_area_code_cities.to_sql('area_code_cities', con, if_exists="replace", index=False)

df_easy_words = pd.read_sql(
"""
    SELECT a.area_code, a.phonics, w.word FROM area_code_phonics a 
        LEFT JOIN words w on w.cleaned = a.phonics
    WHERE w.count IS NULL OR w.count > 1000000
    ORDER BY a.area_code, w.word;
""", con)
df_easy_words.to_sql('single_words', con, if_exists="replace", index=False)

def get_parts(string):
    if len(string) > 3:
        return None
    elif len(string) == 3:
        return [
            string,
            string[0:1] + ' ' + string[1:3],
            string[0:2] + ' ' + string[2:3],
            string[0] + ' '  + string[1] + ' ' + string[2]
        ]
    elif len(string) == 2:
        return [
            string,
            string[0] + ' ' + string[1]
        ]
    else:
        return [string]
    
def get_parts_from_row(row):
    return get_parts(row['phonics'])
    
df_area_code_phonics_parts = pd.read_sql("SELECT * FROM area_code_phonics;", con)
df_area_code_phonics_parts['parts'] = df_area_code_phonics_parts.apply(get_parts_from_row, axis=1)
df_area_code_phonics_parts = df_area_code_phonics_parts.explode('parts')

print(df_area_code_phonics_parts)

def get_words_from_parts(parts):
    part_words = []
    for part in parts.split(' '):
        words = pd.read_sql(
            f"""
                SELECT word 
                FROM words 
                WHERE (LEN(phonics) < 3 AND COUNT > 100000000) OR COUNT > 100000
                    AND cleaned = '{part}';
            """,
            con
        )['word'].tolist()
        part_words.append(words)

    return part_words

def get_words_from_parts_from_row(row):
    part_words = get_words_from_parts(row['parts'])

    # if we couldn't find a word for one of the parts it's not viable
    for pw in part_words:
        if len(pw) == 0:
            return []
        
    prod = list(it.product(*part_words))
    return [" ".join(words) for words in prod]

# uncomment to do calculation
# df_area_code_phonics_parts = df_area_code_phonics_parts.head(100)
# df_area_code_phonics_parts['words'] = df_area_code_phonics_parts.apply(get_words_from_parts_from_row, axis=1)
# df_area_code_phonics_parts = df_area_code_phonics_parts.explode('words')
# df_area_code_phonics_parts.to_sql('area_code_words', con, if_exists="replace", index=False)

# print(df_area_code_phonics_parts)



con.close()