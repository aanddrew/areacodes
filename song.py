import pandas as pd
import numpy as np
import sqlite3
import itertools as it
from typing import List
from enum import Enum

con = sqlite3.connect("raw/words.db")

codewords = []

with open("mnemofy/code_words.txt", "r") as file:
    lines = file.readlines()[1:]
    codewords = [(line.split(' ')[0], line.split(' ')[1].replace('\n', '')) for line in lines]

current_code = codewords[0][0]
words = [codewords[0][1]]

all_codes = [line.replace('\n', '') for line in open('codes_lines.txt', 'r').readlines()]
print(all_codes)

# words I don't like :3
bad_words = [
    'algeria', 
    'lasmo', 
    'alsop', 'lusby', 'lissouba',
    'povich',

    'wigfall', 'wigfall', 'oakville', 'kvale', 'kuffel', 'kufahl', 'koval', 'kivela', 'keville', 'keevil', 'iacovelli',

    'muffley',
    'bonifay',
    'pamacho'
]

# replace words manually
replace_words = {
    'gelco': 'chilling',
    'kangyo': 'coinage',
    'wojnilower': 'nailer',
    'allegheny': 'lagoon',
    'momokawa': 'mimic',
    'nunnelly': 'nunley',
    'shyjan': 'shoeshine',
    'bigelow': 'buckle',
    'lilco': 'lilac',
    'rourke': 'roaring',
    'kuchma': 'ketchum',
    'geodyne': 'showdown',
    'negro': 'inquiry',
    'kawakami': 'kagami',
    'lovejoy': 'lavish',
    'oldham': 'lithium',
    'windish': 'nitzsche',
    'nomura': 'anymore',
    'wagner': 'ignore',
    'mambo': 'mumbo',
    'chisholm': 'chasm',
    'olthoff': 'leadoff',
    'minebea': 'manweb',
    'schmidt': 'ashamed',
    'benbow': 'bonobo',
    'hanson': 'insane',
    'nolan': 'nylon',
    'petipa': 'potpie',
    'chatwal': 'shuttle',
    'kasich': 'gossage',
    'rykoff': 'rockoff',
    'rosario': 'razor',
    'checkoff': 'shockwave',
    'chalabi': 'shelby',
    'maffucci': 'haimovitch',
    'namibia': 'nimby',
    'kokan': 'quicken',
    'kvetch': 'kovich'
}

no_codes = {}

prev_code = ""
for code in all_codes:
    found_code = False
    for (code_in, word) in codewords: 
        if code_in == code:
            found_code = True
            break
    if not found_code:
        no_codes[prev_code] = code
        print(f"No word found for code: {code}, previous_code = {prev_code}")

    prev_code = code

print(no_codes)

if '' in no_codes.keys():
    words = [no_codes['']] + words
for (code, word) in codewords:
    if code != current_code and word not in bad_words:
        if word in replace_words.keys():
            words.append(replace_words[word])
        else: 
            words.append(word)

        if code in no_codes.keys():
            words.append(no_codes[code])

        current_code = code

print(f"All codes has len {len(all_codes)}")
print(f"Word list has len = {len(words)}")



song_code_words = []
i = 0
for (code, word) in zip(all_codes, words):
    song_code_words.append({
        'song_index': i,
        'area_code': code,
        'word': word
    })
    i += 1

song_df = pd.DataFrame(song_code_words)
song_df.to_sql('song_better', con, index=False, if_exists='replace')

cities_list = pd.read_sql(
"""
    SELECT sb.song_index / 16 + 1 as song_index, main_city 
    FROM song_better sb
    LEFT JOIN area_code_cities acc ON acc.area_code = sb.area_code
    WHERE sb.song_index % 16 == 0
    ORDER BY sb.song_index;
""", con).values.tolist()
# print(cities_list.values.tolist())

with open("code_words.txt", "w") as file:
    for cw in song_code_words:
        code = cw['area_code']
        word = cw['word']
        file.write(f"{code} {word}\n")

with open("cities_list.txt", "w") as file:
    for city_list in cities_list:
        file.write(f"{city_list[0]} {city_list[1]}\n")

# file.write(f"{cities_list[int(i / 16)][1]} ({int(i/16) + 1})\n")
with open("song_better.txt", "w") as file:
    i = 0
    for word in words:
        if i % 16 == 0 and i != 0:
            file.write('\n\n')
        elif i % 8 == 0:
            file.write('\n')
        else:
            file.write(' ')
        file.write(word)
        i += 1
