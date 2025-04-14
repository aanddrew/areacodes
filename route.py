import pandas as pd
import numpy as np
import re
import sqlite3
import itertools as it
from typing import List
from enum import Enum

con = sqlite3.connect("raw/words.db")

route_df = pd.read_csv('manual_route_raw.csv')
route_graph = {}
all_points = []
for row in route_df.iterrows():
    points = row[1]['WKT'].split(' ')[1:5]
    points = [point.replace('(', '').replace(',', '').replace(')', '') for point in points]
    points = [f"{points[0]} {points[1]}", f"{points[2]} {points[3]}"]

    for point in points:
        if point not in all_points:
            all_points.append(point)

    if points[0] in route_graph.keys():
        route_graph[points[0]].append(points[1])
    else:
        route_graph[points[0]] = [points[1]]

    if points[1] in route_graph.keys():
        route_graph[points[1]].append(points[0])
    else:
        route_graph[points[1]] = [points[0]]

# validate and bring points into the correct order
current_point = all_points[0]
path = [current_point]

done_traversing = False
while not done_traversing:
    connections = route_graph[current_point]
    done_traversing = True
    for point in connections:
        if point not in path:
            path.append(point)
            current_point = point
            done_traversing = False

print(f"Path contains {len(path)} points")

def get_lng(row):
    return row['point'].split(' ')[0]
def get_lat(row):
    return row['point'].split(' ')[1]

path_frame = pd.DataFrame(path)
path_frame.columns = ['point']
path_frame['lng'] = path_frame.apply(get_lng, axis=1)
path_frame['lat'] = path_frame.apply(get_lat, axis=1)
path_frame.to_sql("route_raw", con, if_exists='replace', index_label='route_index')

df_area_code_words = pd.read_sql(
    """
    SELECT * FROM area_code_words
    WHERE LENGTH(words) - LENGTH(REPLACE(words, ' ', '')) < 2;
    """, con)

df_area_code_words['word_list'] = df_area_code_words['words'].str.split(' ')

df_song_words = pd.DataFrame(df_area_code_words['word_list'].explode().unique())
df_song_words.columns = ['word']
df_words = pd.read_sql("SELECT * FROM words", con)

df_song_words = df_song_words.merge(df_words, on='word', how='left')
# df_song_words.to_csv('df_song_words.csv')

def get_word_score(row):
    if row['word_list'] is None:
        return -1
    
    total_count = 0
    for word in row['word_list']:
        count = df_song_words[df_song_words['word'] == 'news'].iloc[0]['count']
        total_count += count
    return total_count

df_area_code_words['word_score'] = df_area_code_words.apply(get_word_score, axis=1)
df_area_code_words = df_area_code_words.drop('word_list', axis=1)
df_area_code_words.to_sql("area_code_words_scored", con, if_exists='replace', index=False)

ranked_df = None

path_numbers = {}
for point in path:
    lng, lat = point.split(' ')

    city_words = pd.read_sql(
    f"""
        WITH ALL_WORDS AS (
        SELECT 
            acc.area_code,
            acws.words,
            acws.word_score as words_score
        FROM area_code_cities acc
        LEFT JOIN area_code_words_scored acws ON acc.area_code = acws.area_code
        WHERE acc.lat = {lat} AND acc.lng = {lng}
        )
        SELECT *, ROW_NUMBER() OVER (PARTITION BY area_code ORDER BY words_score) AS words_rank
        FROM ALL_WORDS
    """, con)
    if len(city_words) == 0:
        print(f"CITY NOT FOUND AT lat: {lat}, lng: {lng}")
    else:
        if ranked_df is None:
            ranked_df = city_words
        else:
            ranked_df = pd.concat([ranked_df, city_words])

# print(ranked_df)
ranked_df.to_sql('area_code_words_ranked', con, if_exists='replace', index=False)
# words_df.to_sql('words', con, if_exists="replace", index=False)

song_df = pd.read_sql(
"""
    SELECT 
        r.route_index,
        acc.state,
        acc.main_city,
        acc.area_code,
        CASE 
            WHEN acwr.words IS NULL THEN '(' || acc.area_code || ')'
            ELSE acwr.words
        END AS words
    FROM route_raw r
    LEFT JOIN area_code_cities acc ON acc.lat = r.lat AND acc.lng = r.lng
    LEFT JOIN area_code_words_ranked acwr ON acwr.area_code = acc.area_code AND acwr.words_rank = 1;
""", con)
song_df.to_sql("song", con, if_exists='replace', index=False)

song_words = song_df['words'].to_list()
print(song_words)
with open("song.txt", "w") as file:
    i = 1
    for word in song_words:
        file.write(word)
        if i % 16 == 0:
            file.write('\n\n')
        else:
            file.write(', ')
        i += 1

con.close()