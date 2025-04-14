import pandas as pd
import numpy as np
import re
import sqlite3
import itertools as it
from typing import List
from enum import Enum

con = sqlite3.connect("words.db")

route_df = pd.read_csv('manual_route_raw.csv')
route_graph = {}
lines = []
for row in route_df.iterrows():
    points = row[1]['WKT'].split(' ')[1:5]
    points = [point.replace('(', '').replace(',', '').replace(')', '') for point in points]
    points = [float(point) for point in points]
    lines.append([np.array([points[0], points[1]]), np.array([points[2], points[3]])])

# validate and bring points into the correct order
current_point = lines[0][0]

path = [current_point]
done_traversing = False
while not done_traversing:
    done_traversing = True
    i = 0
    while i < len(lines):
        line = lines[i]
        next_point = None
        if np.linalg.norm(line[0] - current_point) < 0.01:
            next_point = line[1]
        elif np.linalg.norm(line[1] - current_point) < 0.01:
            next_point = line[0]

        if next_point is not None:
            del lines[i]
            path.append(current_point)
            current_point = next_point
            done_traversing = False
        i += 1

    lines.sort(key=lambda x: x[0][0])
    if done_traversing:
        for line in lines:
            print(line)

print(path[-1])
print(len(path))

# words_df.to_sql('words', con, if_exists="replace", index=False)
