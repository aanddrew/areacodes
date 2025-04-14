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
print(route_graph)

done_traversing = False
while not done_traversing:
    connections = route_graph[current_point]
    done_traversing = True
    for point in connections:
        if point not in path:
            path.append(point)
            current_point = point
            done_traversing = False

print(path[-1])
print(len(path))

# words_df.to_sql('words', con, if_exists="replace", index=False)
