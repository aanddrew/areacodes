import pandas as pd
import numpy as np
import re
import sqlite3
import itertools as it
from typing import List
from enum import Enum

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

# pd.read_sql("""
# -- DELETE FROM area_code_words WHERE LENGTH(parts) == 3 AND words IS NULL;
# 
# -- WITH three_letter_words AS (
# --   SELECT acw.area_code, acw.phonics, acw.parts, w.word AS words 
# --   FROM area_code_words acw
# --   LEFT JOIN words w ON acw.phonics = w.cleaned
# --   WHERE 
# --     LENGTH(acw.parts) = 3
# --     AND w.count > 2000000
# -- )
# -- INSERT INTO area_code_words
# -- SELECT * FROM three_letter_words;
# """, con)


rect_regions = [
    {
        'name': 'socal', 
        'min_lat': 31.102211,
        'min_lng': -122.5721339,
        'max_lat': 35.655639, 
        'max_lng': -115.732556
    }
]

def get_region(row):
    for region in rect_regions: 
        if (row['lat'] >= region['min_lat'] and
            row['lat'] <= region['max_lat'] and
            row['lng'] >= region['min_lng'] and
            row['lng'] <= region['max_lng']):
            return region['name']
    return ''

df_area_code_cities = pd.read_sql("SELECT * FROM area_code_cities", con)
df_area_code_cities['region'] = df_area_code_cities.apply(get_region, axis=1)
df_area_code_cities.to_sql('area_code_cities', con, if_exists='replace', index=False)
# print(df_area_code_cities)

df_points = pd.read_sql("""
    SELECT DISTINCT 
        state, 
        main_city, 
        lat,
        lng 
    FROM area_code_cities 
    WHERE lat BETWEEN 23.891128 AND 49.0490684
      AND lng BETWEEN -127.079265 AND -61.470050
      AND state NOT IN ('Ontario', 'Quebec')
    ORDER BY state, main_city
""", con)
df_points.to_csv('points.csv', index=False)

class Node:
    x: float
    y: float
    name: str

    def __init__(self, x: float, y: float, name: str = ""):
        self.x = x
        self.y = y
        self.name = name
    
    def __str__(self):
        return f"{self.name, self.x, self.y}"

class Box:
    left: float
    right: float
    bot: float
    top: float

    def __init__(self, left: float, right: float, bot: float, top: float):
        self.left = left
        self.right = right
        self.bot = bot
        self.top = top

    def __str__(self):
        return f"(l: {self.left:.7f}, r: {self.right:.7f}, b: {self.bot:.7f}, t: {self.top:.7f}"
    
class KDTreeBox:
    nodes: List[Node]
    box: Box

    def __init__(self, nodes: List[Node], box: Box):
        self.nodes = nodes
        self.box = box

    def __str__(self):
        return f"{self.nodes}, {self.box}"

class Axis(Enum):
    X = 'x',
    Y = 'y'

class KDTree:
    children: List["KDTree"]
    box: KDTreeBox
    split_axis: Axis

    def __init__(self, children: List["KDTree"], box: KDTreeBox, split_axis: Axis):
        self.box = box
        self.children = children
        self.split_axis = split_axis

    def is_leaf(self) -> bool:
        return len(self.children) == 0

city_nodes: List[Node] = []
for index, row in df_points.iterrows():
    city_nodes.append(Node(row['lng'], row['lat'], row['main_city']))

def split(nodes: List[Node], bounding_box: Box, axis: Axis=Axis.Y) -> List[KDTreeBox]:
    def get_x(node: Node):
        return node.x
    def get_y(node: Node):
        return node.y

    get_val = get_x
    if axis == Axis.Y:
        get_val = get_y

    min_val = np.min([get_val(node) for node in nodes])
    max_val = np.max([get_val(node) for node in nodes])
    split_val = (min_val + max_val) / 2

    left = []
    right = []
    even_split = False
    while not even_split:
        left  = [node for node in nodes if get_val(node) < split_val]
        right = [node for node in nodes if get_val(node) >= split_val]

        # we can have up to 2 more in either side :shrug:
        if abs(len(left) - len(right)) < 2:
            even_split = True
        else:
            if len(left) > len(right):
                max_val = split_val
                split_val = (min_val + split_val) / 2
            else:
                min_val = split_val
                split_val = (max_val + split_val) / 2
    
    min_x = bounding_box.left
    max_x = bounding_box.right
    min_y = bounding_box.bot
    max_y = bounding_box.top
    if axis == Axis.X:
        left_box  = KDTreeBox(left,  Box(min_x, split_val, min_y, max_y))
        right_box = KDTreeBox(right, Box(split_val, max_x, min_y, max_y))
        return [left_box, right_box]
    elif axis == Axis.Y:
        bot_box = KDTreeBox(left,  Box(min_x, max_x, min_y, split_val))
        top_box = KDTreeBox(right, Box(min_x, max_x, split_val, max_y))
        return [bot_box, top_box]

def tree_build(nodes: List[Node], bounding_box: Box, split_axis: Axis = Axis.X) -> KDTree:
    if len(nodes) == 1:
        return KDTree([], KDTreeBox([nodes[0]], bounding_box), split_axis)

    halves = split(nodes, bounding_box, split_axis)
    new_split_axis = split_axis
    if split_axis == Axis.X:
        new_split_axis = Axis.Y
    elif split_axis == Axis.Y:
        new_split_axis = Axis.X

    half_trees = [
        tree_build(halves[0].nodes, halves[0].box, new_split_axis),
        tree_build(halves[1].nodes, halves[1].box, new_split_axis)
    ]

    return KDTree(children=half_trees, box=KDTreeBox(nodes, bounding_box), split_axis=split_axis)

min_x = np.min([node.x for node in city_nodes])
max_x = np.max([node.x for node in city_nodes])
min_y = np.min([node.y for node in city_nodes])
max_y = np.max([node.y for node in city_nodes])
tree = tree_build(city_nodes, Box(min_x, max_x, min_y, max_y), split_axis=Axis.X)

def pre_order_traverse(root: KDTree, level) -> List[KDTreeBox]:
    if root.is_leaf():
        return [root.box]
    
    check_tree = root
    check_level = level
    while check_level > 0 and not check_tree.is_leaf():
        check_tree = check_tree.children[0]
        check_level -= 1

    if check_tree.is_leaf():
        return [root.box]

    boxes = [] 
    for child in root.children:
        boxes += pre_order_traverse(child, level) 

    return boxes

for level in range(6):
    leaves = pre_order_traverse(tree, level)
    with open(f"polygons/polygons_level{level}.csv", "w", newline='') as file:
        file.write("WKT,name\n")
        for leaf in leaves:
            points_string = ""
            points_string += f"{leaf.box.left:.7f} {leaf.box.top:.7f}, "
            points_string += f"{leaf.box.left:.7f} {leaf.box.bot:.7f}, "
            points_string += f"{leaf.box.right:.7f} {leaf.box.bot:.7f}, "
            points_string += f"{leaf.box.right:.7f} {leaf.box.top:.7f}, "
            points_string += f"{leaf.box.left:.7f} {leaf.box.top:.7f}" # same as start

            polygon_string = f"\"POLYGON (({points_string}))\",\"{leaf.nodes[0].name}\"\n"
            file.write(polygon_string)
            file.write(f"\"POINT ({leaf.nodes[0].x:.7f} {leaf.nodes[0].y:.7f})\",\"{leaf.nodes[0].name} city\"\n")

class Direction(Enum):
    Left = 'left'
    Right = 'right'
    Up = 'up'
    Down = 'down'

def traverse(tree: KDTree, from_direction: Direction, to_direction: Direction) -> List[KDTreeBox]:
    if tree.is_leaf():
        return [tree.box]
    
    LEFT_INDEX = 0
    RIGHT_INDEX = 1
    BOT_INDEX = 0
    TOP_INDEX = 1

    first = []
    last = []

    if tree.split_axis == Axis.X:
        if from_direction == Direction.Left:
            first = traverse(tree.children[LEFT_INDEX], Direction.Left, Direction.Right)
            last  = traverse(tree.children[RIGHT_INDEX], Direction.Left, to_direction)
        elif from_direction == Direction.Right:
            first = traverse(tree.children[RIGHT_INDEX], Direction.Right, Direction.Left)
            last  = traverse(tree.children[LEFT_INDEX], Direction.Right, to_direction)
        elif from_direction == Direction.Down:
            first = traverse(tree.children[LEFT_INDEX], Direction.Down, Direction.Right)
            last  = traverse(tree.children[RIGHT_INDEX], Direction.Left, to_direction)
        elif from_direction == Direction.Up:
            first = traverse(tree.children[LEFT_INDEX], Direction.Up, Direction.Right)
            last  = traverse(tree.children[RIGHT_INDEX], Direction.Left, to_direction)
    if tree.split_axis == Axis.Y:
        if from_direction == Direction.Down:
            first = traverse(tree.children[BOT_INDEX], Direction.Down, Direction.Up)
            last  = traverse(tree.children[TOP_INDEX], Direction.Down, to_direction)
        elif from_direction == Direction.Up:
            first = traverse(tree.children[BOT_INDEX], Direction.Up, Direction.Down)
            last  = traverse(tree.children[TOP_INDEX], Direction.Up, to_direction)
        elif from_direction == Direction.Left:
            first = traverse(tree.children[TOP_INDEX], Direction.Left, Direction.Down)
            last  = traverse(tree.children[BOT_INDEX], Direction.Up, to_direction)
        elif from_direction == Direction.Right:
            first = traverse(tree.children[TOP_INDEX], Direction.Right, Direction.Down)
            last  = traverse(tree.children[BOT_INDEX], Direction.Up, to_direction)

    def score_4_points(points):
        vec1 = (points[0][0] - points[1][0], points[0][1] - points[1][1])
        vec2 = (points[2][0] - points[1][0], points[2][1] - points[1][1])

        vec3 = (points[1][0] - points[2][0], points[1][1] - points[2][1])
        vec4 = (points[3][0] - points[2][0], points[3][1] - points[2][1])

        dot1 = (vec1[0] * vec2[0]) + (vec1[1] * vec2[1])
        dot2 = (vec1[0] * vec2[0]) + (vec1[1] * vec2[1])

        return dot1 + dot2

    def rotate(route: List[KDTreeBox]):
        if len(route) >= 4:
            for i in range(1, len(route) - 2):
                points = [
                    [route[i-1].nodes[0].x, route[i-1].nodes[0].y],
                    [route[i].nodes[0].x, route[i].nodes[0].y],
                    [route[i+1].nodes[0].x, route[i+1].nodes[0].y],
                    [route[i+2].nodes[0].x, route[i+2].nodes[0].y]
                ]
                score_unswapped = score_4_points(points)

                temp_point = points[1]
                points[1] = points[2]
                points[2] = temp_point

                score_swapped = score_4_points(points)

                if score_swapped < score_unswapped:
                    temp_node = route[i]
                    route[i] = route[i + 1]
                    route[i + 1] = temp_node

    rotate(first)
    rotate(last)

    route = first + last + [first[0]]

    rotate(route)
    route = route[:-1]

    return route

route = traverse(tree, Direction.Left, Direction.Right)
with open("route.csv", "w", newline='') as file:
    file.write("WKT,name\n")
    for i in range(len(route) - 1):
        start = route[i]
        end = route[i + 1]
        file.write(f"\"LINESTRING ({start.nodes[0].x:.7f} {start.nodes[0].y:.7f}, {end.nodes[0].x:.7f} {end.nodes[0].y:.7f})\",\"{start.nodes[0].name}\"\n")

con.close()