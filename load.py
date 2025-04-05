import pandas as pd
import re
import sqlite3

con = sqlite3.connect("words.db")

df = pd.read_csv('unigram_freq.csv')
df.to_sql('words', con, if_exists="replace", index=False)

digit_letters = [
    {'digit': '0', 'letter': 's'},
    {'digit': '0', 'letter': 'z'},
    {'digit': '1', 'letter': 't'},
    {'digit': '1', 'letter': 'd'},
    {'digit': '1', 'letter': 'th'},
    {'digit': '2', 'letter': 'n'},
    {'digit': '3', 'letter': 'm'},
    {'digit': '4', 'letter': 'r'},
    {'digit': '5', 'letter': 'l'},
    {'digit': '6', 'letter': 'j'},
    {'digit': '6', 'letter': 'ch'},
    {'digit': '6', 'letter': 'sh'},
    {'digit': '7', 'letter': 'c'},
    {'digit': '7', 'letter': 'k'},
    {'digit': '7', 'letter': 'g'},
    {'digit': '7', 'letter': 'q'},
    {'digit': '7', 'letter': 'ck'},
    {'digit': '8', 'letter': 'v'},
    {'digit': '8', 'letter': 'f'},
    {'digit': '8', 'letter': 'ph'},
    {'digit': '9', 'letter': 'p'},
    {'digit': '9', 'letter': 'b'},
]

digit_df = pd.DataFrame(digit_letters)
digit_df.to_sql('digit_letters', con, if_exists="replace", index=False)

df = pd.read_sql("SELECT word FROM words;", con)
def clean_word(row):
    if row['word'] is None:
        return ''

    no_vowels = re.sub('[aeiou]', '', row['word'])
    no_dupes = ''
    i = 0
    while i < len(no_vowels):
        current_letter = no_vowels[i]
        while i < len(no_vowels) and no_vowels[i] == current_letter:
            i += 1
        no_dupes += current_letter
    return no_dupes

df['cleaned'] = df.apply(clean_word, axis=1)
print(df)

con.close()