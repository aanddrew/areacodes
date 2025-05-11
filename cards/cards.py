from PIL import Image, ImageDraw, ImageFont 
import pandas as pd

DPI = 200
FONT_SIZE = int(DPI / 5)
# MTG card dimensions
WIDTH_INCHES = 2.5
HEIGHT_INCHES = 3.5

def in_to_px(inches):
    return int(inches * DPI)

CENTER_X = int(DPI * WIDTH_INCHES / 2)
CENTER_Y = int(DPI * HEIGHT_INCHES / 2)

cards = pd.read_csv("cards.csv").to_dict('records')
print(cards)
    
font = ImageFont.truetype('Comic Sans MS.ttf', FONT_SIZE)

for card in cards:
    img = Image.new('RGB', (in_to_px(WIDTH_INCHES), in_to_px(HEIGHT_INCHES)), color='white')
    d = ImageDraw.Draw(img)
    title = card['Title']
    d.text((CENTER_X, CENTER_Y), title, fill='black', font=font, anchor='mm')

    img.save(f"output/{card['Card Shape Number']}.jpg")