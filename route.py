import pandas as pd
import numpy as np
import re
import sqlite3
import itertools as it
from typing import List
from enum import Enum

con = sqlite3.connect("words.db")

