
import time
import os
import pandas as pd
import tushare as ts

from nature import Book



dss = 'ini\\'
b1 = Book(dss,'hold_security.csv')

b1.pandian()
