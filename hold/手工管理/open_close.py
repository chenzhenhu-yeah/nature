
import time
import os
import pandas as pd
import tushare as ts

from nature import Book



dss = 'ini\\'
b1 = Book(dss,'hold_security.csv')


ins_dict = {'ins':'open_portfolio', 'portfolio':'overlap','code':'profit','agent':'pingan'}
#ins_dict = {'ins':'close_portfolio', 'portfolio':'overlap','agent':'pingan'}

b1.deal_ins(ins_dict)
