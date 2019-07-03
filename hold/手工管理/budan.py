
import time
import os
import pandas as pd
import tushare as ts

from nature import Book

ins_dict = {}
ins_dict_list = []

dss = '../../../data/'
b1 = Book(dss,'csv/hold_security.csv')
b1.deal_ins(...)
