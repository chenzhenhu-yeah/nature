
import time
from datetime import datetime
import os
import pandas as pd
import tushare as ts

from nature import Book

ins_dict = {}
ins_dict_list = [
{'ins': 'sell_order', 'portfolio': '5G', 'code': '300461', 'num': 1300, 'price': 22.60, 'cost': 29380, 'agent': 'pingan', 'name': '田中精机'},
{'ins': 'sell_order', 'portfolio': '5G', 'code': '300036', 'num': 2000, 'price': 15.40, 'cost': 30800, 'agent': 'pingan', 'name': '超图软件'},
{'ins': 'buy_order', 'portfolio': 'overlap', 'code': '300514', 'num': 2000, 'price': 14.52, 'cost': 29040, 'agent': 'pingan', 'name': '友讯达'},
{'ins': 'sell_order', 'portfolio': 'overlap', 'code': '300514', 'num': 2000, 'price': 14.56, 'cost': 29120, 'agent': 'pingan', 'name': '友讯达'},
]


# now = datetime.now()
# today = now.strftime('%Y%m%d')
strdate = input('date(20190201):')


dss = 'ini\\'
ins = 'copy ' + dss + 'hold_security.csv ' + dss + 'hold_security.csv_' + strdate
os.system(ins)
print(ins)
#return
#
b1 = Book(dss,'hold_security.csv')

file_trade_record = 'log\\trade_record\\pingan.csv'
df = pd.read_csv(file_trade_record,dtype='str',sep='&')
df = df[df.date>=strdate]
#print(df['date'])

for i,row in df.iterrows():
    ins_dict = str(row.ins)
    print(ins_dict)
    if ins_dict == 'nan':
        pass
    else:
        ins_dict = eval(ins_dict)
        # print(type(ins_dict))
        # print(ins_dict)
        b1.deal_ins(ins_dict)
