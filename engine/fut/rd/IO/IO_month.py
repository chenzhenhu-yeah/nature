import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import zipfile
import os

from nature import get_dss


year = '2020'
zf = get_dss() + 'backtest/IO/' + year + '/202004.zip'

with zipfile.ZipFile(zf) as z:
    file_list = sorted( z.namelist() )
    # print(file_list)
    for fn in file_list:  
         f = z.open(fn)
         print(fn)

         df = pd.read_csv(f, encoding='gbk', error_bad_lines = False)
         df = df[df['合约代码'].str.startswith('IO')]
         df = df.rename(columns={'合约代码':'symbol', '今开盘':'open',
                                 '最高价':'high', '最低价':'low',
                                 '成交量':'volume', '成交金额':'amount',
                                 '持仓量':'hold', '今收盘':'close',
                                 '今结算':'settle','涨跌1':'change1',
                                 '涨跌2':'change2','Delta':'delta'})

         date = fn[:8]
         date = date[:4] + '-' + date[4:6] +  '-' + date[6:8]
         # print(date)
         df['date'] = date
         # df['symbol'] = df['symbol'].str.slice(0,13)
         df['symbol'] = df['symbol'].str.strip()
         df = df.loc[:,['date','symbol','open','high','low','close','volume','amount','hold','settle','change1','change2','delta']]

         # print(df.head())
         filename = get_dss() + 'backtest/IO/' + 'IO' + year + '.csv'
         if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
         else:
            df.to_csv(filename, index=False)

         # break
