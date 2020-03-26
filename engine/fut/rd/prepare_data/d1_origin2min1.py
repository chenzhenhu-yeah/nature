
import os
import time
import datetime
import json
import pandas as pd

from nature import get_dss

obj_dict ={
           'm01' : {'2018':['m1801','m1901'], '2019':['m1901','m2001'], },
           'm05' : {'2018':['m1805','m1905'], '2019':['m1905','m2005'], },
           'm09' : {'2018':['m1809','m1909'], '2019':['m1909','m2009'], },
           'y01' : {'2018':['y1801','y1901'], '2019':['y1901','y2001'], },
           'y05' : {'2018':['y1805','y1905'], '2019':['y1905','y2005'], },
           'y09' : {'2018':['y1809','y1909'], '2019':['y1909','y2009'], },
           'p01' : {'2018':['p1801','p1901'], '2019':['p1901','p2001'], },
           'p05' : {'2018':['p1805','p1905'], '2019':['p1905','p2005'], },
           'p09' : {'2018':['p1809','p1909'], '2019':['p1909','p2009'], },
          }


def cp(pz, obj, y, symbol):
    fn = "C:\\Users\\czh\\Documents\\critical\\data\\FutAC_Min1_Std_"+y+"\\" + symbol + '.csv'
    df = pd.read_csv(fn, skiprows=1, header=None, names=['market','symbol','datetime','open','high','low','close','volume','amout','hold'])
    df['date'] = df.datetime.str[:10]
    df['time'] = df.datetime.str[11:]
    df = df.loc[:,['symbol','date','time','open','high','low','close','volume','amout','hold']]
    print(df.head(3))

    fn_glue = get_dss() +'backtest/fut/'+  pz + '/' + obj + '.csv'
    if os.path.exists(fn_glue):
        df.to_csv(fn_glue, index=False, mode='a', header=None)
    else:
        df.to_csv(fn_glue, index=False)

if __name__ == "__main__":
    pz = 'p'
    obj = pz + '01'
    y_list = ['2018', '2019']

    d = obj_dict[obj]
    for y in y_list:
        for symbol in d[y]:
            cp(pz,obj,y,symbol)
