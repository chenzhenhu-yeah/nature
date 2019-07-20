
import os
import pandas as pd
import tushare as ts

from nature import get_trading_dates

def down_daily_run(dss):
    listfile = os.listdir(dss + 'daily')
    listfile.sort(reverse=True)

    lastday = listfile[0][:10]
    print(lastday)
    dates = get_trading_dates(dss,lastday)
    dates.pop(0)                             #担心daily数据更新不及时
    #dates = ['2019-04-01']

    for today in dates:
        fss = dss + 'daily/' + today + '_stk_all.csv'
        try:
            if os.path.exists(fss):
                print('exist',today)
            else:
                df = ts.get_day_all(today)
                df.to_csv(fss,index=False,encoding='gbk')
                print('got ',today)
        except:
            print('error ',today)

if __name__ == '__main__':
    down_daily_run(r'../../data/')
