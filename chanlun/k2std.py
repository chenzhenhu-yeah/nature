
from pyecharts import Line
from pyecharts import Kline
from pyecharts import Overlap

import pandas as pd
from datetime import datetime, timedelta

df1 = pd.read_csv('min1_20190815_SR001.csv')
#df1['datetime'] = df1['date'] + ' ' + df1['time']

up = 0
preHigh = 0
preLow = 0
preOpen = 0
preClose = 0
preDT = None
preTM = None
r = []

for i, row in df1.iterrows():

    # 定方向
    if row.high > preHigh and row.low >= preLow:
        up = 1
    if row.high <= preHigh and row.low < preLow:
        up = -1

    # 左包含右
    if preHigh >= row.high and preLow <= row.low :
        # print(preTM, preHigh, preLow)
        # print(row.time, row.high, row.low)
        # print('L'+'-'*30)

        if up == 1:
            preLow = row.low
            if preOpen<= preClose:
                preOpen = preLow
            else:
                preClose = preLow
        if up == -1:
            preHigh = row.high
            if preOpen <= preClose:
                preClose = preHigh
            else:
                preOpen = preLow
        r.pop()

    # 右包含左
    elif preHigh <= row.high and preLow >= row.low :
        # print(preTM, preHigh, preLow)
        # print(row.time, row.high, row.low)
        # print('R'+'-'*30)

        if up == 1:
            row.low = preLow
            if row.open <= row.close:
                row.open = row.low
            else:
                row.close = row.low
        if up == -1:
            row.high = preHigh
            if row.open <= row.close:
                row.close = row.high
            else:
                row.open = row.high

        preDT = row.date
        preTM = row.time
        preOpen = row.open
        preClose = row.close
        preHigh = row.high
        preLow = row.low
        r.pop()

    # 无包含
    else:
        # print(preTM, preHigh, preLow)
        # print(row.time, row.high, row.low)
        # print('N'+'-'*30)

        preHigh = row.high
        preLow  = row.low
        preDT = row.date
        preTM = row.time
        if row.open <= row.close:
            preOpen = row.low
            preClose = row.high
        else:
            preOpen = row.high
            preClose = row.low




    r.append([preDT,preTM,preOpen,preHigh,preLow,preClose,0])


    #
    # if i == 5:
    #     break

#print(r)
df = pd.DataFrame(r, columns=['date','time','open','high','low','close','volume'])
df.to_csv('std1.csv', index=False)



# #print(df1.head())
# dt_list =  list(df1['datetime'])
# #print(dt_list)
# k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
# #print(k_plot_value)



# if __name__ == '__main__':
#     pass
