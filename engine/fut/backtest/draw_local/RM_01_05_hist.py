import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss


def gen_line(df1, s1):
    #df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])
    print( len(dt_list1) )
    # dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list1 = np.array(close_list1)
    # print(close_list1)

    line1 = Line(init_opts=opts.InitOpts(width='1500px'))
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=1500,max_=3000) )
    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( s1, y_axis=close_list1, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1


if __name__ == '__main__':

    fn = get_dss() +'backtest/fut/RM/day_RM_01.csv'
    df1 = pd.read_csv(fn)
    # print(df1.head(3))
    # df1_2018 = df1[(df1.date >= '2018-01-01') & (df1.date <= '2018-12-31')]
    # df1_2019 = df1[(df1.date >= '2019-01-01') & (df1.date <= '2019-12-31')]
    # print(df1_2018.head(3))
    # print(df1_2019.head(3))

    fn = get_dss() +'backtest/fut/RM/day_RM_05.csv'
    df2 = pd.read_csv(fn)
    # print(df2.head(3))
    # df2_2018 = df2[(df2.date >= '2018-01-01') & (df2.date <= '2018-12-31')]
    # df2_2019 = df2[(df2.date >= '2019-01-01') & (df2.date <= '2019-12-31')]
    # print(df2_2018.head(3))
    # print(df2_2019.head(3))


    result = pd.merge(df2, df1, how='left', on=['date','time'], indicator=True)
    print(result.head(3))
    # print(type(result))

    df1 = result.loc[:, ['date','time','close_y'] ]
    df1['close'] = df1.close_y
    print(df1.head(3))

    df2 = result.loc[:, ['date','time','close_x'] ]
    df2['close'] = df2.close_x
    print(df2.head(3))

    # assert( len(df1) == len(df2) )
    print(len(df1))
    print(len(df2))

    line1_01 = gen_line(df1, '01')
    line1_05 = gen_line(df2, '05')
    line = line1_01.overlap(line1_05)
    line.render('RM_01_05_hist.html')

    # line = gen_line(df1, '01')
    # line.render('RM_01_05_hist.html')

    # line1_2019.render()

    # line1 = gen_line(df1, symbols[0])
    # line2 = gen_line(df2,symbols[1])
    #
    # # fn = get_dss() +'backtest/fut/m/' + 'day_' + symbols[2] + '.csv'
    # # df = pd.read_csv(fn)
    # # line3 = gen_line(df,symbols[2])
    #
    # line = line1.overlap(line2)
    # #line = line.overlap(line3)
    # line.render()
