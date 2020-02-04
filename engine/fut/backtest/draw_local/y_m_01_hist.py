import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss


def gen_two_line(df1, s1, df2, s2):
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    dt_list1 =  list(df1['datetime'])
    dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list1 = np.array(close_list1)

    close_list2 = df2.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list2 = np.array(close_list2)

    line1 = Line(init_opts=opts.InitOpts(width='1500px'))
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=1500,max_=4500) )
    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( s1, y_axis=close_list1, )
    line1.add_yaxis( s2, y_axis=close_list2, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1


if __name__ == '__main__':

    fn = get_dss() +'backtest/fut/m/day_m_01.csv'
    df1 = pd.read_csv(fn)
    # print(df1.head(3))
    df1_2018 = df1[(df1.date >= '2018-01-01') & (df1.date <= '2018-12-31')]
    df1_2019 = df1[(df1.date >= '2019-01-01') & (df1.date <= '2019-12-31')]
    # print(df1_2018.head(3))
    # print(df1_2019.head(3))

    fn = get_dss() +'backtest/fut/y/day_y_01.csv'
    df2 = pd.read_csv(fn)
    # print(df2.head(3))
    df2_2018 = df2[(df2.date >= '2018-01-01') & (df2.date <= '2018-12-31')]
    df2_2019 = df2[(df2.date >= '2019-01-01') & (df2.date <= '2019-12-31')]
    # print(df2_2018.head(3))
    # print(df2_2019.head(3))

    assert( len(df1) == len(df2) )

    df1_2018['close'] = df2_2018.close - df1_2018.close
    df1_2019['close'] = df2_2019.close - df1_2019.close

    line1 = gen_two_line(df1_2018, '2018', df1_2019, '2019')
    line1.render('y_m_01_hist.html')
    # line1_2018 = gen_line(df1_2018, '2018')
    # line1_2019 = gen_line(df1_2019, '2019')
    # #line = line1_2018.overlap(line1_2019)
    # line = line1_2019.overlap(line1_2018)
    # line.render('y_m_hist.html')

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
