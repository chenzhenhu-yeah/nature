import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss


def gen_line(df1, symbol):
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    dt_list =  list(df1['datetime'])
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    line1 = Line(init_opts=opts.InitOpts(width='1500px'))
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=2000),
                           datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",range_start=90,range_end=100,), ],
                         )
    line1.add_xaxis( xaxis_data=list(df1['datetime']) )
    line1.add_yaxis( symbol, y_axis=close_list, )

    return line1

def m_y_2101_min15():
    fn = get_dss() +'fut/bar/min15_m2005.csv'
    df1 = pd.read_csv(fn)
    df1 = df1[df1.date >= '2020-01-01']
    df1 = df1.reset_index()
    # print(df1.head(3))

    fn = get_dss() +'fut/bar/min15_y2005.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.date >= '2020-01-01']
    df2 = df2.reset_index()
    # print(df2.head(3))

    df1['close'] = df2.close - df1.close
    # print(df1.close)

    line1 = gen_line(df1, 'y-m')
    line1.render('templates/y_m_2101_min15.html')

if __name__ == '__main__':
    pass
