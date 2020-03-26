import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss


def gen_line(df1, s1, price_min, price_max):
    #df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])
    # print( len(dt_list1) )
    # dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list1 = np.array(close_list1)
    # print(close_list1)

    line1 = Line(init_opts=opts.InitOpts(width='1390px', height='700px'))
    #line1 = Line()
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=price_min,
                                                    max_=price_max,
                                                    splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)),
                                                    ),
                          datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",range_start=0,range_end=100,),
                                         opts.DataZoomOpts(is_show=False,type_="inside",range_start=0,range_end=100,), ],
                         )
    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( s1, y_axis=close_list1, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1

def gen_ma(df1,n):
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])

    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)
    ma_n = talib.SMA(close_list, n)
    # print(ma_120)
    # print( type(ma_120) )

    line = Line()
    line.add_xaxis( xaxis_data=dt_list1 )
    line.add_yaxis( str(n), y_axis=ma_n,label_opts=opts.LabelOpts(is_show=False),)

    return line

def gen_poit_open(df2):
    df = df2[df2.offset=='开']
    print(df)
    if len(df)>0:
        dt_list =  list(df['datetime'])
        price_list = df.apply(lambda record: float(record['price']), axis=1).tolist()
        price_list = np.array(price_list)
    else:
        dt_list = []
        price_list = []

    c = Scatter()
    c.add_xaxis(dt_list)
    c.add_yaxis('', price_list, label_opts=opts.LabelOpts(position='top'))
    return c

def gen_poit_close(df2):
    df = df2[df2.offset=='平']
    if len(df) > 0:
        dt_list =  list(df['datetime'])
        price_list = df.apply(lambda record: float(record['price']), axis=1).tolist()
        price_list = np.array(price_list)
    else:
        dt_list = []
        price_list = []

    c = Scatter()
    c.add_xaxis(dt_list)
    c.add_yaxis('', price_list, label_opts=opts.LabelOpts(position='bottom'))
    return c

def draw_charts():

    pz = 'm'
    vtSymbol = 'm'

    fn = get_dss( )+ 'backtest/fut/' + pz + '/day_' + vtSymbol + '.csv'
    df1 = pd.read_csv(fn)
    # df1 = df1[df1.date >= '2019-01-20']
    #df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    price_min = int( df1.close.min() * 0.99 )
    price_max = df1.close.max()

    #fn  = get_dss( )+ 'fut/engine/dalicta/signal_dalicta_duo_deal_m.csv'
    fn  = get_dss( )+ 'fut/engine/dalicta/signal_dalicta_kong_deal_m.csv'

    df2 = pd.read_csv(fn)
    dt_list = df2['datetime'].tolist()
    dt_list = [dt[:10] for dt in dt_list]
    # print(dt_list)
    df2['datetime'] = dt_list

    line = gen_line(df1, vtSymbol, price_min, price_max)
    line_ma_10 = gen_ma(df1, 10)
    line_ma_30 = gen_ma(df1, 30)
    line_ma_60 = gen_ma(df1, 60)
    line = line.overlap(line_ma_10)
    line = line.overlap(line_ma_30)
    line = line.overlap(line_ma_60)

    scatter_open = gen_poit_open(df2)
    scatter_close = gen_poit_close(df2)
    line = line.overlap(scatter_open)
    line = line.overlap(scatter_close)

    fn = get_dss( )+ 'backtest/render/k_deal_dalicta_' + vtSymbol + '.html'
    line.render(fn)


if __name__ == "__main__":
    draw_charts()
