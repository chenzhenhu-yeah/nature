import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss

def gen_kline(df1, symbol):
    dt_list =  list(df1['datetime'])
    #print(dt_list)
    k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
    #print(k_plot_value)

    kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis(symbol, k_plot_value)
    kline.set_global_opts(title_opts=opts.TitleOpts(title='K线'),
                          datazoom_opts=[opts.DataZoomOpts()],)
                          #xaxis_opts=opts.AxisOpts(type_='time'))
    return kline

#----------------------------------------------------------------------
def sma(close_list, n, array=False):
    """简单均线"""
    result = talib.SMA(close_list, n)
    if array:
        return result
    return result[-1]

#----------------------------------------------------------------------
def std_here(close_list, n, array=False):
    """标准差"""
    result = talib.STDDEV(close_list, n)
    if array:
        return result
    return result[-1]

#----------------------------------------------------------------------
def boll(close_list, n, dev, array=True):
    """布林通道"""
    mid = sma(close_list, n, array)
    std = std_here(close_list, n, array)

    up = mid + std * dev
    down = mid - std * dev

    return up, down

#----------------------------------------------------------------------
def gen_boll(df1, n, dev):
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)
    dt_list = df1['datetime'].tolist()

    up, down = boll(close_list, n, dev)

    line = Line()
    line.add_xaxis( xaxis_data=dt_list )
    line.add_yaxis( 'boll_up',
                    y_axis=up,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.add_yaxis( 'boll_down',
                    y_axis=down,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.set_global_opts(
                        xaxis_opts=opts.AxisOpts(is_show=False),
                        legend_opts=opts.LegendOpts(is_show=True,pos_right="40%")
                        )

    return line

#----------------------------------------------------------------------
if __name__ == '__main__':

    pz = 'ru'
    vtSymbol = 'ru'
    fn = get_dss() +'backtest/fut/' + pz + '/' + 'day_' + vtSymbol + '.csv'
    #fn = get_dss() +'backtest/fut/' + pz + '/' + 'min30_' + vtSymbol + '.csv'

    df1 = pd.read_csv(fn)
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    print(df1.head())


    kline = gen_kline(df1, vtSymbol)
    line1  = gen_boll(df1, 20, 2)

    p = kline.overlap(line1)

    fn = get_dss( )+ 'backtest/render/bar_boll.html'
    p.render(fn)
