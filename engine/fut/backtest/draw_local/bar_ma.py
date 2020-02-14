import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss

def gen_kline(df1):
    dt_list =  list(df1['datetime'])
    #print(dt_list)
    k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
    #print(k_plot_value)

    kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis('日K', k_plot_value)
    kline.set_global_opts(title_opts=opts.TitleOpts(title='Kline-基本示例'),
                          datazoom_opts=[opts.DataZoomOpts()],)
                          #xaxis_opts=opts.AxisOpts(type_='time'))
    return kline

def gen_ma(df1,n):
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)
    ma_n = talib.SMA(close_list, n)
    # print(ma_120)
    # print( type(ma_120) )

    line = Line()
    line.add_xaxis( xaxis_data=list(df1['datetime']) )
    line.add_yaxis( str(n), y_axis=ma_n,label_opts=opts.LabelOpts(is_show=False),)

    return line

if __name__ == '__main__':
    pz = 'ru'
    vtSymbol = 'ru'
    fn = get_dss() +'backtest/fut/' + pz + '/' + 'day_' + vtSymbol + '.csv'
    #fn = get_dss() +'backtest/fut/' + pz + '/' + 'min30_' + vtSymbol + '.csv'

    df1 = pd.read_csv(fn)
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    print(df1.head())


    kline = gen_kline(df1)
    line1  = gen_ma(df1, 10)
    line2  = gen_ma(df1, 30)
    line3  = gen_ma(df1, 60)

    d = kline.overlap(line1)
    d = d.overlap(line2)
    d = d.overlap(line3)

    fn = get_dss( )+ 'backtest/render/bar_ma_' + vtSymbol + '.html'
    d.render(fn)
