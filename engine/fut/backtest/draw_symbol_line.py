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
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=2000) )
    line1.add_xaxis( xaxis_data=list(df1['datetime']) )
    line1.add_yaxis( symbol,
                     y_axis=close_list,
                     #color=c,
                  )

    return line1


if __name__ == '__main__':

    #symbols = ['m1901','m1905','m1805']
    symbols = ['m2001','m2005']

    fn = get_dss() +'backtest/fut/m/' + 'day_' + symbols[0] + '.csv'
    df = pd.read_csv(fn)
    line1 = gen_line(df, symbols[0])

    fn = get_dss() +'backtest/fut/m/' + 'day_' + symbols[1] + '.csv'
    df = pd.read_csv(fn)
    line2 = gen_line(df,symbols[1])

    # fn = get_dss() +'backtest/fut/m/' + 'day_' + symbols[2] + '.csv'
    # df = pd.read_csv(fn)
    # line3 = gen_line(df,symbols[2])

    line = line1.overlap(line2)
    #line = line.overlap(line3)
    line.render()
