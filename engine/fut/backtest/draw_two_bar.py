import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss


def gen_kline(df1):

    dt_list =  list(df1['datetime'])
    kline_data = df1.apply(lambda record: [float(record['open']), float(record['close']), float(record['low']), float(record['high'])], axis=1).tolist()


    kline = Kline(init_opts=opts.InitOpts(width='1500px'))
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis('日K', kline_data)
    kline.set_global_opts(title_opts=opts.TitleOpts(title='Kline-基本示例'),
                          datazoom_opts=[opts.DataZoomOpts()],)
                          #xaxis_opts=opts.AxisOpts(type_='time'))

    return kline


def draw_charts():

    #fn = get_dss() +'backtest/fut/m/' + 'm_01_05.csv'
    fn = get_dss() +'backtest/fut/m/' + 'day_m1901.csv'
    df1 = pd.read_csv(fn)
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    #print(df1.head())
    kline1 = gen_kline(df1)

    fn = get_dss() +'backtest/fut/m/' + 'day_m1905.csv'
    df2 = pd.read_csv(fn)
    df2['datetime'] = df2['date'] + ' ' + df2['time']
    #print(df1.head())
    kline2 = gen_kline(df2)

    grid_chart = Grid(
        init_opts=opts.InitOpts(
            width="1000px",
            height="700px",
            animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    grid_chart.add(
        kline1,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="45%"),
    )
    grid_chart.add(
        kline2,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="57%", height="17%" ),
    )

    grid_chart.render("brush.html")


if __name__ == "__main__":
    draw_charts()
