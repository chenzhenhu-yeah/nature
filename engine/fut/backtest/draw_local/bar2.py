import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss


def gen_kline_one(df1):

    dt_list =  list(df1['datetime'])
    kline_data = df1.apply(lambda record: [float(record['open']), float(record['close']), float(record['low']), float(record['high'])], axis=1).tolist()


    kline = Kline(init_opts=opts.InitOpts(width='1500px'))
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis('m', kline_data)
    kline.set_global_opts(title_opts=opts.TitleOpts(title='K线'),
                          #datazoom_opts=[opts.DataZoomOpts()],
                          #xaxis_opts=opts.AxisOpts(type_='time'))
                          yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                          datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1],range_start=0,range_end=100,),
                                         opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1],range_start=0,range_end=100,), ],
                          tooltip_opts=opts.TooltipOpts( trigger="axis",axis_pointer_type="cross" ),
                          #axispointer_opts=opts.AxisPointerOpts(is_show=True, link=[{"xAxisIndex": "all"}], ),
                         )

    return kline

def gen_kline_two(df1):

    dt_list =  list(df1['datetime'])
    kline_data = df1.apply(lambda record: [float(record['open']), float(record['close']), float(record['low']), float(record['high'])], axis=1).tolist()


    kline = Kline()
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis('y', kline_data, xaxis_index=1,yaxis_index=1,)
    kline.set_global_opts(yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                          legend_opts=opts.LegendOpts(is_show=True,pos_right="40%")
                          #xaxis_opts=opts.AxisOpts(type_='time'))
                         )

    return kline

def draw_charts():

    #fn = get_dss() +'backtest/fut/m/' + 'm_01_05.csv'
    fn = get_dss() +'backtest/fut/m/' + 'day_m.csv'
    df1 = pd.read_csv(fn)
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    #print(df1.head())
    kline1 = gen_kline_one(df1)

    fn = get_dss() +'backtest/fut/y/' + 'day_y.csv'
    df2 = pd.read_csv(fn)
    df2['datetime'] = df2['date'] + ' ' + df2['time']
    #print(df1.head())
    kline2 = gen_kline_two(df2)

    grid_chart = Grid(
        init_opts=opts.InitOpts(
            width="1300px",
            height="700px",
            #animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    grid_chart.add(
        kline1,
        grid_opts=opts.GridOpts(pos_left="3%", pos_right="3%", height="39%"),
    )
    grid_chart.add(
        kline2,
        grid_opts=opts.GridOpts(
            pos_left="3%", pos_right="3%", pos_top="53%", height="39%" ),
    )

    grid_chart.render("brush.html")


if __name__ == "__main__":
    draw_charts()
