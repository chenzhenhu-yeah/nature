import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time

import json
import tushare as ts


from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq
from nature import VtBarData, ArrayManager

import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss

################################################################################
class Resid():
    def gen_line_one(self, df1, symbol):

        df1['datetime'] = df1['date']
        dt_list1 =  list(df1['datetime'])
        # print( len(dt_list1) )
        # dt_list1 = [s[5:10] for s in dt_list1]
        close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list1 = np.array(close_list1)


        kline = Line(init_opts=opts.InitOpts(width='1500px'))
        kline.add_xaxis( dt_list1 )
        kline.add_yaxis(symbol, close_list1,label_opts=opts.LabelOpts(is_show=False))
        kline.set_global_opts(title_opts=opts.TitleOpts(title='日线'),
                              #datazoom_opts=[opts.DataZoomOpts()],
                              #xaxis_opts=opts.AxisOpts(type_='time'))
                              yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                              datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1],range_start=0,range_end=100,),
                                             opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1],range_start=0,range_end=100,), ],
                              tooltip_opts=opts.TooltipOpts( trigger="axis",axis_pointer_type="cross" ),
                              #axispointer_opts=opts.AxisPointerOpts(is_show=True, link=[{"xAxisIndex": "all"}], ),
                             )

        return kline

    def gen_line_two(self, df1):

        df1['datetime'] = df1['date']
        dt_list1 =  list(df1['datetime'])
        # print( len(dt_list1) )
        # dt_list1 = [s[5:10] for s in dt_list1]
        close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list1 = np.array(close_list1)


        kline = Line()
        kline.add_xaxis( dt_list1 )
        kline.add_yaxis('resid', close_list1, xaxis_index=1,yaxis_index=1,label_opts=opts.LabelOpts(is_show=False))
        kline.set_global_opts(yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                              legend_opts=opts.LegendOpts(is_show=True,pos_right="40%")
                              #xaxis_opts=opts.AxisOpts(type_='time'))
                             )

        return kline

    def draw(self, symbol, fn_render):

        fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
        df = pd.read_csv(fn)
        df1 = df.loc[:,['date','time','close']]

        n = 30
        close_list = df.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)
        ma_arr = talib.SMA(close_list, n)
        df['ma'] = ma_arr
        df['close'] = df['close'] - df['ma']
        df2 = df.loc[:,['date','time','close']]

        line1 = self.gen_line_one(df1, symbol)
        line2 = self.gen_line_two(df2)

        grid_chart = Grid(
            init_opts=opts.InitOpts(
                width="1300px",
                height="700px",
                #animation_opts=opts.AnimationOpts(animation=False),
            )
        )
        grid_chart.add(
            line1,
            grid_opts=opts.GridOpts(pos_left="5%", pos_right="3%", height="39%"),
        )
        grid_chart.add(
            line2,
            grid_opts=opts.GridOpts(
                pos_left="5%", pos_right="3%", pos_top="53%", height="39%" ),
        )

        grid_chart.render(fn_render)


if __name__ == '__main__':
    symbol = 'm2009'

    fn_render = 'bar_test.html'
    bar_ma = Resid()
    bar_ma.draw(symbol, fn_render)
