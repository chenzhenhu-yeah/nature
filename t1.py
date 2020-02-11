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


class Bar_Cci():
    def gen_kline(self, df1):
        df1['datetime'] = df1['date']
        dt_list =  list(df1['datetime'])
        k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
        #print(k_plot_value)

        kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
        kline.add_xaxis( dt_list )
        kline.add_yaxis( 'bar', k_plot_value )
        kline.set_global_opts(title_opts=opts.TitleOpts(title='日线'),
                              datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1],range_start=0,range_end=100,),
                                             opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1],range_start=0,range_end=100,), ],
                              tooltip_opts=opts.TooltipOpts( axis_pointer_type="cross" ),
                              axispointer_opts=opts.AxisPointerOpts( is_show=True, link=[{"xAxisIndex": "all"}], ),
                              )

        return kline


    def gen_cci(self, df1, n):
        high_list = df1.apply(lambda record: float(record['high']), axis=1).tolist()
        high_list = np.array(high_list)

        low_list = df1.apply(lambda record: float(record['low']), axis=1).tolist()
        low_list = np.array(low_list)

        close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)

        rsi_n = talib.CCI(high_list, low_list, close_list, n)

        line = Line()
        line.add_xaxis( xaxis_data=list(df1['datetime']) )
        line.add_yaxis( 'cci_'+str(n),
                        y_axis=rsi_n,
                        xaxis_index=1,
                        yaxis_index=1,
                        label_opts=opts.LabelOpts(is_show=False),
                      )

        line.set_global_opts(yaxis_opts=opts.AxisOpts(min_=-150,max_=150),
                             # xaxis_opts=opts.AxisOpts(is_show=False),
                             xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False),),
                             legend_opts=opts.LegendOpts(is_show=True,pos_right="38%"),
                            )
        line.set_series_opts(
                            label_opts=opts.LabelOpts(is_show=False),
                            )

        return line


    def draw(self, symbol, fn_render):
        fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
        df1 = pd.read_csv(fn)
        # print(df1.head())
        price_min = int( df1.close.min() * 0.99 )
        price_max = df1.close.max()

        kline = self.gen_kline(df1)
        line_cci = self.gen_cci(df1, 100)

        grid_chart = Grid(
            init_opts=opts.InitOpts(
                width="1390px",
                height="700px",
                animation_opts=opts.AnimationOpts(animation=False),
            )
        )
        grid_chart.add(
            kline,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="60%"),
        )
        grid_chart.add(
            line_cci,
            grid_opts=opts.GridOpts(
                pos_left="10%", pos_right="8%", pos_top="75%", height="17%" ),
        )

        grid_chart.render(fn_render)


if __name__ == '__main__':
    symbol = 'CF005'

    fn_render = 'bar_test.html'
    bar_ma = Bar_Cci()
    bar_ma.draw(symbol, fn_render)
