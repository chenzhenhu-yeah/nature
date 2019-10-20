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

    #kline = Kline(init_opts=opts.InitOpts(width='1000px'))
    kline = Kline()
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis('日K', kline_data)

    kline.set_global_opts(title_opts=opts.TitleOpts(title='Kline-基本示例'),
                          datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1,2],range_start=98,range_end=100,),
                                         opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1,2],range_start=98,range_end=100,), ],
                          legend_opts=opts.LegendOpts(is_show=False),
                          yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                          tooltip_opts=opts.TooltipOpts(
                                                        trigger="axis",
                                                        axis_pointer_type="cross",
                                                        background_color="rgba(245, 245, 245, 0.8)",
                                                        border_width=1,
                                                        border_color="#ccc",
                                                        textstyle_opts=opts.TextStyleOpts(color="#000"),
                                                        ),
                          # visualmap_opts=opts.VisualMapOpts(
                          #                                   is_show=False,
                          #                                   dimension=2,
                          #                                   series_index=5,
                          #                                   is_piecewise=True,
                          #                                   pieces=[
                          #                                       {"value": 1, "color": "#ec0000"},
                          #                                       {"value": -1, "color": "#00da3c"}, ],
                          #                                  ),
                          axispointer_opts=opts.AxisPointerOpts(
                                                                is_show=True,
                                                                link=[{"xAxisIndex": "all"}],
                                                                label=opts.LabelOpts(background_color="#777"),
                                                               ),
                          # brush_opts=opts.BrushOpts(
                          #                             x_axis_index="all",
                          #                             brush_link="all",
                          #                             out_of_brush={"colorAlpha": 0.1},
                          #                             brush_type="lineX",
                          #                          ),
                      )

    return kline

def gen_rsi(df1):

    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    rsi_5 = talib.RSI(close_list, 5)
    rsi_10 = talib.RSI(close_list, 10)

    line = Line()
    line.add_xaxis( xaxis_data=list(df1['datetime']) )
    line.add_yaxis( 'rsi_5',
                    y_axis=rsi_5,
                    xaxis_index=1,
                    yaxis_index=1,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.add_yaxis( 'rsi_10',
                    y_axis=rsi_10,
                    xaxis_index=1,
                    yaxis_index=1,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.set_global_opts(
                        # xaxis_opts=opts.AxisOpts(is_show=False),
                        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False),),
                        )
    line.set_series_opts(
                        label_opts=opts.LabelOpts(is_show=False),
                        markline_opts=opts.MarkLineOpts(
                            data=[opts.MarkLineItem(y=66),opts.MarkLineItem(y=33),],
                            )
                        )

    return line


def gen_atr(df1):

    high_list = df1.apply(lambda record: float(record['high']), axis=1).tolist()
    high_list = np.array(high_list)
    low_list = df1.apply(lambda record: float(record['low']), axis=1).tolist()
    low_list = np.array(low_list)
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    atr_1 = talib.ATR(high_list, low_list, close_list, 1)
    atr_10 =  talib.SMA(atr_1, 10)
    atr_14 =  talib.SMA(atr_1, 14)
    atr_50 = talib.SMA(atr_1, 50) * 0.8

    line = Line()
    line.add_xaxis( xaxis_data=list(df1['datetime']) )
    line.add_yaxis( 'atr_1',
                    y_axis=atr_1,
                    xaxis_index=2,
                    yaxis_index=2,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    # line.add_yaxis( 'atr_10',
    #                 y_axis=atr_10,
    #                 xaxis_index=2,
    #                 yaxis_index=2,
    #                 label_opts=opts.LabelOpts(is_show=False),
    #               )
    line.add_yaxis( 'atr14',
                    y_axis=atr_14,
                    xaxis_index=2,
                    yaxis_index=2,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.add_yaxis( 'atr50',
                    y_axis=atr_50,
                    xaxis_index=2,
                    yaxis_index=2,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.set_global_opts(
                        xaxis_opts=opts.AxisOpts(is_show=False),
                        )

    return line

def gen_poit_one(df2):

    df = df2[(df2.offset=='开') & (df2.direction=='多')]
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

def gen_poit_two(df2):
    df = df2[(df2.offset=='开') & (df2.direction=='空')]
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

def gen_poit_three(df2):
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
    c.add_yaxis('', price_list)
    return c

def draw_charts():

    vtSymbol = 'c2001'

    fn = get_dss( )+ 'fut/bar/min5_' + vtSymbol + '.csv'
    df1 = pd.read_csv(fn)
    df1 = df1[df1.date >= '2019-10-15']
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    # print(df1.head())

    fn  = get_dss( )+ 'fut/check/signal_atrrsi_' + vtSymbol + '.csv'
    df2 = pd.read_csv(fn)

    kline = gen_kline(df1)
    line_rsi = gen_rsi(df1)
    line_atr = gen_atr(df1)
    scatter_deal_one = gen_poit_one(df2)
    scatter_deal_two = gen_poit_two(df2)
    scatter_deal_three = gen_poit_three(df2)

    kline_scatter = kline.overlap(scatter_deal_one)
    kline_scatter = kline_scatter.overlap(scatter_deal_two)
    kline_scatter = kline_scatter.overlap(scatter_deal_three)

    grid_chart = Grid(
        init_opts=opts.InitOpts(
            width="1000px",
            height="700px",
            animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    grid_chart.add(
        kline_scatter,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="45%"),
    )
    grid_chart.add(
        line_rsi,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="57%", height="17%" ),
    )

    grid_chart.add(
        line_atr,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="76%", height="17%" ),
    )

    grid_chart.render("brush.html")


if __name__ == "__main__":
    draw_charts()
