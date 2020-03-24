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

    #line1 = Line(init_opts=opts.InitOpts(width='1500px', height='600px'))
    line1 = Line()
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=price_min,
                                                    max_=price_max,
                                                    splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)),
                                                    ),
                          datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1,2],range_start=0,range_end=100,),
                                         opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1,2],range_start=0,range_end=100,), ],
                          #legend_opts=opts.LegendOpts(is_show=False),
                          #tooltip_opts=opts.TooltipOpts( trigger="axis",axis_pointer_type="cross" ),
                          tooltip_opts=opts.TooltipOpts( axis_pointer_type="cross" ),
                          axispointer_opts=opts.AxisPointerOpts( is_show=True, link=[{"xAxisIndex": "all"}], ),
                         )
    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( s1, y_axis=close_list1, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1

def gen_cci(df1, n):

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


#----------------------------------------------------------------------
def er(close_list, n):
    """efficiency_ratio"""
    direction = close_list[-1] - close_list[-n]
    volatility = 0
    for i in range(1,n):
        volatility += abs( close_list[-i] - close_list[-i-1] )
    if volatility == 0:
        return 0
    else:
        return round( abs(direction/volatility), 2 )


def gen_er(df1, n):
    dt_list = list(df1['datetime'])
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    r = [np.nan]*(n-1)
    size = len(close_list)
    for i in range(n,size+1):
        c_list = close_list[:i]
        r.append( er(c_list,n) )

    line = Line()
    line.add_xaxis( xaxis_data=dt_list )
    line.add_yaxis( 'er_'+str(n),
                    y_axis=r,
                    xaxis_index=2,
                    yaxis_index=2,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.set_global_opts(
                        xaxis_opts=opts.AxisOpts(is_show=False),
                        legend_opts=opts.LegendOpts(is_show=True,pos_right="30%")
                        )

    return line

def gen_atr(df1, n):

    high_list = df1.apply(lambda record: float(record['high']), axis=1).tolist()
    high_list = np.array(high_list)
    low_list = df1.apply(lambda record: float(record['low']), axis=1).tolist()
    low_list = np.array(low_list)
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    atr_1 = talib.ATR(high_list, low_list, close_list, 1)
    atr_n =  talib.SMA(atr_1, n)

    line = Line()
    line.add_xaxis( xaxis_data=list(df1['datetime']) )
    line.add_yaxis( 'atr_'+str(n),
                    y_axis=atr_n,
                    xaxis_index=1,
                    yaxis_index=1,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.set_global_opts(
                        xaxis_opts=opts.AxisOpts(is_show=False),
                        legend_opts=opts.LegendOpts(is_show=True,pos_right="30%")
                        )

    return line

def gen_poit_open(df2):
    df = df2[df2.offset=='开']
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

    pz = 'CF'
    vtSymbol = 'CF'

    fn = get_dss( )+ 'backtest/fut/' + pz + '/day_' + vtSymbol + '.csv'
    df1 = pd.read_csv(fn)
    # df1 = df1[df1.date >= '2019-01-20']
    #df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    price_min = int( df1.close.min() * 0.99 )
    price_max = df1.close.max()

    #fn  = get_dss( )+ 'fut/engine/cci_raw/signal_cci_raw_duo_deal_CF.csv'
    fn  = get_dss( )+ 'fut/engine/cci_raw/signal_cci_raw_kong_deal_CF.csv'

    # fn  = get_dss( )+ 'fut/engine/cci_enhance/signal_cci_enhance_duo_deal_CF.csv'
    # fn  = get_dss( )+ 'fut/engine/cci_enhance/signal_cci_enhance_kong_deal_CF.csv'

    df2 = pd.read_csv(fn)
    dt_list = df2['datetime'].tolist()
    dt_list = [dt[:10] for dt in dt_list]
    # print(dt_list)
    df2['datetime'] = dt_list
    #df2 = df2[df2.datetime >= '2019-01-20']



    line = gen_line(df1, vtSymbol, price_min, price_max)
    line_cci = gen_cci(df1, 100)
    line_er = gen_er(df1, 20)
    # line_atr = gen_atr(df1, 10)

    scatter_open = gen_poit_open(df2)
    scatter_close = gen_poit_close(df2)
    line = line.overlap(scatter_open)
    line = line.overlap(scatter_close)

    grid_chart = Grid(
        init_opts=opts.InitOpts(
            width="1300px",
            height="700px",
            animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    grid_chart.add(
        line,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="40%"),
    )
    grid_chart.add(
        line_cci,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="55%", height="17%" ),
    )

    grid_chart.add(
        line_er,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="76%", height="17%" ),
    )

    fn = get_dss( )+ 'backtest/render/k_deal_cci_' + vtSymbol + '.html'
    grid_chart.render(fn)


if __name__ == "__main__":
    draw_charts()
