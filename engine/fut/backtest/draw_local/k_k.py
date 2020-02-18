import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss


def gen_line_one(df1):

    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])
    # print( len(dt_list1) )
    # dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list1 = np.array(close_list1)


    kline = Line(init_opts=opts.InitOpts(width='1500px'))
    kline.add_xaxis( dt_list1 )
    kline.add_yaxis('m', close_list1)
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

def gen_line_two(df1):

    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])
    # print( len(dt_list1) )
    # dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list1 = np.array(close_list1)


    kline = Line()
    kline.add_xaxis( dt_list1 )
    kline.add_yaxis('y', close_list1, xaxis_index=1,yaxis_index=1,)
    kline.set_global_opts(yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                          legend_opts=opts.LegendOpts(is_show=True,pos_right="40%")
                          #xaxis_opts=opts.AxisOpts(type_='time'))
                         )

    return kline

def draw_charts():

    # #fn = get_dss() +'backtest/fut/m/' + 'm_01_05.csv'
    # fn = get_dss() +'backtest/fut/m/' + 'day_m.csv'
    # df1 = pd.read_csv(fn)
    # df1['datetime'] = df1['date'] + ' ' + df1['time']
    # #print(df1.head())
    # kline1 = gen_kline_one(df1)
    #
    # fn = get_dss() +'backtest/fut/y/' + 'day_y.csv'
    # df2 = pd.read_csv(fn)
    # df2['datetime'] = df2['date'] + ' ' + df2['time']
    # #print(df1.head())
    # kline2 = gen_kline_two(df2)

#--------------------------------------------------------------------------

    fn = 'bar_kamaresid_raw_duo_CF.csv'
    fn = 'bar_kamaresid_raw_duo_m.csv'

    df = pd.read_csv(fn)
    df.columns = ['date','time','close','q1','q2','resid']
    df1 = df.loc[:,['date','time','close']]
    df2 = df.loc[:,['date','time','resid']]
    df2['close'] = df2['resid']
    print(df1.head(3))
    print(df2.head(3))
    s1 = 'close'
    s2 = 'resid'

    line1 = gen_line_one(df1)
    line2 = gen_line_two(df2)


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

    fn = get_dss( )+ 'backtest/render/k_k_' + s1 + '_' + s2 + '.html'
    grid_chart.render(fn)


if __name__ == "__main__":
    draw_charts()
