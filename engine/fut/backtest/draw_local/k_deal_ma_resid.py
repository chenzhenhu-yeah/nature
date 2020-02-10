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
                          legend_opts=opts.LegendOpts(is_show=False),
                         )
    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( s1, y_axis=close_list1, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1

def gen_ma(df1,n):
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])

    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)
    ma_n = talib.SMA(close_list, n)
    # print(ma_120)
    # print( type(ma_120) )

    line = Line()
    line.add_xaxis( xaxis_data=dt_list1 )
    line.add_yaxis( str(n), y_axis=ma_n,label_opts=opts.LabelOpts(is_show=False),)

    return line

def gen_line_ma(dt_list, ma_list):
    line = Line()
    line.add_xaxis( xaxis_data=dt_list )
    line.add_yaxis( 'n', y_axis=ma_list, label_opts=opts.LabelOpts(is_show=False), )

    return line

def gen_resid(dt_list, close_list):
    line = Line()
    line.add_xaxis( xaxis_data=dt_list )
    line.add_yaxis( 'resid',
                    y_axis=close_list,
                    xaxis_index=1,
                    yaxis_index=1,
                    label_opts=opts.LabelOpts(is_show=False),
                  )

    line.set_global_opts(#yaxis_opts=opts.AxisOpts(min_=-150,max_=150),
                         # xaxis_opts=opts.AxisOpts(is_show=False),
                         xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False),),
                        )
    line.set_series_opts(
                        label_opts=opts.LabelOpts(is_show=False),
                        )

    return line


def gen_atr(df1, n):
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])

    high_list = df1.apply(lambda record: float(record['high']), axis=1).tolist()
    high_list = np.array(high_list)
    low_list = df1.apply(lambda record: float(record['low']), axis=1).tolist()
    low_list = np.array(low_list)
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    atr_1 = talib.ATR(high_list, low_list, close_list, 1)
    atr_n =  talib.SMA(atr_1, n)

    line = Line()
    line.add_xaxis( xaxis_data=dt_list1 )
    line.add_yaxis( 'atr_'+str(n),
                    y_axis=atr_n,
                    xaxis_index=2,
                    yaxis_index=2,
                    label_opts=opts.LabelOpts(is_show=False),
                  )
    line.set_global_opts(
                        xaxis_opts=opts.AxisOpts(is_show=False),
                        )

    return line

def draw_charts():

    pz = 'CF'
    vtSymbol = 'CF'

    fn = get_dss( )+ 'backtest/fut/' + pz + '/day_' + vtSymbol + '.csv'
    df1 = pd.read_csv(fn)
    df1 = df1[df1.date >= '2019-01-20']
    # df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    dt_list1 = list(df1['datetime'])
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)
    ma_list = talib.SMA(close_list, 20)
    close_list = close_list[19:]
    # ma_list = ma_list[19:]


    price_min = df1.close.min() - 100
    price_max = df1.close.max() + 100

    import statsmodels.api as sm
    X = df1.close.tolist()
    X = sm.add_constant(X[19:])
    # print(len(X))
    Y = ma_list[19:]
    model = sm.OLS(Y,X).fit()

    # print(model.summary())
    fit_value = [np.nan]*19 + list(model.fittedvalues)
    # print( len(fit_value) )
    # print( len(ma_list) )
    # print( ma_list - fit_value)
    # print(model.resid)
    resid =  ma_list - fit_value

    line = gen_line(df1, vtSymbol, price_min, price_max)
    line_ma = gen_ma(df1, 20)
    # line_ma = gen_line_ma(dt_list1, ma_list)
    line_ma_fit = gen_line_ma(dt_list1, fit_value)

    line = line.overlap(line_ma)
    line = line.overlap(line_ma_fit)

    line_resid = gen_resid(dt_list1, resid)
    line_atr = gen_atr(df1, 10)

    grid_chart = Grid(
        init_opts=opts.InitOpts(
            width="1300px",
            height="700px",
            animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    grid_chart.add(
        line,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="45%"),
    )
    grid_chart.add(
        line_resid,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="57%", height="17%" ),
    )

    grid_chart.add(
        line_atr,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="76%", height="17%" ),
    )

    fn = get_dss( )+ 'backtest/render/k_deal_cci_' + vtSymbol + '.html'
    grid_chart.render(fn)


if __name__ == "__main__":
    draw_charts()
