import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss


def gen_line(df1, s1, price_min, price_max):
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])
    dt_list1 = [s[5:10] for s in dt_list1]
    # print( len(dt_list1) )
    # dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list1 = np.array(close_list1)
    # print(close_list1)

    line1 = Line(init_opts=opts.InitOpts(width='1500px', height='600px'))
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=price_min,
                                                    max_=price_max,
                                                    splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)),
                                                    ),
                           datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",range_start=0,range_end=100,),],
                         )
    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( s1, y_axis=close_list1, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1


if __name__ == '__main__':

    fn1 = get_dss() +'backtest/fut/OI/day_OI_01.csv'
    fn1 = get_dss() +'backtest/fut/y/day_y_01.csv'
    df1 = pd.read_csv(fn1)
    # print(df1.head(3))
    df1_2018 = df1[(df1.date >= '2018-01-01') & (df1.date <= '2018-12-31')]
    df1_2019 = df1[(df1.date >= '2019-01-01') & (df1.date <= '2019-12-31')]
    # print(df1_2018.head(3))
    # print(df1_2019.head(3))


    fn2 = get_dss() +'backtest/fut/RM/day_RM_01.csv'
    fn2 = get_dss() +'backtest/fut/m/day_m_01.csv'
    df2 = pd.read_csv(fn2)
    # print(df2.head(3))
    df2_2018 = df2[(df2.date >= '2018-01-01') & (df2.date <= '2018-12-31')]
    df2_2019 = df2[(df2.date >= '2019-01-01') & (df2.date <= '2019-12-31')]
    # print(df2_2018.head(3))
    # print(df2_2019.head(3))

    assert( len(df1) == len(df2) )

    df1_2018['close'] = df1_2018['close'] - df2_2018['close']
    price_min = df1_2018.close.min()
    price_max = df1_2018.close.max()

    df1_2019['close'] = df1_2019['close'] - df2_2019['close']
    price_min = min( price_min, df1_2019.close.min() )
    price_max = max( price_max, df1_2019.close.max() )


    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    day_list = ['01','02','03','04','05','06','07','08','09','10',
                '11','12','13','14','15','16','17','18','19','20',
                '21','22','23','24','25','26','27','28','29','30','31']
    dt_list = ["2019-{}-{}".format(x,y) for x in month_list for y in day_list]
    # print(dt_list)
    n = len(dt_list)
    df1_all = pd.DataFrame([])
    df1_all['close'] = [np.nan]*n
    df1_all['date']  = dt_list
    # print(df1_all)


    line1_all  = gen_line(df1_all, '', price_min, price_max)
    line1_2018 = gen_line(df1_2018, '2018', price_min, price_max)
    line1_2019 = gen_line(df1_2019, '2019', price_min, price_max)
    line = line1_all.overlap(line1_2018)
    line = line.overlap(line1_2019)

    fn = get_dss( )+ 'backtest/render/k2_ic_year.html'
    line.render(fn)
