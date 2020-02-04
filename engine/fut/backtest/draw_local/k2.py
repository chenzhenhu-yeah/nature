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
    pz1, s1 = 'RM', 'RM_01'                        # s1价格低
    pz2, s2 = 'RM', 'RM_05'                        # s2价格高
    price_min, price_max = 1800, 2800              # 价差区间

    fn = get_dss() +'backtest/fut/' + pz1 + '/day_' + s1 + '.csv'
    df1 = pd.read_csv(fn)

    fn = get_dss() +'backtest/fut/' + pz2 + '/day_' + s2 + '.csv'
    df2 = pd.read_csv(fn)

    print(df1.head(3))
    print(df2.head(3))

    print(len(df1))
    print(len(df2))

    line1_01 = gen_line(df1, s1, price_min, price_max)
    line1_05 = gen_line(df2, s2, price_min, price_max)
    #line = line1_01.overlap(line1_05)
    line = line1_05.overlap(line1_01)
    line.render('html/k2_'+s1+'_'+s2+'.html')
