import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss

def gen_line(df1):
    #df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    dt_list1 =  list(df1['datetime'])
    # print( len(dt_list1) )
    # dt_list1 = [s[5:10] for s in dt_list1]
    close_list1 = df1.apply(lambda record: float(record['close1']), axis=1).tolist()
    close_list1 = np.array(close_list1)
    # print(close_list1)
    close_list2 = df1.apply(lambda record: float(record['close2']), axis=1).tolist()
    close_list2 = np.array(close_list2)

    price_min = 9000 
    line1 = Line(init_opts=opts.InitOpts(width='1500px', height='600px'))
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=price_min,
                                                    #max_=price_max,
                                                    splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)),
                                                    ),
                           datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",range_start=0,range_end=100,),],
                         )

    line1.extend_axis(
            yaxis=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(formatter="{value} °C")
            )
        )



    line1.add_xaxis( xaxis_data=dt_list1 )
    line1.add_yaxis( 'left', y_axis=close_list1, )
    line1.add_yaxis( 'right', y_axis=close_list2,  yaxis_index=1)

    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1


if __name__ == '__main__':

    fn = 'bar_kamaresid_raw_duo_CF.csv'
    df = pd.read_csv(fn)
    df.columns = ['date','time','close1','q1','q2','close2']

    line = gen_line(df)
    line.render('html/k2_multiY.html')
