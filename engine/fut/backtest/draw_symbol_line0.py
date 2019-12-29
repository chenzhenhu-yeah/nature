import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss


#fn = get_dss() +'backtest/fut/m/' + 'm_01_05.csv'
fn = get_dss() +'backtest/fut/m/' + 'day_m1901.csv'
df1 = pd.read_csv(fn)
df1['datetime'] = df1['date'] + ' ' + df1['time']
#print(df1.head())
dt_list =  list(df1['datetime'])
#print(dt_list)
#k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
#print(k_plot_value)
close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
close_list = np.array(close_list)

fn = get_dss() +'backtest/fut/m/' + 'day_m1905.csv'
df2 = pd.read_csv(fn)
df2['datetime'] = df2['date'] + ' ' + df2['time']
#print(df2.head())
dt_list2 =  list(df2['datetime'])
close_list2 = df2.apply(lambda record: float(record['close']), axis=1).tolist()
close_list2 = np.array(close_list2)


fn = get_dss() +'backtest/fut/m/' + 'day_m1805.csv'
df3 = pd.read_csv(fn)
df3['datetime'] = df3['date'] + ' ' + df3['time']
#print(df2.head())
dt_list3 =  list(df3['datetime'])
close_list3 = df3.apply(lambda record: float(record['close']), axis=1).tolist()
close_list3 = np.array(close_list3)


line1 = Line()
line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=2000) )
line1.add_xaxis( xaxis_data=list(df1['datetime']) )
line1.add_yaxis( 'atr_20',
                y_axis=close_list,
                #label_opts=opts.LabelOpts(is_show=False),
              )
# line.add_yaxis( 'atr_50',
#                 y_axis=close_list2,
#                 is_connect_nones=True
#                 #label_opts=opts.LabelOpts(is_show=False),
#               )
# line.render()

line2 = Line()
line2.add_xaxis( xaxis_data=list(df2['datetime']) )
line2.add_yaxis( 'atr_50',
                y_axis=close_list2,
                is_connect_nones=True
                #label_opts=opts.LabelOpts(is_show=False),
              )


line3 = Line()
line3.add_xaxis( xaxis_data=list(df3['datetime']) )
line3.add_yaxis( 'atr_70',
                y_axis=close_list3,
                is_connect_nones=True
                #label_opts=opts.LabelOpts(is_show=False),
              )

l = line1.overlap(line2)
l = l.overlap(line3)
l.render()



# if __name__ == '__main__':
#     pass
