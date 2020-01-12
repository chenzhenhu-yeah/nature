import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib


from nature import get_dss

pz = 'OI'
fn = get_dss() +'backtest/fut/' + pz + '/' + 'day_' + pz + '.csv'
#fn = get_dss() +'backtest/fut/m/' + 'min30_m1901.csv'

df1 = pd.read_csv(fn)
df1['datetime'] = df1['date'] + ' ' + df1['time']
print(df1.head())
dt_list =  list(df1['datetime'])
#print(dt_list)
k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
#print(k_plot_value)

kline = Kline(init_opts=opts.InitOpts(width='1500px'))
kline.add_xaxis( list(df1['datetime']) )
kline.add_yaxis('日K', k_plot_value)
kline.set_global_opts(title_opts=opts.TitleOpts(title='Kline-基本示例'),
                      datazoom_opts=[opts.DataZoomOpts()],)
                      #xaxis_opts=opts.AxisOpts(type_='time'))

close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
close_list = np.array(close_list)
ma_120 = talib.SMA(close_list, 120)

line = Line()
line.add_xaxis( xaxis_data=list(df1['datetime']) )
line.add_yaxis( 'ma_120',
                y_axis=ma_120,
                label_opts=opts.LabelOpts(is_show=False),
              )

#kline.render()
kline.overlap(line).render()

# if __name__ == '__main__':
#     pass
