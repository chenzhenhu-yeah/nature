

import pyecharts.options as opts
from pyecharts.charts import Line, Kline


import pandas as pd
from datetime import datetime, timedelta


fn = 'k.csv'
df1 = pd.read_csv(fn)
df1['datetime'] = df1['date'] + ' ' + df1['time']
#df1.datetime = df1.datetime.apply( lambda a:datetime.strptime(a, '%Y-%m-%d %H:%M:%S') )
print(type(df1.datetime))

k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
#print(k_plot_value)

kline = Kline(init_opts=opts.InitOpts(width='1500px'))
kline.add_xaxis( list(df1['datetime']) )
kline.add_yaxis('日K', k_plot_value)
kline.set_global_opts(title_opts=opts.TitleOpts(title='Kline-基本示例'),
                      datazoom_opts=[opts.DataZoomOpts()],)
                      #xaxis_opts=opts.AxisOpts(type_='time'))


df_p = pd.read_csv('points10.csv')
#df_p.datetime = df_p.datetime.apply( lambda a:datetime.strptime(a, '%Y-%m-%d %H:%M:%S') )
#print(type(df_p.datetime))
#da['volume'] = da['volume'].apply(lambda vol: vol if vol > 0 else 0)

# k_plot_value = df1.apply(lambda record: [record['open'], , record['high']], axis=1).tolist()

line = Line()
line.add_xaxis( list(df_p.datetime) )
line.add_yaxis("商家A", df_p.value, linestyle_opts=opts.LineStyleOpts(type_='dotted'))


line2 = Line()
line2.add_xaxis( ['2019-08-14 21:29:00', '2019-08-15 13:30:00'] )
line2.add_yaxis("商家B", [5552.0, 5463.0], linestyle_opts=opts.LineStyleOpts(type_='solid', width=3))

# line3 = Line(init_opts=opts.InitOpts(width='1500px'))
# line3.add_xaxis(df_p.datetime)
# line3.add_yaxis("商家C", df_p.value/4, linestyle_opts=opts.LineStyleOpts(type_='solid'))
# line3.set_global_opts(title_opts=opts.TitleOpts(title='Line-基本示例',subtitle='aaa'),
#                      xaxis_opts=opts.AxisOpts(type_='time'))
#line2.render()

# overlap = Overlap()
# overlap.add(line)
# overlap.add(line2)
# overlap.render()
#
# line.overlap(line2)
# line.overlap(line3)
kline.overlap(line)
kline.overlap(line2)
#kline.render()
kline.render()
