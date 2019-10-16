import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar

import pandas as pd
from datetime import datetime, timedelta


fn = 'bar/min1_SR001.csv'

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
kline.render()



# if __name__ == '__main__':
#     pass
