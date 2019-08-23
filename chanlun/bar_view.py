
from pyecharts import Line
from pyecharts import Kline

from pyecharts import Overlap

import pandas as pd
from datetime import datetime, timedelta

fn = 'min1_20190815_SR001.csv'
fn = 'std1.csv'

df1 = pd.read_csv(fn)
df1['datetime'] = df1['date'] + ' ' + df1['time']

#print(df1.head())
dt_list =  list(df1['datetime'])
#print(dt_list)
k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
#print(k_plot_value)

kline = Kline("K 线图示例")
kline.add("日K", dt_list, k_plot_value, is_datazoom_show=True, )
#kline.render()
#
line = Line()
# line.add('test',['2019-08-14 21:04:00','2019-08-14 22:22:00','2019-08-14 22:50:00'],[5546,5530,5536])
# #line.render()

#2019-08-14,21:04:00,5546
#2019-08-14,22:22:00,5530
#2019-08-14,22:50:00,5536.

overlap = Overlap()
overlap.add(kline)
#overlap.add(line)
overlap.render()

# if __name__ == '__main__':
#     pass
