
from pyecharts import Line
from pyecharts import Kline

from pyecharts import Overlap

import pandas as pd
from datetime import datetime, timedelta

fn = 'min1_20190815_SR001.csv'
fn = 'k.csv'

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


df_p = pd.read_csv('points1.csv')
line = Line()
line.add('test',df_p.datetime,df_p.value, line_type='dashed')
# #line.render()

#2019-08-14,21:04:00,5546
#2019-08-14,22:22:00,5530
#2019-08-14,22:50:00,5536.

overlap = Overlap()
overlap.add(kline)
overlap.add(line)
overlap.render()

# if __name__ == '__main__':
#     pass



# #print(df1.head())
# dt_list =  list(df1['datetime'])
# #print(dt_list)
# k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
# #print(k_plot_value)
