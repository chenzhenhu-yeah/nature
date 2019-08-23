
from pyecharts import Line
from pyecharts import Kline
from pyecharts import Overlap

import pandas as pd
from datetime import datetime, timedelta

class Bar():
    """K线数据"""

    #----------------------------------------------------------------------
    def __init__(self, bar):
        """Constructor"""
        self.open = bar.open                        # OHLC
        self.high = bar.high
        self.low = bar.low
        self.close = bar.close

        self.datetime = bar.datetime                # python的datetime时间对象
        self.volume = bar.volume                    # 成交量

class Tbfx:
    """顶分型和底分型"""

    def __init__(self, bar1, bar2, bar3):
        self.bar1= bar1
        self.bar2= bar2
        self.bar3= bar3

    def is_top(self):
        if self.bar2.high > self.bar1.high and self.bar2.high > self.bar3.high and self.bar2.low >= self.bar1.low and self.bar2.low >= self.bar3.low:
            return True
        else:
            return False

    def is_bottom(self):
        if self.bar2.high <= self.bar1.high and self.bar2.high <= self.bar3.high and self.bar2.low < self.bar1.low and self.bar2.low < self.bar3.low:
            return True
        else:
            return False

df1 = pd.read_csv('std1.csv')
df1['datetime'] = df1['date'] + ' ' + df1['time']

bar1 = Bar(df1.iloc[0,:])
bar2 = Bar(df1.iloc[1,:])
print(bar1.__dict__)
print(bar2.__dict__)

r = []
df1 = df1[2:]
for i, row in df1.iterrows():
    bar3 = Bar(row)
    #print(bar3.__dict__)

    fx = Tbfx( bar1, bar2, bar3)

    if fx.is_bottom():
        if r == []:
            r.append(fx)
        else:
            last_fx = r[-1]
            if last_fx.is_top():
                r.append(fx)
            if last_fx.is_bottom():
                if fx.bar2.low < last_fx.bar2.low:
                    r.pop()
                    r.append(fx)

    if fx.is_top():
        if r == []:
            r.append(fx)
        else:
            last_fx = r[-1]
            if last_fx.is_bottom():
                r.append(fx)
            if last_fx.is_top():
                if fx.bar2.high > last_fx.bar2.high:
                    r.pop()
                    r.append(fx)

    bar1 = bar2
    bar2 = bar3

    #break


print(r)
# df = pd.DataFrame(r, columns=['date','time','open','high','low','close','volume'])
# df.to_csv('std1.csv', index=False)
#


# #print(df1.head())
# dt_list =  list(df1['datetime'])
# #print(dt_list)
# k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
# #print(k_plot_value)



# if __name__ == '__main__':
#     pass
