
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

    # 判断是否顶分型
    def is_top(self):
        if self.bar2.high > self.bar1.high and self.bar2.high > self.bar3.high and self.bar2.low >= self.bar1.low and self.bar2.low >= self.bar3.low:
            return True
        else:
            return False

    # 判断是否底分型
    def is_bottom(self):
        if self.bar2.high <= self.bar1.high and self.bar2.high <= self.bar3.high and self.bar2.low < self.bar1.low and self.bar2.low < self.bar3.low:
            return True
        else:
            return False

    # 获取分型的特征点
    def get_point(self):
        if self.is_top():
            return Point(self.bar2.datetime, self.bar2.high)
        if self.is_bottom():
            return Point(self.bar2.datetime, self.bar2.low)

class Point():
    """连接点"""
    #----------------------------------------------------------------------
    def __init__(self, x, y):
        """Constructor"""
        self.x = x
        self.y = y


if __name__ == '__main__':

    df1 = pd.read_csv('k.csv')
    df1['datetime'] = df1['date'] + ' ' + df1['time']

    # 保存前2个bar
    bar1 = Bar(df1.iloc[0,:])
    bar2 = Bar(df1.iloc[1,:])

    # 从第3个bar开始遍历
    r = []
    df1 = df1[2:]
    for i, row in df1.iterrows():
        bar3 = Bar(row)
        fx = Tbfx(bar1, bar2, bar3)

        if fx.is_bottom():
            if r == []:
                r.append([i,fx])
            else:
                item = r[-1]
                last_i = item[0]
                last_fx = item[1]
                if last_fx.is_top():
                    # 与上一个分型间至少隔一个bar，当前分型成立
                    if i-last_i>3:
                        r.append([i,fx])
                    # 处理反向缺口
                    if i-last_i==1 and fx.bar2.high<last_fx.bar1.low:
                        r.append([i,fx])
                    # 处理反向缺口
                    if i-last_i==2 and fx.bar1.high<last_fx.bar1.low:
                        r.append([i,fx])

                if last_fx.is_bottom():
                    if fx.bar2.low < last_fx.bar2.low:
                        r.pop()
                        r.append([i,fx])

        if fx.is_top():
            if r == []:
                r.append([i,fx])
            else:
                item = r[-1]
                last_i = item[0]
                last_fx = item[1]
                if last_fx.is_bottom():
                    # 与上一个分型间至少隔一个bar，当前分型成立
                    if i-last_i>3:
                        r.append([i,fx])
                    # 处理反向缺口
                    if i-last_i==1 and fx.bar2.low>last_fx.bar1.high:
                        r.append([i,fx])
                    # 处理反向缺口
                    if i-last_i==2 and fx.bar1.low>last_fx.bar1.high:
                        r.append([i,fx])

                if last_fx.is_top():
                    if fx.bar2.high > last_fx.bar2.high:
                        r.pop()
                        r.append([i,fx])

        bar1 = bar2
        bar2 = bar3
        #break


    #print(r)
    start_p = None
    end_p = None
    bi_point_array = []
    for i, item in enumerate(r):
        fx = item[1]
        if i == 0:
            start_p = fx.get_point()
        else:
            end_p = fx.get_point()
            bi_point_array.append( [start_p.x,start_p.y,end_p.x,end_p.y] )
            start_p = end_p

    df = pd.DataFrame(bi_point_array, columns=['s_x','s_y','e_x','e_y'])
    df.to_csv('bi.csv', index=False)
