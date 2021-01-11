
import os
import time
import datetime
import json
import pandas as pd


# 默认空值
EMPTY_STRING = ''
EMPTY_UNICODE = u''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0

########################################################################
class VtBaseData(object):
    """回调函数推送数据的基础类，其他数据类继承于此"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.gatewayName = EMPTY_STRING         # Gateway名称
        self.rawData = None                     # 原始数据

########################################################################
class VtBarData(VtBaseData):
    """K线数据"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtBarData, self).__init__()

        self.vtSymbol = EMPTY_STRING        # vt系统代码
        self.symbol = EMPTY_STRING          # 代码
        self.exchange = EMPTY_STRING        # 交易所

        self.open = EMPTY_FLOAT             # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        self.close_bfq = EMPTY_FLOAT

        self.date = EMPTY_STRING            # bar开始的时间，日期
        self.time = EMPTY_STRING            # 时间
        self.datetime = None                # python的datetime时间对象

        self.interval = EMPTY_UNICODE       # K线周期

        self.volume = EMPTY_INT             # 成交量
        self.OpenInterest = EMPTY_INT       # 持仓量

        self.AskPrice = 0.0            # 挂卖价
        self.BidPrice = 0.0            # 挂买价
        self.AskVolume = 1             # 挂卖量
        self.BidVolume = 1             # 挂买量

        self.AveragePrice = 0.0        # 均价
        self.UpperLimitPrice = 0.0     # 涨板价
        self.LowerLimitPrice = 0.0     # 跌板价
        self.PreOpenInterest = 0.0     # 昨持仓

        self.PreSettlementPrice = 0.0  # 前结算价
        self.PreClosePrice = 0.0       # 前收盘价
        self.OpenPrice = 0.0           # 开盘价
        self.PreDelta = 0.0
        self.CurrDelta = 0.0

    def print_bar(self):
        print(self.vtSymbol)
        print(self.date)
        print(self.open)
        print(self.high)
        print(self.low)
        print(self.close)



########################################################################
class BarGenerator(object):

    #----------------------------------------------------------------------
    def __init__(self, minx):
        """Constructor"""
        self.minx = minx
        self.bar_dict = {}
        self.r_dict = {}

    #----------------------------------------------------------------------
    def update_bar(self, new_bar):
        symbol = new_bar.vtSymbol

        if symbol in self.r_dict:
            r = self.r_dict[symbol]
        else:
            r = []
            self.r_dict[symbol] = r

        if symbol in self.bar_dict:
            bar = self.bar_dict[symbol]
        else:
            bar = new_bar
            self.bar_dict[symbol] = bar
            return None

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if self.minx == 'min5':
            if new_bar.time[3:5] in ['04','09','14','19','24','29','34','39','44','49','54','59']:
                # 将 bar的秒钟改为整点，推送并保存bar
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        elif self.minx == 'min15':
            if new_bar.time[3:5] in ['14','29','44','59']:
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        elif self.minx == 'min30':
            min30_list = ['09:29','09:59','10:44','11:14',
                          '13:44','14:14','14:44','14:59', \
                          '21:29','21:59','22:29','22:59', \
                          '23:29','23:59','00:29','00:59', \
                          '01:29','01:59','02:29']
            if new_bar.time[:5] in min30_list:
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        elif self.minx == 'day':
            if new_bar.time[:5] in ['15:00']:
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [new_bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        self.bar_dict[symbol] = bar
        return None

    #----------------------------------------------------------------------
    def save_bar(self, bar):
        df = pd.DataFrame([bar.__dict__])
        cols = ['date','time','open','high','low','close','volume']
        df = df[cols]

        fname = get_dss() + 'fut/put/rec/' + self.minx + '_' + bar.vtSymbol + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False)

########################################################################


def five2day(symbol):
    fn = 'FutAC_Min5_Std_2020/'+symbol+'.csv'
    df = pd.read_csv(fn, skiprows=1, header=None)
    df.columns = ['market', 'symbol', 'date','open','high','low','close','volume','amount','hold']
    df['time'] = df['date'].str[11:]
    df['date'] = df['date'].str[:10]
    # print(df.head())
    # return

    # 生成minx
    g_day = BarGenerator('day')
    for i, row in df.iterrows():
        new_bar = VtBarData()
        new_bar.vtSymbol = symbol
        new_bar.date = row.date
        new_bar.time = row.time
        new_bar.open = row.open
        new_bar.high = row.high
        new_bar.low =  row.low
        new_bar.close = row.close
        g_day.update_bar(new_bar)

    # 保存day
    r_day = g_day.r_dict[symbol]
    df_symbol = pd.DataFrame(r_day, columns=['date','time','open','high','low','close','volume'])
    fname = 'day_'+symbol+'.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')


if __name__ == "__main__":
    symbol = 'm2105'
    five2day(symbol)
