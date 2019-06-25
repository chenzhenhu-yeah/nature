
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import sys
sys.path.append(r'../')
from down_k.get_trading_dates import get_trading_dates
from hu_signal.macd import init_signal_macd, signal_macd_sell, signal_macd_buy
from hu_signal.k_pattern import signal_k_pattern

def backtest(dss, begin_date, end_date, codes):
    """
    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """
    r = []
    all_dates = get_trading_dates(dss, begin_date, end_date)
    print(all_dates)

    # 准备信号
    df_signal_macd = init_signal_macd(dss,codes)
    #print(len(df_signal_macd))

    # 按照日期一步步回测
    for i, _date in enumerate(all_dates):
        print('Backtest at %s.' % _date)

        #macd卖出信号
        to_sell_codes = signal_macd_sell(codes, _date, df_signal_macd)
        for code in to_sell_codes:
            r.append(_date + ' *** sell signal '+ code)
            print('                    ********* sell signal '+ code)

        #macd买入出信号
        to_buy_codes = signal_macd_buy(codes, _date, df_signal_macd)
        for code in to_buy_codes:
            r.append(_date + ' *** buy signal '+ code)
            print('                    ********* buy signal '+ code)

        #k_pattern
        pattern_codes = signal_k_pattern(dss, codes, _date)
        for item in pattern_codes:
            r.append(_date + ' *** pattern signal '+ item)
            print('                    ********* pattern signal '+ item)

    return r

def signal_run(dss):
    filename = dss + 'care_stocks.csv'

    now = datetime.now()
    end_date = now.strftime('%Y-%m-%d')
    begin_date = (now-timedelta(days=5)).strftime('%Y-%m-%d')
    #end_date = '2018-10-21'
    #begin_date = '2018-10-11'

    df = pd.read_csv(filename,dtype={'code':str})
    codes = set(df['code'])
    #print(codes)
    return backtest(dss, begin_date, end_date, codes)

if __name__ == "__main__":
    signal_run(r'../data/')
