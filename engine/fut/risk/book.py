import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, get_contract, to_log, get_tick
from nature import get_symbols_quote


def extract_trade():
    """从成交反馈文件中提取最新成交记录，追加到got_trade文件中"""

    # 提取当前日期下的成交记录
    now = datetime.now()
    today = int( now.strftime('%Y%m%d') )
    # today = 20201217
    fn = get_dss() +  'fut/engine/gateway_trade.csv'
    df = pd.read_csv(fn)
    df = df[df.TradingDay >= today]                 # 当日交易记录
    df = df.drop_duplicates()
    # print(df)
    n1 = len(df)
    if n1 == 0:
        return

    # 对数据进行预处理，符合文件字段的要求
    date_list = list(df.TradingDay.astype('str'))
    date_list = [x[:4]+'-'+x[4:6]+'-'+x[6:8] for x in date_list]
    df['TradingDay'] = date_list
    del df['SysID']
    df['wipe'] = 'no'
    df['before'] = ''
    df['current'] = 'secure'
    df['got'] = 'no'
    df['seq'] = 0

    # 在原文件的基础上合并新的成交记录，去重后重新写入文件
    fn0 = get_dss() + 'fut/engine/book/got_trade.csv'
    df0 = pd.read_csv(fn0)
    df0 = pd.concat([df0, df], sort=False)
    df0 = df0.drop_duplicates(subset = ['OrderID','TradeID','TradingDay'], keep='first')
    df0['seq'] = range(1, len(df0)+1)
    df0.to_csv(fn0, index=False)


def alter_book_by_rec(row):
    pnl = 0
    pos_dict = {}
    close_dict = {}
    pz = get_contract(row.InstrumentID).pz
    size = int(get_contract(row.InstrumentID).size)
    fixed_commission = int(get_contract(row.InstrumentID).fixed_commission)

    # 读取文件最新值
    fn_book = get_dss() + 'fut/engine/book/book_' + pz + '_' + row.current + '.csv'
    if os.path.exists(fn_book):
        df_book = pd.read_csv(fn_book)
        rec = df_book.iloc[-1,:]
        pnl = rec.pnl
        pos_dict = eval(rec.posDict)
        close_dict = eval(rec.closeDict)

    # 如果是book中不存在的新合约，赋初始值为交易价
    if row.InstrumentID not in close_dict:
        close_dict[row.InstrumentID] = row.Price

    # 如果是book中不存在的新合约，赋持仓初始值为0
    if row.InstrumentID not in pos_dict:
        pos_dict[row.InstrumentID] = 0

    # 更新net_pnl
    if row.Offset in ['Close', 'CloseToday']:
        if pos_dict[row.InstrumentID] == 0:
            raise ValueError
        else:
            pnl += (row.Price - close_dict[row.InstrumentID]) * pos_dict[row.InstrumentID] * size

    if row.Offset == 'Open':
        pnl += (row.Price - close_dict[row.InstrumentID]) * pos_dict[row.InstrumentID] * size


    pnl -= abs(row.Volume * fixed_commission)

    # 统一更新价格
    close_dict[row.InstrumentID] = row.Price

    # 更新pos_dict
    if row.Direction == 'Buy':
        pos_dict[row.InstrumentID] += row.Volume
    if row.Direction == 'Sell':
        pos_dict[row.InstrumentID] -= row.Volume

    df_book = pd.DataFrame([[row.TradingDay,pnl,str(pos_dict),str(close_dict)]], columns=['date','pnl','posDict','closeDict'])
    if os.path.exists(fn_book):
        df_book.to_csv(fn_book, index=False, header=None, mode='a')
    else:
        df_book.to_csv(fn_book, index=False)

def trade2book():
    """ 分析got_trade文件，按需生成book文件，往book文件中加入新的的成交记录 """
    r = []
    fn = get_dss() + 'fut/engine/book/got_trade.csv'
    df0 = pd.read_csv(fn)
    df = df0[df0.got == 'no']

    for i, row in df.iterrows():
        try:
            if row.got == 'no':
                if row.wipe == 'yes':
                    rec = row.copy()
                    rec.current = rec.before
                    rec.Volume = -rec.Volume
                    alter_book_by_rec(rec)
                alter_book_by_rec(row)
                df0.at[i,'got'] = 'yes'
        except:
            pass

    df0.to_csv(fn, index=False)

def fresh_rec_price(rec, fname):
    dss = get_dss()
    pnl = rec.pnl
    pos_dict = eval(rec.posDict)
    close_dict = eval(rec.closeDict)

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    rec.date = today

    # 从每天收盘行情切片中获取最新行情数据
    fn = dss + 'opt/' + today[:7] + '.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
        for symbol in pos_dict:
            df2 = df[df.Instrument == symbol]
            size = int(get_contract(symbol).size)
            if len(df2) > 0:
                row = df2.iloc[-1,:]
                pnl += (row.LastPrice - close_dict[symbol]) * pos_dict[symbol] * size
                close_dict[symbol] = row.LastPrice
            else:
                # 从bar文件中读取行情数据
                fn = get_dss() + 'fut/bar/day_' + symbol + '.csv'
                if os.path.exists(fn):
                    df2 = pd.read_csv(fn)
                    row = df2.iloc[-1,:]
                    pnl += (row.close - close_dict[symbol]) * pos_dict[symbol] * size
                    close_dict[symbol] = row.close

    rec.pnl = pnl
    rec.posDict = str(pos_dict)
    rec.closeDict = str(close_dict)

    df = pd.DataFrame([rec])
    df.to_csv(fname, index=False, header=None, mode='a')

def proc_rec_mature(rec, fname):
    dss = get_dss()
    pos_dict = eval(rec.posDict)
    close_dict = eval(rec.closeDict)

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    rec.date = today

    s_list = []
    # 对于已到期的期权合约(IO除外)，将持仓置为0，并清理；若有行权，添加行权记录到got_trade文件
    for symbol in pos_dict:
        pos = pos_dict[symbol]
        c = get_contract(symbol)
        # 目前期货合约暂不处理（返回值为None）；回头也返回一个到期日，过期作清零处理
        if c.mature is not None:
            # 到期的，都要进行清零处理
            if c.mature <= today:
                s_list.append(symbol)
                if c.be_opt and symbol[:2] != 'IO' and pos != 0:
                    size = c.size
                    if c.exchangeID == 'CZCE':                       # tick数据的特点，需对郑商所数据进行特殊处理
                        size = 1
                    settle = get_tick(c.basic)['AveragePrice'] / size
                    fn = get_dss() + 'fut/engine/book/got_trade.csv'
                    df = pd.read_csv(fn)
                    exercise = ['', c.exchangeID, c.basic, 'Open', int(time.time()), round(settle,4),
                                int(time.time()), '15:00:01', today, abs(pos), 'no', '',
                                'hedge' if 'hedge' in fname else 'secure', 'no', len(df)+1]

                    if (pos > 0 and c.opt_flag == 'C' and settle >= c.strike ) or \
                       (pos < 0 and c.opt_flag == 'P' and settle <= c.strike ):
                       # 以strike作为价格买开仓basic
                       exercise[0] = 'Buy'
                    if (pos > 0 and c.opt_flag == 'P' and settle <= c.strike ) or \
                       (pos < 0 and c.opt_flag == 'C' and settle >= c.strike ):
                       # 以strike作为价格卖开仓basic
                       exercise[0] = 'Sell'
                    if exercise[0] != '':
                        df = pd.DataFrame([exercise])
                        df.to_csv(fn, index=False, header=None, mode='a')

    if s_list != []:
        # 清零处理
        for s in s_list:
            if s in pos_dict:
                pos_dict.pop(s)
            if s in close_dict:
                close_dict.pop(s)
        rec.posDict = str(pos_dict)
        rec.closeDict = str(close_dict)
        df = pd.DataFrame([rec])
        df.to_csv(fname, index=False, header=None, mode='a')

def fresh_book():
    """更新book文件，计算当日盈亏"""
    # 获取opt目录下全部book文件
    dirname = get_dss() + 'fut/engine/book/'
    listfile = os.listdir(dirname)

    for filename in listfile:
        # 逐个文件更新收盘价，并计算当日盈亏
        if filename[:5] == 'book_':
            fname = dirname + filename
            df = pd.read_csv(fname)
            rec = df.iloc[-1,:]
            fresh_rec_price(rec, fname)
            proc_rec_mature(rec, fname)

def book_run():
    # 以下调用顺序不能乱！

    extract_trade()
    trade2book()
    fresh_book()

if __name__ == '__main__':
    book_run()

    pass
