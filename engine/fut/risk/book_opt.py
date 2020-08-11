import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, get_contract, to_log
from nature import get_symbols_quote


def get_trade():
    """从成交反馈文件中提取期权成交记录，追加到opt_trade文件中"""

    now = datetime.now()
    today = int( now.strftime('%Y%m%d') )

    fn = get_dss() +  'fut/engine/gateway_trade.csv'
    df = pd.read_csv(fn)
    df = df[df.TradingDay == today]                 # 当日交易记录
    df = df[df.InstrumentID.str.len() >= 9]         # 期权合约
    # print(df)
    if len(df) > 0:
        df = df.drop_duplicates()
        del df['OrderID']
        del df['SysID']
        df['book'] = ''
        df['portfolio'] = ''
        df['margin'] = 0
        date_list = list(df.TradingDay.astype('str'))
        date_list = [x[:4]+'-'+x[4:6]+'-'+x[6:8] for x in date_list]
        df['TradingDay'] = date_list
        # print(df)
        fn = get_dss() + 'fut/engine/opt/opt_trade.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, header=None, mode='a')
        else:
            df.to_csv(fn, index=False)

def update_rec_price(rec):
    dss = get_dss()
    net_pnl = rec.netPnl
    pos_dict = eval(rec.posDict)
    close_dict = eval(rec.closeDict)

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    rec.date = today

    fn = dss + 'opt/' + today[:7] + '.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
        for symbol in pos_dict:
            df2 = df[df.Instrument == symbol]
            if len(df2) > 0:
                row = df2.iloc[-1,:]
                size = int(get_contract(symbol).size)
                net_pnl += (row.LastPrice - close_dict[symbol]) * pos_dict[symbol] * size
                close_dict[symbol] = row.LastPrice

    rec.netPnl = net_pnl
    rec.closeDict = str(close_dict)

def fresh_book():
    """更新book文件，计算当日盈亏"""

    # 获取opt目录下全部book文件
    dirname = get_dss() + 'fut/engine/opt/'
    listfile = os.listdir(dirname)

    for filename in listfile:
        # 逐个文件更新收盘价，并计算当日盈亏
        if filename[:5] == 'book_':
            fn = dirname + filename
            df = pd.read_csv(fn)
            rec = df.iloc[-1,:]

            update_rec_price(rec)
            df = pd.DataFrame([rec])
            df.to_csv(fn, index=False, header=None, mode='a')

def alter_book_by_rec(row):
    margin = 0
    net_pnl = 0
    pos_dict = {}
    close_dict = {}
    size = int(get_contract(row.InstrumentID).size)

    # 读取文件最新值
    fn_book = get_dss() + 'fut/engine/opt/' + row.book + '.csv'
    if os.path.exists(fn_book):
        df_book = pd.read_csv(fn_book)
        rec = df_book.iloc[-1,:]
        margin = rec.margin
        net_pnl = rec.netPnl
        pos_dict = eval(rec.posDict)
        close_dict = eval(rec.closeDict)
    margin += row.margin

    # 如果是book中不存在的新合约，赋初始值为交易价
    if row.InstrumentID not in close_dict:
        close_dict[row.InstrumentID] = row.Price

    # 如果是book中不存在的新合约，赋初始值
    if row.InstrumentID not in pos_dict:
        pos_dict[row.InstrumentID] = 0

    # 更新net_pnl
    if row.Offset == 'Open':
        if row.Direction == 'Buy':
            net_pnl += (row.Price - close_dict[row.InstrumentID]) * row.Volume * size
        if row.Direction == 'Sell':
            net_pnl -= (row.Price - close_dict[row.InstrumentID]) * row.Volume * size

    if row.Offset == 'Close':
            net_pnl -= (row.Price - close_dict[row.InstrumentID]) * pos_dict[row.InstrumentID] * size

    # 统一更新价格
    close_dict[row.InstrumentID] = row.Price

    # 更新pos_dict
    if row.Direction == 'Buy':
        pos_dict[row.InstrumentID] += row.Volume
    if row.Direction == 'Sell':
        pos_dict[row.InstrumentID] += -row.Volume

    df_book = pd.DataFrame([[row.TradingDay,margin,net_pnl,str(pos_dict),str(close_dict)]], columns=['date','margin','netPnl','posDict','closeDict'])
    if os.path.exists(fn_book):
        df_book.to_csv(fn_book, index=False, header=None, mode='a')
    else:
        df_book.to_csv(fn_book, index=False)

def trade2book():
    """ 分析opt_trade文件，往book文件中加入新的的成交记录, 关联mutual与book文件 """
    dss = get_dss()
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    r = []
    fn = dss + 'fut/engine/opt/opt_trade.csv'
    df = pd.read_csv(fn)
    for i, row in df.iterrows():
        pz = str(get_contract(row.InstrumentID).pz)
        row.book = 'book_' + pz
        row.portfolio = 'mutual'

        # 设置mutual与book文件的关系
        p = row.portfolio
        fn_p = ''
        pz = str(get_contract(row.InstrumentID).pz)
        if p[:6] == 'mutual':
            fn_p = dss + 'fut/engine/mutual/portfolio_' + p + '_' + pz + '_var.csv'
            if os.path.exists(fn_p):
                df_p = pd.read_csv(fn_p)
                if len(df_p) >= 1:
                    rec = df_p.iloc[-1,:]
                    pos_dict = eval(rec.posDict)
                    if row.book not in pos_dict:
                        pos_dict[row.book] = 1
                        rec.posDict = str(pos_dict)
                        rec.datetime = today + ' 15:00:00'
                        df_p = pd.DataFrame([rec])
                        df_p.to_csv(fn_p, index=False, header=None, mode='a')
                else:
                    r.append(row)
                    continue
            else:
                r.append(row)
                continue
        alter_book_by_rec(row)

    # 回写文件
    if r == []:
        df = pd.DataFrame([], columns=['Direction','ExchangeID','InstrumentID','Offset','Price','TradeID','TradeTime','TradingDay','Volume','book','portfolio','margin'])
    else:
        df = pd.DataFrame(r)
    df.to_csv(fn, index=False)

def book_opt_run():
    # 以下调用顺序不能乱！
    get_trade()
    trade2book()
    fresh_book()

if __name__ == '__main__':
    book_opt_run()

    pass
