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

def update_rec_price(rec):
    dss = get_dss()
    net_pnl = rec.netPnl
    pos_dict = eval(rec.posDict)
    close_dict = eval(rec.closeDict)

    now = datetime.now()
    today = now.strftime('%Y%m%d')
    rec.date = today

    for symbol in pos_dict:
        # 读 day_symbol.csv 文件，获取最新收盘价，再做其他处理
        fn = dss + 'fut/bar/day_' + symbol + '.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            row = df.iloc[-1,:]
            net_pnl += (row.close - close_dict[symbol]) * pos_dict[symbol]
            close_dict[symbol] = row.close

    rec.netPnl = net_pnl
    rec.closeDict = str(close_dict)

def fresh_book():

    # 获取opt目录下全部booking文件
    dirname = get_dss() + 'fut/engine/opt/'
    listfile = os.listdir(dirname)

    for filename in listfile:
        # print(filename)
        # 逐个文件更新收盘价，并计算当日盈亏
        if filename[:7] == 'booking':
            fn = dirname + filename
            # print(fn)
            df = pd.read_csv(fn)
            rec = df.iloc[-1,:]
            update_rec_price(rec)
            df = pd.DataFrame([rec])
            df.to_csv(fn, index=False, header=None, mode='a')


# 分析opt_trade文件，生成新的booking文件，或转成booked文件
def new_book():
    dss = get_dss()
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    r = []
    fn = dss + 'fut/engine/opt/opt_trade.csv'
    df = pd.read_csv(fn)
    for i, row in df.iterrows():
        # print( row.book, type(row.book), row.InstrumentID )
        if row.book != row.book:           # 值为nan
            r.append(row)
        else:
            # 设置daliopt等策略与booking的关系
            p = row.portfolio
            if p != p:           # 值为nan
                pass
            else:
                if p[:7] == 'daliopt':
                    fn_p = dss + 'fut/engine/daliopt/portfolio_' + p + '_var.csv'
                    df_p = pd.read_csv(fn_p)
                    rec = df_p.iloc[-1,:]
                    pos_dict = eval(rec.posDict)
                    if row.book not in pos_dict:
                        pos_dict[row.book] = 1
                        rec.posDict = str(pos_dict)
                        rec.datetime = today + ' 15:00:00'
                        df_p = pd.DataFrame([rec])
                        df_p.to_csv(fn_p, index=False, header=None, mode='a')

            fn_book = dss + 'fut/engine/opt/' + row.book + '.csv'
            if row.Offset == 'Open':
                margin = 0
                net_pnl = 0
                pos_dict = {}
                close_dict = {}
                if os.path.exists(fn_book):
                    df_book = pd.read_csv(fn_book)
                    rec = df_book.iloc[-1,:]
                    margin = rec.margin
                    net_pnl = rec.netPnl
                    pos_dict = eval(rec.posDict)
                    close_dict = eval(rec.closeDict)

                margin += row.margin
                close_dict[row.InstrumentID] = row.Price
                if row.InstrumentID not in pos_dict:
                    if row.Direction == 'Buy':
                        pos_dict[row.InstrumentID] = row.Volume
                    if row.Direction == 'Sell':
                        pos_dict[row.InstrumentID] = -row.Volume
                else:
                    if row.Direction == 'Buy':
                        pos_dict[row.InstrumentID] += row.Volume
                    if row.Direction == 'Sell':
                        pos_dict[row.InstrumentID] += -row.Volume

                df_book = pd.DataFrame([[row.TradingDay,margin,net_pnl,str(pos_dict),str(close_dict)]], columns=['date','margin','netPnl','posDict','closeDict'])
                if os.path.exists(fn_book):
                    df_book.to_csv(fn_book, index=False, header=None, mode='a')
                else:
                    df_book.to_csv(fn_book, index=False)

            if row.Offset == 'Close':
                df_book = pd.read_csv(fn_book)
                rec = df_book.iloc[-1,:]
                margin = rec.margin
                net_pnl = rec.netPnl
                pos_dict = eval(rec.posDict)
                close_dict = eval(rec.closeDict)

                net_pnl += (row.Price - close_dict[row.InstrumentID]) * pos_dict[row.InstrumentID]
                close_dict[row.InstrumentID] = row.Price
                if row.Direction == 'Buy':
                    pos_dict[row.InstrumentID] += row.Volume
                if row.Direction == 'Sell':
                    pos_dict[row.InstrumentID] += -row.Volume

                df_book = pd.DataFrame([[row.TradingDay,margin,net_pnl,str(pos_dict),str(close_dict)]], columns=['date','margin','netPnl','posDict','closeDict'])
                df_book.to_csv(fn_book, index=False, header=None, mode='a')

            # 如果已结清，转成booked文件
            booked = True
            df_book = pd.read_csv(fn_book)
            rec = df_book.iloc[-1,:]
            pos_dict = eval(rec.posDict)
            for symbol in pos_dict:
                if pos_dict[symbol] != 0:
                    booked = False

            if booked == True:
                fn_booked = fn_book.replace('booking', 'booked')
                os.rename(fn_book,fn_booked)

    if r == []:
        df = pd.DataFrame([], columns=['Direction','ExchangeID','InstrumentID','Offset','Price','TradeID','TradeTime','TradingDay','Volume','book','portfolio','margin'])
    else:
        df = pd.DataFrame(r)
    df.to_csv(fn, index=False)


# 从成交反馈文件中提取期权成交记录，追加到opt_trade文件中
def get_trade():
    now = datetime.now()
    today = int( now.strftime('%Y%m%d') )

    today =  int('20200409')

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
        # print(df)
        fn = get_dss() + 'fut/engine/opt/opt_trade.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, header=None, mode='a')
        else:
            df.to_csv(fn, index=False)

def fresh_daliopt():
    dss = get_dss()
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    pz_list = ['m','RM','MA']
    for pz in pz_list:
        fn_p = dss + 'fut/engine/daliopt/portfolio_daliopt_' + pz + '_var.csv'
        df_p = pd.read_csv(fn_p)
        rec = df_p.iloc[-1,:]
        pos_dict = eval(rec.posDict)
        value = rec.portfolioValue
        net_pnl = 0
        book_list = pos_dict.keys()
        if book in book_list:
            fn = dss + 'fut/engine/opt/' + book + '.csv'
            if os.path.exists(fn):
                # 读取booking文件
                df = pd.read_csv(fn)
                row = df.iloc[-1,:]
                net_pnl += row.netPnl
            else:
                # 读取booked文件
                fn = fn.replace('booking', 'booked')
                df = pd.read_csv(fn)
                row = df.iloc[-1,:]
                value += row.netPnl
                pos_dict.pop(book)

        rec.datetime = today + ' 15:00:00'
        rec.portfolioValue = value
        rec.netPnl = net_pnl
        rec.posDict = str(pos_dict)
        df_p = pd.DataFrame([rec])
        df_p.to_csv(fn_p, index=False, header=None, mode='a')

def book_opt_run():
    # 以下调用顺序不能乱！

    fresh_book()
    new_book()
    get_trade()
    fresh_daliopt()


if __name__ == '__main__':
    book_opt_run()
