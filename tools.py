#  -*- coding: utf-8 -*-
import os
import pandas as pd
import time
from datetime import datetime
import json
from csv import DictReader
import smtplib
from email.mime.text import MIMEText
import traceback

from nature import to_log


def get_symbols_quote():
    symbols_list = []

    # 加载配置
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    if 'symbols_quote' in setting:
        symbols = setting['symbols_quote']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    if 'symbols_quote_01' in setting:
        symbols = setting['symbols_quote_01']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    if 'symbols_quote_05' in setting:
        symbols = setting['symbols_quote_05']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    if 'symbols_quote_06' in setting:
        symbols = setting['symbols_quote_06']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    if 'symbols_quote_09' in setting:
        symbols = setting['symbols_quote_09']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    if 'symbols_quote_10' in setting:
        symbols = setting['symbols_quote_10']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    if 'symbols_quote_12' in setting:
        symbols = setting['symbols_quote_12']
        if len(symbols) > 0:
            symbols_list += symbols.split(',')

    symbols_list = sorted(list(set(symbols_list)))
    return symbols_list

class Contract(object):
    def __init__(self,pz,size,price_tick,variable_commission,fixed_commission,slippage,exchangeID,margin):
        """Constructor"""
        self.pz = pz
        self.size = size
        self.price_tick = price_tick
        self.variable_commission = variable_commission
        self.fixed_commission = fixed_commission
        self.slippage = slippage
        self.exchangeID = exchangeID
        self.margin = margin

def get_contract(symbol):
    pz = symbol[:2]
    if pz.isalpha():
        pass
    else:
        pz = symbol[:1]

    contract_dict = {}
    filename_setting_fut = get_dss() + 'fut/cfg/setting_pz.csv'
    with open(filename_setting_fut,encoding='utf-8') as f:
        r = DictReader(f)
        for d in r:
            contract_dict[ d['pz'] ] = Contract( d['pz'],int(d['size']),float(d['priceTick']),float(d['variableCommission']),float(d['fixedCommission']),float(d['slippage']),d['exchangeID'],float(d['margin']) )

    if pz in contract_dict:
        return contract_dict[pz]
    else:
        return None
        #assert False

def send_email(dss, subject, content):
    try:

        # # 第三方 SMTP 服务
        # mail_host = 'smtp.yeah.net'              # 设置服务器
        # mail_username = 'chenzhenhu@yeah.net'   # 用户名
        # mail_auth_password = "852299"       # 授权密码

        # 加载配置
        config = open(dss+'csv/config.json')
        setting = json.load(config)
        mail_host = setting['mail_host']              # 设置服务器
        mail_username = setting['mail_username']          # 用户名
        mail_auth_password = setting['mail_auth_password']     # 授权密码
        # print(mail_host, mail_username, mail_auth_password)

        # # 第三方 SMTP 服务
        # mail_host = 'smtp.qq.com'              # 设置服务器
        # mail_username = '395772397@qq.com'   # 用户名
        # mail_auth_password = "pwqgqmexjvhbbhjd"       # 授权密码

        sender = setting['sender']
        receivers = setting['receivers']
        #receivers = '270114497@qq.com, zhenghaishu@126.com' # 多个收件人

        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = sender
        message['To'] =  receivers
        message['Subject'] = str(subject)
        #smtpObj = smtplib.SMTP(mail_host, 25)                               # 生成smtpObj对象，使用非SSL协议端口号25
        smtpObj = smtplib.SMTP_SSL(mail_host, 465)                         # 生成smtpObj对象，使用SSL协议端口号465
        smtpObj.login(mail_username, mail_auth_password)                    # 登录邮箱
        # smtpObj.sendmail(sender, receivers, message.as_string())          # 发送给一人
        smtpObj.sendmail(sender, receivers.split(','), message.as_string()) # 发送给多人
        print ("邮件发送成功")
    except smtplib.SMTPException as e:
        print ("Error: 无法发送邮件")
        print(e)
        s = traceback.format_exc()
        to_log(s)


def is_trade_time():
    #to_log('in is_trade_time')

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    weekday = int(now.strftime('%w'))
    #print(weekday)
    if 1 <= weekday <= 5:
        t = time.localtime()
        if (t.tm_hour>9 and t.tm_hour<15) or (t.tm_hour==9 and t.tm_min>16) :
            return True
    else:
        return False

def is_price_time():
    #to_log('in is_price_time')

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    weekday = int(now.strftime('%w'))
    #print(weekday)
    if 1 <= weekday <= 5:
        t = time.localtime()
        if (t.tm_hour>9 and t.tm_hour<15) or (t.tm_hour==9 and t.tm_min>31) :
            return True
    else:
        return False

#----------------------------------------------------------------------
def is_trade_day():
    #now = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
    now = datetime.now()
    today = now.strftime('%Y%m%d')
    today = datetime.strptime(today + ' ' + '00:00:00', '%Y%m%d %H:%M:%S')
    weekday = int(now.strftime('%w'))
    #print(weekday)

    if 1 <= weekday <= 5:
        return True, today
    else:
        return False, today

def get_nature_day():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    return today

#----------------------------------------------------------------------
def get_dss():
    path = os.getcwd()
    i = path.find('repo')
    #print(path[:i])
    return path[:i] + 'repo\\data\\'

#----------------------------------------------------------------------
def get_ts_code(code):
    if code[0] == '6':
        code += '.SH'
    else:
        code += '.SZ'

    return code


#----------------------------------------------------------------------
def is_market_date():
    r = True
    fn = get_dss() +  'fut/engine/market_date.csv'
    if os.path.exists(fn):
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        tm = now.strftime('%H:%M:%S')
        # print('in is_market_date, now time is: ', tm)

        df = pd.read_csv(fn)
        df = df[df.date == today]
        if len(df) > 0:
            morning_state = df.iat[0,1]
            night_state = df.iat[0,2]
            if tm > '08:30:00' and tm < '09:00:00' and morning_state == 'close':
                r = False
            if tm > '20:30:00' and tm < '21:00:00' and night_state == 'close':
                r = False

    return r


if __name__ == '__main__':
    # dss = get_dss()
    # send_email(dss, 'subject', 'content')

    pass
