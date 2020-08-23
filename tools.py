#  -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import time
from datetime import datetime

import math
from math import sqrt, log
from scipy import stats
import scipy.stats as si

import json
from csv import DictReader
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

from nature import to_log

#----------------------------------------------------------------------
def get_dss():
    path = os.getcwd()
    i = path.find('repo')
    #print(path[:i])
    return path[:i] + 'repo\\data\\'

#----------------------------------------------------------------------
def get_repo():
    path = os.getcwd()
    i = path.find('repo')
    #print(path[:i])
    return path[:i] + 'repo\\'


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

#----------------------------------------------------------------------
def get_symbols_trade():
    # 加载品种
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    symbols = setting['symbols_trade']
    symbols_list = symbols.split(',')

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    # today = '2020-07-03'

    # 加载期权
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df1 = pd.read_csv(fn)
    strike_list = []
    for pz in ['IO', 'm', 'RM']:
        for flag in ['m0', 'm1']:
            df2 = df1[df1.pz == pz]
            df2 = df2[df2.flag == flag]
            df2 = df2[df2.mature >= today]

            for i, row in df2.iterrows():
                symbol = row.symbol
                symbols_list.append(row.obj + symbol[len(pz):])

                strike_min = int(row.strike_min1)
                strike_max = int(row.strike_max1)
                gap = int(row.gap1)
                strike_list = range(strike_min, strike_max, gap)

                if row.gap2 == row.gap2:                     # 非nan
                    print('gap2 :' + row.gap2)
                    strike_min = int(row.strike_min2)
                    strike_max = int(row.strike_max2)
                    gap = int(row.gap2)
                    strike_list += range(strike_min, strike_max, gap)

                for strike in strike_list:
                    symbols_list.append( symbol + row.dash_c + str(strike) )
                    symbols_list.append( symbol + row.dash_p + str(strike) )

    # 加载cu、al、zn
    yymm_list = ['2007','2008','2009','2010','2011','2012',
            '2101','2102','2103','2104','2105','2106','2107','2108','2109','2110','2111','2112',
            '2201','2202','2203','2204','2205','2206','2207','2208','2209','2210','2211','2212',
            '2301','2302','2303','2304','2305','2306','2307','2308','2309','2310','2311','2312',
            '2401','2402','2403','2404','2405','2406','2407','2408','2409','2410','2411','2412',
            '2501','2502','2503','2504','2505','2506','2507','2508','2509','2510','2511','2512',
            '2601','2602','2603','2604','2605','2606','2607','2608','2609','2610','2611','2612',
            '2701','2702','2703','2704','2705','2706','2707','2708','2709','2710','2711','2712',
            '2801','2802','2803','2804','2805','2806','2807','2808','2809','2810','2811','2812',
            '2901','2902','2903','2904','2905','2906','2907','2908','2909','2910','2911','2912',
           ]
    yymm = today[2:4] + today[5:7]
    index = yymm_list.index(yymm)
    for s in ['cu', 'al', 'zn']:
        for i in range(index, index+9):
            symbols_list.append( s + yymm_list[i] )

    symbols_list = list( set( symbols_list ) )
    print(sorted(symbols_list))
    return symbols_list

#----------------------------------------------------------------------
def get_symbols_quote():
    symbols_list = []
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    # today = '2020-07-03'

    try:
        # 加载 期权，对于IO还要加载IF
        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df = pd.read_csv(fn)
        for pz in ['IO', 'm', 'RM', 'MA', 'CF', 'c']:
            df2 = df[df.pz == pz]
            df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
            df2 = df2[df2.mature >= today]
            for i, row in df2.iterrows():
                symbol = row.symbol
                symbols_list.append(row.obj + symbol[len(pz):])

                strike_min = int(row.strike_min1)
                strike_max = int(row.strike_max1)
                gap = int(row.gap1)
                strike_list = range(strike_min, strike_max, gap)

                if row.gap2 == row.gap2:                     # 非nan
                    print('gap2 :' + row.gap2)
                    strike_min = int(row.strike_min2)
                    strike_max = int(row.strike_max2)
                    gap = int(row.gap2)
                    strike_list += range(strike_min, strike_max, gap)

                for strike in strike_list:
                    symbols_list.append( symbol + row.dash_c + str(strike) )
                    symbols_list.append( symbol + row.dash_p + str(strike) )

        # 加载cu、al、zn
        yymm_list = ['2007','2008','2009','2010','2011','2012',
                '2101','2102','2103','2104','2105','2106','2107','2108','2109','2110','2111','2112',
                '2201','2202','2203','2204','2205','2206','2207','2208','2209','2210','2211','2212',
                '2301','2302','2303','2304','2305','2306','2307','2308','2309','2310','2311','2312',
                '2401','2402','2403','2404','2405','2406','2407','2408','2409','2410','2411','2412',
                '2501','2502','2503','2504','2505','2506','2507','2508','2509','2510','2511','2512',
                '2601','2602','2603','2604','2605','2606','2607','2608','2609','2610','2611','2612',
                '2701','2702','2703','2704','2705','2706','2707','2708','2709','2710','2711','2712',
                '2801','2802','2803','2804','2805','2806','2807','2808','2809','2810','2811','2812',
                '2901','2902','2903','2904','2905','2906','2907','2908','2909','2910','2911','2912',
               ]

        yymm = today[2:4] + today[5:7]
        index = yymm_list.index(yymm)
        # print(index)

        for s in ['cu', 'al', 'zn']:
            for i in range(index, index+9):
                symbols_list.append( s + yymm_list[i] )
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

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
    print(symbols_list)
    return symbols_list

def send_email_old(dss, subject, content):
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


def send_email(dss, subject, content, attach_list=[]):
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

        message = MIMEMultipart()
        message['From'] = sender
        message['To'] =  receivers
        message['Subject'] = str(subject)
        message.attach(MIMEText(content, 'plain', 'utf-8'))

        if attach_list == []:
            pass
        else:
            # 构造附件
            now = datetime.now()
            today = now.strftime("%Y%m%d_")
            for i, attach in enumerate(attach_list):
                att = MIMEText(open(attach, "rb").read(), "base64", "utf-8")
                att["Content-Type"] = "application/octet-stream"
                # 附件名称非中文时的写法
                att["Content-Disposition"] = 'attachment; filename=' + today+str(i)+attach[-4:]
                # 附件名称为中文时的写法
                # att.add_header("Content-Disposition", "attachment", filename=("gbk", "", "测试结果.txt"))
                # att.add_header("Content-Disposition", "attachment", filename="测试结果.txt")
                message.attach(att)

        #smtpObj = smtplib.SMTP(mail_host, 25)                               # 生成smtpObj对象，使用非SSL协议端口号25
        smtpObj = smtplib.SMTP_SSL(mail_host, 465)                           # 生成smtpObj对象，使用SSL协议端口号465
        smtpObj.login(mail_username, mail_auth_password)                     # 登录邮箱
        smtpObj.sendmail(sender, receivers, message.as_string())             # 发送给一人
        # smtpObj.sendmail(sender, receivers.split(','), message.as_string()) # 发送给多人
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

#----------------------------------------------------------------------
# 欧式看涨期权BSM定价公式
def bsm_call_value(S0, K, T, r, sigma):
    """
    Parameters:
    ==========
    S0: float
        标的物初始价格水平
    K: float
       行权价格
    T: float
       到期日
    r: float
       固定无风险短期利率
    sigma: float
       波动因子
    Returns
    ==========
    value: float
    """
    # 为避免divide by zero，K T sigma 三个参数不能为零。
    try:
        S0 = float(S0)
        d1 = (np.log(S0 /K) + (r + 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
        d2 = (np.log(S0 /K) + (r - 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
        value = (S0 * stats.norm.cdf(d1, 0, 1) - K * np.exp(-r * T) * stats.norm.cdf(d2, 0, 1))
    except:
        value = float('nan')
        print('divide by zero')

    return value

# print( bsm_call_value(3800, 3900, 30/365, 0.01, 0.18) )

# 欧式看跌期权BSM定价公式
def bsm_put_value(S0, K, T, r, sigma):
    put_value = bsm_call_value(S0,K,T,r,sigma) - S0 + math.exp(-r * T) * K
    return put_value

    # S0 = float(S0)
    # d1 = (np.log(S0 /K) + (r + 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
    # d2 = (np.log(S0 /K) + (r - 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
    # value = K * np.exp(-r * T) * stats.norm.cdf(-d2, 0, 1) - S0 * stats.norm.cdf(-d1, 0, 1)
    # return value

# print( bsm_put_value(3800, 3900, 30/365, 0.01, 0.18) )


# 网上的算法，有些数据下易出错-------------------------------------------------------------------------
# def bsm_call_imp_vol(S0, K, T, r, C0, sigma_est, it=100):
#     for i in range(it):
#         sigma_est -= ((bsm_call_value(S0, K, T, r, sigma_est) - C0)
#                      / bsm_vega(S0, K, T, r, sigma_est))
#     return sigma_est
#
# def bsm_put_imp_vol(S0, K, T, r, C0, sigma_est, it=100):
#     for i in range(it):
#         sigma_est -= ((bsm_put_value(S0, K, T, r, sigma_est) - C0)
#                      / bsm_vega(S0, K, T, r, sigma_est))
#     return sigma_est

# 自己设计的新算法------------------------------------------------------------------------------------
def bsm_call_imp_vol(S0, K, T, r, C0):
    sigma = 0.1
    for i in range(100, 10000, 10):
        sigma = i / 1E4
        bsm = bsm_call_value(S0, K, T, r, sigma)
        # print(sigma, bsm)
        if bsm >= C0:
            break

    return sigma

def bsm_put_imp_vol(S0, K, T, r, C0):
    sigma = 0.1
    for i in range(100, 10000, 10):
        sigma = i / 1E4
        bsm = bsm_put_value(S0, K, T, r, sigma)
        if bsm >= C0:
            break

    return sigma


if __name__ == '__main__':
    get_symbols_trade()
    # get_symbols_quote()
    pass
