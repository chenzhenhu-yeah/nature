import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import math
from math import sqrt, log
from scipy import stats
import scipy.stats as si

from nature import get_dss, get_inx

# import warnings
# warnings.filterwarnings('error')


# 欧式期权BSM定价公式
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
    S0 = float(S0)
    d1 = (np.log(S0 /K) + (r + 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
    d2 = (np.log(S0 /K) + (r - 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
    value = (S0 * stats.norm.cdf(d1, 0, 1) - K * np.exp(-r * T) * stats.norm.cdf(d2, 0, 1))
    return value

# 欧式看跌期权BSM定价公式
def bsm_put_value(S0, K, T, r, sigma):
    put_value = bsm_call_value(S0,K,T,r,sigma) - S0 + math.exp(-r * T) * K
    return put_value

    # S0 = float(S0)
    # d1 = (np.log(S0 /K) + (r + 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
    # d2 = (np.log(S0 /K) + (r - 0.5 * sigma**2) * T )/(sigma * np.sqrt(T))
    # value = K * np.exp(-r * T) * stats.norm.cdf(-d2, 0, 1) - S0 * stats.norm.cdf(-d1, 0, 1)
    # return value


def bsm_vega(S0, K, T, r, sigma):
    """
    Vega 计算
    """
    S0 = float(S0)
    d1 = (np.log(S0/K)) + (r+0.5*sigma**2)*T /(sigma*sqrt(T))
    vega = S0 * stats.norm.cdf(d1, 0, 1) * np.sqrt(T)
    return vega

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
    sigma = 0
    for i in range(100, 10000, 1):
        sigma = i / 1E4
        bsm = bsm_call_value(S0, K, T, r, sigma)
        if bsm >= C0:
            break

    return sigma

def bsm_put_imp_vol(S0, K, T, r, C0):
    sigma = 0
    for i in range(100, 10000, 1):
        sigma = i / 1E4
        bsm = bsm_put_value(S0, K, T, r, sigma)
        if bsm >= C0:
            break

    return sigma


# ------------------------------------------------------------------------------------

def d(s,k,r,T,sigma):
    d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return (d1,d2)

def delta(s,k,r,T,sigma,n):
    d1 = d(s,k,r,T,sigma)[0]
    delta0 = n * si.norm.cdf(n * d1)
    return delta0


def gamma(s,k,r,T,sigma):
    d1 = d(s,k,r,T,sigma)[0]
    gamma = si.norm.pdf(d1) / (s * sigma * np.sqrt(T))
    return gamma

def vega(s,k,r,T,sigma):
    d1 = d(s,k,r,T,sigma)[0]
    vega = (s * si.norm.pdf(d1) * np.sqrt(T)) / 100
    return vega

def theta(s,k,r,T,sigma,n):
    d1 = d(s,k,r,T,sigma)[0]
    d2 = d(s,k,r,T,sigma)[1]

    # theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2)) / 365
    theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2)) / 100
    return theta

def calc_greeks(year):
    c = input('请确认波指已维护最新值，press y to continue: ')
    if c != 'y':
        return

    fn_sigma = get_dss() + 'backtest/IO/hs300波指.csv'
    df_sigma = pd.read_csv(fn_sigma)
    df_sigma = df_sigma.set_index('date')

    mature_dict = {'IO2002':'2020-02-21','IO2003':'2020-03-20','IO2004':'2020-04-17','IO2005':'2020-05-15','IO2006':'2020-06-19',
                   'IO2007':'2020-07-17','IO2009':'2020-09-18','IO2012':'2020-12-18','IO2103':'2021-03-19'}

    fn1 = get_dss() + 'backtest/IO/' + 'IO' + year + '.csv'
    fn2 = get_dss() + 'backtest/IO/' + 'IO' + year + '_greeks.csv'

    df1 = pd.read_csv(fn1)
    df2 = pd.read_csv(fn2)
    df1 = df1.loc[len(df2):,:]
    # print(df1.head(3))

    for i, row in df1.iterrows():
        df_index = get_inx('000300', row.date, row.date)
        if len(df_index) == 0:
            print('缺少沪深300日线数据')
            assert False
        S0 = df_index.iat[0,3]
        # S0 = 3912.57

        call = True if row['symbol'][7] == 'C' else False
        K = float( row['symbol'][-4:] )
        r = 0.03
        C0 = float( row['close'] )

        sa = float( df_sigma.at[row.date, 'close'] ) / 100
        # sa = 0.2178
        # print(sa)

        if row['symbol'][:6] not in mature_dict.keys():
            assert False

        date_mature = mature_dict[ row['symbol'][:6] ]
        date_mature = datetime.datetime.strptime(date_mature, '%Y-%m-%d')
        today = datetime.datetime.strptime(row.date, '%Y-%m-%d')
        T = float((date_mature - today).days) / 365

        if T > 0:
            if call == True:
                n = 1
                iv = bsm_call_imp_vol(S0, K, T, r, C0)
            else:
                n = -1
                iv = bsm_put_imp_vol(S0, K, T, r, C0)

            row['hs300'] = S0
            row['sigma'] = sa
            row['delta9'] = delta(S0,K,r,T,sa,n)
            row['gamma'] = gamma(S0,K,r,T,sa)
            row['theta'] = theta(S0,K,r,T,sa,n)
            row['vega'] = vega(S0,K,r,T,sa)
            row['iv'] = iv
        else:
            row['hs300'] = S0
            row['delta9'] = 0
            row['gamma'] = 0
            row['theta'] = 0
            row['vega'] = 0
            row['sigma'] = 0

        df = pd.DataFrame([row])
        df.to_csv(fn2, index=False, mode='a', header=None)

def test_iv():
    r = 0.03
    S0 = 4489

    K = 4600
    T = 10/365

    C0 = 64.8
    sa = bsm_call_imp_vol(S0, K, T, r, C0)

    # C0 = 71.4
    # sa =  bsm_put_imp_vol(S0, K, T, r, C0)

    print(sa)

def test_greeks():
    S0 = 2.8
    K = 2.65
    T = 30/365
    r = 0.03
    sigma = 0.2
    sa = sigma
    C0 = 213.6

    bsm = bsm_call_value(S0, K, T, r, C0)
    print(bsm)

    n = 1
    print( delta(S0,K,r,T,sa,n) )
    print( gamma(S0,K,r,T,sa) )
    print( theta(S0,K,r,T,sa,n) )
    print( vega(S0,K,r,T,sa) )

def export_data(dt):
    r = []
    symbol_list = ['IO2005-C-3700', 'IO2005-P-3700', 'IO2005-C-3800', 'IO2005-P-3800', 'IO2005-C-3900', 'IO2005-P-3900', 'IO2005-C-4000', 'IO2005-P-4000', 'IO2005-C-4100', 'IO2005-P-4100', 'IO2006-C-3900', 'IO2006-P-3900']
    # symbol_list = []

    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '_greeks.csv'
    df = pd.read_csv(fn)

    for symbol in symbol_list:
        df1 = df[ (df.date == dt) & (df.symbol == symbol) ]
        row = df1.iloc[0,:]
        r += [row.close, row.delta9, row.gamma, 100*row.theta, 100*row.vega, 100*row.iv]

    # df = pd.DataFrame(r, columns=['date', 'symbol', 'close', 'delta9', 'gamma', 'theta', 'vega', 'iv'])
    df = pd.DataFrame([r])
    fn = get_dss() + 'backtest/IO/a3.csv'
    df.to_csv(fn, index=False)

def pcp():
    r = 0.03
    T = 15/365
    S = 3912

    result = []
    dt = '2020-04-30'
    x_list = range(3150,4700,50)
    # print(*s_list)

    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '.csv'
    df = pd.read_csv(fn)
    # print(df.head())

    for x in x_list:
        symbol_c = 'IO2005-C-' + str(x)
        symbol_p = 'IO2005-P-' + str(x)
        # print(symbol_c)
        df_c = df[(df.date == dt) & (df.symbol == symbol_c)]
        df_p = df[(df.date == dt) & (df.symbol == symbol_p)]
        row_c = df_c.iloc[0,:]
        row_p = df_p.iloc[0,:]
        pSc = int( row_p.close + S -row_c.close )
        pSc_final = int( pSc * (1 + r*T) )
        result.append( [S, row_c.close, row_p.close, x, pSc, pSc_final, x-pSc_final] )

    df = pd.DataFrame(result, columns=['S', 'call', 'put', 'X', 'pSc', 'pSc_final', 'diff'])
    fn = get_dss() + 'backtest/IO/p1.csv'
    df.to_csv(fn, index=False)

def die_forward_call(dt):
    # 正向蝶式，若权利金支出<0，则存在无风险套利
    result = []
    x_list = range(3150,4650,50)
    # print(*s_list)

    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == dt]
    df = df.set_index('symbol')

    for x in x_list:
        s1 = 'IO2005-C-' + str(x)
        s2 = 'IO2005-C-' + str(x+50)
        s3 = 'IO2005-C-' + str(x+50+50)
        if x+50+50 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    for x in x_list:
        s1 = 'IO2005-C-' + str(x)
        s2 = 'IO2005-C-' + str(x+100)
        s3 = 'IO2005-C-' + str(x+100+100)
        if x+100+100 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    for x in x_list:
        s1 = 'IO2005-C-' + str(x)
        s2 = 'IO2005-C-' + str(x+150)
        s3 = 'IO2005-C-' + str(x+150+150)
        if x+150+150 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    for x in x_list:
        s1 = 'IO2005-C-' + str(x)
        s2 = 'IO2005-C-' + str(x+200)
        s3 = 'IO2005-C-' + str(x+200+200)
        if x+200+200 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    df = pd.DataFrame(result, columns=['s1', 'c1', 's2', 'c2', 's3', 'c3', 'cost'])
    fn = get_dss() + 'backtest/IO/d1.csv'
    df.to_csv(fn, index=False)


def die_forward_put(dt):
    # 正向蝶式，若权利金支出<0，则存在无风险套利
    result = []
    x_list = range(3150,4650,50)
    # print(*s_list)

    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == dt]
    df = df.set_index('symbol')

    for x in x_list:
        s1 = 'IO2005-P-' + str(x)
        s2 = 'IO2005-P-' + str(x+50)
        s3 = 'IO2005-P-' + str(x+50+50)
        if x+50+50 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    for x in x_list:
        s1 = 'IO2005-P-' + str(x)
        s2 = 'IO2005-P-' + str(x+100)
        s3 = 'IO2005-P-' + str(x+100+100)
        if x+100+100 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    for x in x_list:
        s1 = 'IO2005-P-' + str(x)
        s2 = 'IO2005-P-' + str(x+150)
        s3 = 'IO2005-P-' + str(x+150+150)
        if x+150+150 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    for x in x_list:
        s1 = 'IO2005-P-' + str(x)
        s2 = 'IO2005-P-' + str(x+200)
        s3 = 'IO2005-P-' + str(x+200+200)
        if x+200+200 > 4650:
            break

        c1 = df.at[s1,'close']
        c2 = df.at[s2,'close']
        c3 = df.at[s3,'close']
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [s1, c1, s2,c2, s3,c3,cost] )

    df = pd.DataFrame(result, columns=['s1', 'c1', 's2', 'c2', 's3', 'c3', 'cost'])
    fn = get_dss() + 'backtest/IO/d2.csv'
    df.to_csv(fn, index=False)


def calc_hv():
    # df = get_inx('000300', '2019-06-01', '2020-07-15')
    df = get_inx('000300', '2019-06-01')
    # df = df.sort_values('date')
    df = df.set_index('date')
    df = df.sort_index()

    df['ln'] = np.log(df.close)
    df['rt'] = df['ln'].diff(1)
    df['hv'] = df['rt'].rolling(20).std()
    df['hv'] *= np.sqrt(242)

    df = df.iloc[-242:,:]
    print(df.head())
    print(df.tail())

    cur = df.iloc[-1,:]
    hv = cur['hv']
    hv_rank = (hv - df['hv'].min()) / (df['hv'].max() - df['hv'].min())
    print('hv: ', hv)
    print('hv Rank: ', hv_rank)

    # print('     均值：',df['hv'].mean())
    print('0.2分位数:',np.percentile(df['hv'], 20))
    print('0.5分位数:',np.percentile(df['hv'], 50))
    print('0.8分位数:',np.percentile(df['hv'], 80))

    hv_percentile = len(df[df.hv < hv]) / 242
    print('hv percentile: ', hv_percentile)

    plt.figure(figsize=(13,7))
    plt.xticks(rotation=45)
    plt.plot(df['hv'])
    plt.grid(True, axis='y')

    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::30]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    plt.show()


if __name__ == '__main__':
    year = '2020'
    dt = '2020-05-13'

    # calc_hv()

    # calc_greeks(year)

    test_iv()
    # test_greeks()
    # export_data(dt)

    # pcp()
    # die_forward_call(dt)
    # die_forward_put(dt)
