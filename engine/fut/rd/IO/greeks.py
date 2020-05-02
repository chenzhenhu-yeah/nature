import datetime
import numpy as np
import pandas as pd
import math
from math import sqrt, log
from scipy import stats
import scipy.stats as si


from nature import get_dss, get_inx


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

def bsm_call_imp_vol(S0, K, T, r, C0, sigma_est, it=100):
    for i in range(it):
        sigma_est -= ((bsm_call_value(S0, K, T, r, sigma_est) - C0)
                     / bsm_vega(S0, K, T, r, sigma_est))
    return sigma_est

def bsm_put_imp_vol(S0, K, T, r, C0, sigma_est, it=100):
    for i in range(it):
        sigma_est -= ((bsm_put_value(S0, K, T, r, sigma_est) - C0)
                     / bsm_vega(S0, K, T, r, sigma_est))
    return sigma_est


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
    theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2))
    return theta

def calc_greeks(year):
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
        sa = 0.2
        C0 = float( row['close'] )

        if row['symbol'][:6] not in mature_dict.keys():
            assert False

        date_mature = mature_dict[ row['symbol'][:6] ]
        date_mature = datetime.datetime.strptime(date_mature, '%Y-%m-%d')
        today = datetime.datetime.strptime(row.date, '%Y-%m-%d')
        T = float((date_mature - today).days) / 365

        if T > 0:
            if call == True:
                sa = bsm_call_imp_vol(S0, K, T, r, C0, sa, it=30)
                n = 1
            else:
                sa = bsm_put_imp_vol(S0, K, T, r, C0, sa, it=30)
                n = -1

            row['hs300'] = S0
            row['delta9'] = delta(S0,K,r,T,sa,n)
            row['gamma'] = gamma(S0,K,r,T,sa)
            row['theta'] = theta(S0,K,r,T,sa,n)
            row['vega'] = vega(S0,K,r,T,sa)
            row['sigma'] = sa
        else:
            row['hs300'] = S0
            row['delta9'] = 0
            row['gamma'] = 0
            row['theta'] = 0
            row['vega'] = 0
            row['sigma'] = 0

        df = pd.DataFrame([row])
        df.to_csv(fn2, index=False, mode='a', header=None)

if __name__ == '__main__':
    year = '2020'
    calc_greeks(year)

    # S0 = 3912.57
    # K = 3900
    # T = 14/365
    # r = 0.03
    # sigma = 0.1
    #
    #
    # C0 = 71
    # # bsm = bsm_call_imp_vol(S0, K, T, r, C0, sigma, it=100)
    # bsm = bsm_put_imp_vol(S0, K, T, r, C0, sigma, it=30)
    #
    # # sigma = 0.2072
    # # sigma = 0.1986
    # # sigma = 0.1878
    # # bsm = bsm_call_value(S0, K, T, r, sigma)
    # #
    # # sigma = 0.26
    # # bsm = bsm_put_value(S0, K, T, r, sigma)
    #
    # print(bsm)
