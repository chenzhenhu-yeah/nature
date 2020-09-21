import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import zipfile
import os
from datetime import datetime, timedelta

from nature import get_dss, get_contract, get_symbols_quote, to_log


def calc_d_base(date):
    # 计算并保存d_base
    r = []
    symbol_list = get_symbols_quote()
    for symbol in symbol_list:
        if symbol[:2] != 'IO':
            continue

        try:
            basic = symbol[:6]
            strike = get_contract(symbol).strike

            symbol_c = basic + '-C-' + str(strike)
            symbol_p = basic + '-P-' + str(strike)

            fn = get_dss() + 'fut/bar/day_' + symbol_c + '.csv'
            df_c =  pd.read_csv(fn)
            fn = get_dss() + 'fut/bar/day_' + symbol_p + '.csv'
            df_p =  pd.read_csv(fn)

            d_base = df_c.iat[-1,5] + df_p.iat[-1,5]
            r.append( [date, basic, strike, d_base] )
        except Exception as e:
            # s = traceback.format_exc()
            # to_log(s)
            pass

    df = pd.DataFrame(r, columns=['date','basic','strike','d_base'])
    fn = get_dss() + 'opt/sdiffer_d_base.csv'
    if os.path.exists(fn):
        df.to_csv(fn, index=False, mode='a', header=False)
    else:
        df.to_csv(fn, index=False)

def save_sdiffer(date, date_pre, basic_m0, basic_m1, atm, stat):
    fn = get_dss() + 'fut/bar/min5_' + basic_m0 + '-C-' + str(atm) + '.csv'
    df_m0_c = pd.read_csv(fn)
    df_m0_c_pre = df_m0_c[df_m0_c.date == date_pre]
    df_m0_c = df_m0_c[df_m0_c.date == date]

    fn = get_dss() + 'fut/bar/min5_' + basic_m0 + '-P-' + str(atm) + '.csv'
    df_m0_p = pd.read_csv(fn)
    df_m0_p_pre = df_m0_p[df_m0_p.date == date_pre]
    df_m0_p = df_m0_p[df_m0_p.date == date]

    fn = get_dss() + 'fut/bar/min5_' + basic_m1 + '-C-' + str(atm) + '.csv'
    if os.path.exists(fn) == False:
        return
    df_m1_c = pd.read_csv(fn)
    df_m1_c_pre = df_m1_c[df_m1_c.date == date_pre]
    df_m1_c = df_m1_c[df_m1_c.date == date]

    fn = get_dss() + 'fut/bar/min5_' + basic_m1 + '-P-' + str(atm) + '.csv'
    df_m1_p = pd.read_csv(fn)
    df_m1_p_pre = df_m1_p[df_m1_p.date == date_pre]
    df_m1_p = df_m1_p[df_m1_p.date == date]

    print(basic_m0, basic_m1)
    print(atm)
    print(df_m1_c_pre.head())
    print(df_m1_p_pre.head())

    d_base_m1 = df_m1_c_pre.iat[-1,5] + df_m1_p_pre.iat[-1,5]
    d_base_m0 = df_m0_c_pre.iat[-1,5] + df_m0_p_pre.iat[-1,5]

    df_m1_c = df_m1_c.reset_index()
    df_m1_p = df_m1_p.reset_index()
    df_m0_c = df_m0_c.reset_index()
    df_m0_p = df_m0_p.reset_index()

    df_m1_c['diff_m1'] = df_m1_c.close + df_m1_p.close - d_base_m1
    df_m0_c['diff_m0'] = df_m0_c.close + df_m0_p.close - d_base_m0
    df_m1_c['differ'] = df_m1_c.diff_m1 - df_m0_c.diff_m0

    df_m1_c['pz'] = 'IO'
    df_m1_c['basic_m0'] = basic_m0
    df_m1_c['basic_m1'] = basic_m1
    df_m1_c['atm'] = atm
    df_m1_c['diff_m0'] = df_m0_c['diff_m0']
    df_m1_c['stat'] = stat
    df2 = df_m1_c[['date','time','pz','basic_m0','basic_m1','atm','diff_m0','diff_m1','differ','stat']]
    # print(df2.head())
    fn = get_dss() + 'opt/straddle_differ.csv'

    if os.path.exists(fn):
        df2.to_csv(fn, index=False, mode='a', header=False)
    else:
        df2.to_csv(fn, index=False)

# def calc_sdiffer(date, date_pre):
def calc_sdiffer(date):
    # 当月合约临近到期日的数据只看不用
    now = datetime.strptime(date, '%Y-%m-%d')
    next_day = now + timedelta(days = 10)
    next_day = next_day.strftime('%Y-%m-%d')

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df_opt = pd.read_csv(fn)

    df = df_opt[(df_opt.pz == 'IO') & (df_opt.flag == 'm0')]
    basic_m0 = df.iat[0,1]
    mature = df.iat[0,2]

    df = df_opt[(df_opt.pz == 'IO') & (df_opt.flag == 'm1')]
    basic_m1 = df.iat[0,1]

    # 获得上一个交易日，用于确定差值的基准
    symbol_obj = 'IF' + basic_m0[2:]
    fn = get_dss() + 'fut/bar/day_' + symbol_obj + '.csv'
    df = pd.read_csv(fn)
    assert date == df.iat[-1,0]
    date_pre = df.iat[-2,0]

    # 取开盘数据的atm
    fn = get_dss() + 'fut/bar/min5_' + symbol_obj + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == date]
    df = df[df.time == '09:34:00']
    rec = df.iloc[0,:]
    obj = rec.close
    gap = 50
    # gap = 100
    atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
    # print(date, obj, atm)

    if next_day >= mature:
        stat = 'n'
    else:
        stat = 'y'
    save_sdiffer(date, date_pre, basic_m0, basic_m1, atm, stat)

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
    yymm = basic_m1[2:]
    index = yymm_list.index(yymm)
    stat = 'y'
    basic_m0 = basic_m1
    basic_m1 = 'IO' + yymm_list[index+1]
    save_sdiffer(date, date_pre, basic_m0, basic_m1, atm, stat)

    calc_d_base(date)


if __name__ == '__main__':
    calc_sdiffer('2020-09-21')

    # fn = get_dss() + 'opt/straddle_differ.csv'
    # df = pd.read_csv(fn)
    # date_list = sorted(list(set(df.date)))
    # print(date_list)
    #
    # for date in date_list:
    #     calc_d_base(date)

    pass
