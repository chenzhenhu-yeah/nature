
import pandas as pd
import numpy as np

from nature import get_trading_dates, get_stk_hfq, K

def red3(df):
# 红三兵规则：多头趋势确认形态，开启多头攻势。（后续回调可介入）
# 1、第一根涨幅>=3%；
# 2、第二、三根收盘价高于前一日收盘价；
# 3、第二、三根开盘价必须位于前一根实体内；
# 4、连续3根阳线；
# 5、每根阳线“上下影”长度之和小于实体的50%；

# 输入：
# df：columns包括['date','open','low','high','close']
# 共4条记录，第0、1、2条分别对应第3、2、1根K线，前收盘位于第4根K线中。
# 返回：True、False
    r = False
    k0 = K(df.iloc[3])
    k1 = K(df.iloc[2])
    k2 = K(df.iloc[1])
    k3 = K(df.iloc[0])
    if k1.chg_ratio(k0.close) >= 0.03:
        if k2.close>=k1.close and k3.close>=k2.close:
            if k1.entity_has(k2.open) and k2.entity_has(k3.open):
               if k1.is_yang() and k2.is_yang() and k3.is_yang():
                   if k1.is_logo_yang() and k2.is_logo_yang() and k3.is_logo_yang():
                       r = True
    return r


def sunrise(df):
# 旭日东升规则：趋势转折形态，预期艳阳高照。（后续会有阵雨，回调可介入）
# 一、对阴线的要求
# 1、阴线收盘价是最近13日收盘最低；
# 2、跌幅>=5%；
# 二、对阳线的要求
# 1、开盘价高于阴线收盘价；
# 2、收盘价高于阴线开盘价；

# 输入：
# df：columns包括['date','open','low','high','close']
# 共14条记录，第0条记录为最新阳线，第1条记录为最新阴线，其余为之前的12根K线。
# 返回：True、False
    r = False
    k0_close_list = [df.iloc[i].close for i in range(2,14)]
    min_close = min(k0_close_list); # print(min_close)
    k0 = K(df.iloc[2])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[0])

    if k1.chg_ratio(k0.close) <= -0.05:
        if k1.close < min_close:
            if k2.open > k1.close:
                if k2.close > k1.open:
                    r = True
    return r


def dawn(df):
    # 曙光初现规则：趋势转折形态，预期艳阳高照。（后续会有阵雨，回调可介入）
    # 一、对阴线的要求
    # 1、阴线收盘价是最近13日收盘最低；
    # 2、跌幅>=5%；
    # 二、对阳线的要求
    # 1、开盘价低于阴线收盘价；
    # 2、收盘价低于阴线开盘价，吞噬阴线实体的1/2；

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共14条记录，第0条记录为最新阳线，第1条记录为最新阴线，其余为之前的12根K线。
    # 返回：True、False
    r = False
    k0_close_list = [df.iloc[i].close for i in range(2,14)]
    min_close = min(k0_close_list); # print(min_close)
    k0 = K(df.iloc[2])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[0])

    if k1.chg_ratio(k0.close) <= -0.05:
        if k1.close < min_close:
            if k2.open < k1.close:
                if k2.close < k1.open and k2.close > np.mean([k1.open,k1.close]):
                    r = True
    return r


def long_down_tail(df):
    # 空方强弩之末：趋势转折形态，短线反转买入信号。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共14条记录，第0条记录为最新K线，其余为之前的13根K线。
    # 返回：True、False

    r = False
    k1_close_list = [df.iloc[i].close for i in range(1,14)]
    min_close = min(k1_close_list); # print(min_close)
    k0 = K(df.iloc[0])

    if k0.is_long_down_tail():
        if k0.low < min_close:
            r = True
    return r

def long_up_tail(df):
    # 多方强弩之末：趋势转折形态，短线反转卖出信号。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共14条记录，第0条记录为最新K线，其余为之前的13根K线。
    # 返回：True、False

    r = False
    k1_close_list = [df.iloc[i].close for i in range(1,14)]
    max_close = max(k1_close_list); # print(min_close)
    k0 = K(df.iloc[0])

    if k0.is_long_up_tail():
        if k0.high > max_close:
            r = True
    return r

def up_overlap_up(df):
    # 多加多叠区：趋势转折形态，主力底部建仓信号。
    # 1、最新一根K线为标志性阳线；
    # 2、远端K线为标志性阳线，收盘为近期最低；
    # 3、中间不超过7根K线，且收盘不破位，总共多至9根K线；
    # 4、首尾两根阳线有叠区，叠区占比远端K线实体比例不少于2/3.

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共9条记录，第0条记录为最新K线，其余为之前的8根K线。
    # 返回：True、False

    r = False
    k1_close_list = [df.iloc[i].close for i in range(1,9)]
    min_close = min(k1_close_list); # print(min_close)
    k0 = K(df.iloc[0])

    if k0.is_logo_yang() and k0.open>min_close:
        for i in range(2,9):
            if df.iloc[i].close == min_close: #第i根为起始k线
                ki = K(df.iloc[i])
                if ki.is_logo_yang():
                    if k0.close>ki.close:
                        if (ki.close-k0.open)/(ki.close-ki.open)>0.67:
                            r = True
                            break
    return r


def down_overlap_up(df):
    # 空加多叠区：趋势转折形态，主力底部建仓信号。
    # 1、最新一根K线为标志性阳线；
    # 2、远端K线为标志性阴线，收盘为近期最低；
    # 3、中间不超过5根K线，且收盘不破位，总共多至7根K线；
    # 4、首尾两根K线有叠区，叠区占比远端K线实体比例不少于2/3.

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共7条记录，第0条记录为最新K线，其余为之前的6根K线。
    # 返回：True、False

    r = False
    k1_close_list = [df.iloc[i].close for i in range(1,7)]
    min_close = min(k1_close_list); # print(min_close)
    k0 = K(df.iloc[0])

    if k0.is_logo_yang() and k0.open>min_close:
        for i in range(2,7):
            if df.iloc[i].close == min_close: #第i根为起始k线
                ki = K(df.iloc[i])
                if ki.is_logo_yin():
                    if k0.close>ki.open:
                        if (ki.open-k0.open)/(ki.open-ki.close)>0.67:
                            r = True
                            break
    return r

def up_swallow(df):
    # 看涨吞没：趋势确认形态，调整结束，继续看涨。
    # 1、最新一根K线为标志性阳线；
    # 2、次新一根K线为阴线，收盘为近5日最低；
    # 3、阳线开盘小于等于阴线收盘；
    # 4、阳线收盘高于阴线最高及前K线收盘.

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共6条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k1_close_list = [df.iloc[i].close for i in range(1,6)]
    min_close = min(k1_close_list); # print(min_close)
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])

    if k0.is_logo_yang():
        if k1.is_yin() and k1.close == min_close:
            if k0.open <= k1.close:
                if k0.close > max(k1.high, k2.close):
                    r = True
    return r

def down_swallow(df):
    # 看跌吞没：趋势确认形态，调整结束，继续看跌。
    # 1、最新一根K线为标志性阴线；
    # 2、次新一根K线为阳线，收盘为近5日最高；
    # 3、阴线开盘大于等于阳线收盘；
    # 4、阴线收盘低于阳线最低及前K线收盘.

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共6条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k1_close_list = [df.iloc[i].close for i in range(1,6)]
    max_close = max(k1_close_list); # print(min_close)
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])

    if k0.is_logo_yin():
        if k1.is_yan() and k1.close == max_close:
            if k0.open >= k1.close:
                if k0.close < min(k1.low, k2.close):
                    r = True
    return r

def pregnant_yang(df):
    # 孕线怀阳：趋势转折形态，变盘信号，多方出现反击，对失地有部分夺回。
    # 1、处在下跌趋势中，30日线标识趋势，均线呈空头排列；
    # 2、下跌趋势的母体是阴线，幅度5%以上，实体3%以上；
    # 3、子体为阳线，子体最高价最低价必须包含在母体的实体内。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共31条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k1_close_30 = [df.iloc[i].close for i in range(1,31)]
    mean_close_30 = np.mean(k1_close_30);
    k1_close_10 = [df.iloc[i].close for i in range(1,11)]
    mean_close_10 = np.mean(k1_close_10);
    k1_close_5 = [df.iloc[i].close for i in range(1,6)]
    mean_close_5 = np.mean(k1_close_5);
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])

    if mean_close_30>mean_close_10 and mean_close_10>mean_close_5:
        if k1.is_logo_yin() and k1.chg_ratio(k2.close)<-0.05:
            if k0.is_yang() and k0.low > k1.close and k0.high < k1.open:
                r = True
    return r


def pregnant_yin(df):
    # 孕线怀阴：趋势转折形态，变盘信号，多方出现反击，对失地有部分夺回。
    # 1、处在上涨趋势中，30日线标识趋势，均线呈多头排列；
    # 2、上涨趋势的母体是阳线，幅度5%以上，实体3%以上；
    # 3、子体为阴线，子体最高价最低价必须包含在母体的实体内。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共31条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k1_close_30 = [df.iloc[i].close for i in range(1,31)]
    mean_close_30 = np.mean(k1_close_30);
    k1_close_10 = [df.iloc[i].close for i in range(1,11)]
    mean_close_10 = np.mean(k1_close_10);
    k1_close_5 = [df.iloc[i].close for i in range(1,6)]
    mean_close_5 = np.mean(k1_close_5);
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])

    if mean_close_30<mean_close_10 and mean_close_10<mean_close_5:
        if k1.is_logo_yang() and k1.chg_ratio(k2.close)>0.05:
            if k0.is_yin() and k0.low > k1.close and k0.high < k1.open:
                r = True
    return r

def up_pao(df):
    # 多方炮：趋势确认形态，识别主力加仓，空中加油。
    # 1、三根K线，2阳夹1阴；
    # 2、阳线为标志性大阳线，实体3%以上；
    # 3、阴线跌幅3%以内。
    # 需要配合筹码分析，判断是空中加油还是诱多。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共3条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])

    if k0.is_yang() and k1.is_yin() and k2.is_yang():
        if k0.is_logo_yang() and (k0.close/k0.open)>1.03 and k2.is_logo_yang() and (k2.close/k2.open)>1.03 :
            if k1.chg_ratio(k2.close)>-0.03:
                r = True
    return r

def down_pao(df):
    # 空方炮：趋势确认形态，识别主力减仓。
    # 1、三根K线，2阴夹1阳；
    # 2、阴线为标志性大阴线，实体3%以上；
    # 3、阳线涨幅3%以内。
    # 需要配合筹码分析，判断是空中加油还是诱多。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共3条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])

    if k0.is_yin() and k1.is_yang() and k2.is_yin():
        if k0.is_logo_yin() and (k0.close/k0.open)>0.97 and k2.is_logo_yin() and (k2.close/k2.open)>0.97 :
            if k1.chg_ratio(k2.close)<0.03:
                r = True
    return r

def up_3tricks(df):
    # 上升三法：趋势确认形态，多头趋势已形成，上升中继隔暴露继续做多意愿。
    # 1、处在上涨趋势中，5、10、30日均线呈多头排列；
    # 2、共5根K线，第1根和最后1根为阳线，其余为阴线；
    # 3、第1根涨5%以上，最后1根涨3%以上。
    # 4、阴线收盘不能低于第1根阳线最低价。
    # 5、筹码最好为扩散形态，且上方无筹码。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共31条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k1_close_30 = [df.iloc[i].close for i in range(1,31)]
    mean_close_30 = np.mean(k1_close_30);
    k1_close_10 = [df.iloc[i].close for i in range(1,11)]
    mean_close_10 = np.mean(k1_close_10);
    k1_close_5 = [df.iloc[i].close for i in range(1,6)]
    mean_close_5 = np.mean(k1_close_5);
    k0 = K(df.iloc[0])
    k1 = K(df.iloc[1])
    k2 = K(df.iloc[2])
    k3 = K(df.iloc[3])
    k4 = K(df.iloc[4])
    k5 = K(df.iloc[5])

    if mean_close_30<mean_close_10 and mean_close_10<mean_close_5:
        if k0.is_yang() and k1.is_yin() and k2.is_yin() and k3.is_yin() and k4.is_yang():
            if k4.chg_ratio(k5.close) and k0.chg_ratio(k1.close):
                if k3.close>k4.low and k2.close>k4.low and k1.close>k4.low:
                    r = True
    return r

def top_island(df):
    # 顶部岛型反转：趋势转折形态。
    # 1、首尾两个缺口，两个缺口有重叠空间；
    # 2、岛不超过30个交易日；
    # 辅助：成交量的活跃度不能明显低于岛形成之前。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共32条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k0 = K(df.iloc[0])

    for i in range(2,32):
        ki = K(df.iloc[i])
        k1_low_list = [df.iloc[n].low for n in range(1,i)]
        min_low = min(k1_low_list)
        if k0.high < min_low and ki.high < min_low:
            r = True
            break
    return r

def bottom_island(df):
    # 底部岛型反转：趋势转折形态。
    # 1、首尾两个缺口，两个缺口有重叠空间；
    # 2、岛不超过30个交易日；
    # 辅助：成交量的活跃度不能明显低于岛形成之前。

    # 输入：
    # df：columns包括['date','open','low','high','close']
    # 共32条记录，第0条记录为最新K线。
    # 返回：True、Falsepass

    r = False
    k0 = K(df.iloc[0])

    for i in range(2,32):
        ki = K(df.iloc[i])
        k1_high_list = [df.iloc[n].high for n in range(1,i)]
        max_high = max(k1_high_list)
        if k0.low > max_high and ki.low > max_high:
            r = True
            break
    return r


def k_pattern(df):
    '''
    输入：
        df：columns包括['date','open','low','high','close']
        记录按日期降序排列，第0条记录为当前分析日期。
    '''

    r =[]
    # bottom_island
    if bottom_island(df):
        r.append('bottom_island: ')

    # top_island
    if top_island(df):
        r.append('top_island: ')

    # up_3tricks
    if up_3tricks(df):
        r.append('up_3tricks: ')

    # down_pao
    if down_pao(df):
        r.append('down_pao: ')

    # up_pao
    if up_pao(df):
        r.append('up_pao: ')

    # pregnant_yin
    df1 = df[:31]
    if pregnant_yin(df1):
        r.append('pregnant_yin: ')

    # pregnant_yang
    df1 = df[:31]
    if pregnant_yang(df1):
        r.append('pregnant_yang: ')

    # down_overlap_up
    df1 = df[:9]
    if down_overlap_up(df1):
        r.append('down_overlap_up: ')

    # up_overlap_up
    df1 = df[:9]
    if up_overlap_up(df1):
        r.append('up_overlap_up: ')

    # red3
    df1 = df[:4]
    if red3(df1):
        r.append('red3: ')

    # sunrise
    df1 = df[:14]
    if sunrise(df1):
        r.append('sunrise: ')

    # dawn
    df1 = df[:14]
    if dawn(df1):
        r.append('dawn: ')

    # long_down_tail
    df1 = df[:14]
    if long_down_tail(df1):
        r.append('long_down_tail: ')

    # long_up_tail
    df1 = df[:14]
    if long_up_tail(df1):
        r.append('long_up_tail: ')

    return r

def signal_k_pattern(dss,codes, _date):
    r = []
    for code in codes:
        df = get_stk_hfq(dss,code,None,_date)
        item_list = k_pattern(df)
        r += [x+code for x in item_list]
    return r

if __name__ == "__main__":
    #df = pd.read_csv('002570.csv') #,dtype='str',encoding='gbk')
    #df = pd.read_csv('002454.csv')
    # df = pd.read_csv('002792.csv')
    k_pattern(None)
