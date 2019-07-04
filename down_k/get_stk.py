
import pandas as pd
from datetime import datetime, timedelta
import json
import tushare as ts

def get_adj_factor(dss,code):
    """
    获取某个股票的后复权因子
    """
    # 加载配置
    config = open(dss+'csv/config.json')
    setting = json.load(config)
    pro_id = setting['pro_id']              # 设置服务器
    pro = ts.pro_api(pro_id)

    if code[0] == '6':
        code += '.SH'
    else:
        code += '.SZ'
    df = pro.adj_factor(ts_code=code, trade_date='')
    return df

def get_stk_hfq(dss,code,begin_date=None, end_date=None):
    """
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表，降序排列
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=730)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    fss = dss + r'hfq/' + code + '.csv'
    df = None

    try:
        df = pd.read_csv(fss)
        df = df[(df.date>=begin_date) & (df.date<=end_date)]
    except:
        pass

    return df


def get_stk_bfq(dss,code,begin_date=None, end_date=None):
    """
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表，降序排列
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=730)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    fss = dss + r'bfq/' + code + '.csv'
    df = None

    try:
        df = pd.read_csv(fss)
        df = df[(df.date>=begin_date) & (df.date<=end_date)]
    except:
        pass

    return df


if __name__ == "__main__":
    get_stk_bfq(r'../../data/','300408')
