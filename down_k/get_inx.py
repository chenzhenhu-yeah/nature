
import pandas as pd
from datetime import datetime, timedelta

def get_inx(dss,code,begin_date=None, end_date=None):
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

    fss = dss + r'inx/' + code + '.csv'
    df = pd.read_csv(fss)
    df = df[(df.date>=begin_date) & (df.date<=end_date)]

    return df


if __name__ == "__main__":
    df = get_inx(r'../../data/','399001')
    print(df.head())
