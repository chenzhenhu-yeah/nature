
import pandas as pd
from datetime import datetime, timedelta

def get_trading_dates(dss, begin_date=None, end_date=None):
    """
    获取指定日期范围的按照正序排列的交易日列表
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表，升序排列
    """
    # 开始日期，默认今天向前的365个自然日
    now = datetime.now()
    if begin_date is None:
        one_year_ago = now - timedelta(days=365)
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    fss = dss + 'inx/000001.csv'
    df = pd.read_csv(fss, dtype='str')
    df = df[(df.date>=begin_date) & (df.date<=end_date)]
    dates = sorted(list(df['date']))
    #print(dates)

    return dates


if __name__ == "__main__":
    get_trading_dates(r'../../data/')
