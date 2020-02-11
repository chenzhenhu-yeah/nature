
import numpy as np
import pandas as pd
import os
import re
import datetime
import time

from nature import get_dss

"""
被误导了，这个脚本暂时没有用处！
"""

def glue(df_left, df_right, base):
    l = df_left.iloc[-1,]
    r = df_right.iloc[0,]
    assert l.date + l.time == r.date + r.time


    gap = l.close - r.close

    if base == 'left':
        df_right.open += gap
        df_right.high += gap
        df_right.low  += gap
        df_right.close += gap

        df = pd.concat( [df_left, df_right[1:]] )
        df.to_csv('new.csv', index=False)

    if base == 'right':
        df_left.open -= gap
        df_left.high -= gap
        df_left.low  -= gap
        df_left.close -= gap

        df = pd.concat( [df_left, df_right[1:]] )
        df.to_csv('new.csv', index=False)



if __name__ == '__main__':
    pz = 'CF'
    symbol = 'CF009'
    fn_left = '.csv'
    df_left = pd.read_csv(fn_left)
    fn_right = '.csv'
    df_right = pd.read_csv(fn_right)

    glue(df_left, df_right, 'right')
