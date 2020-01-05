
import numpy as np
import pandas as pd
import os
import re
import datetime
import time
import matplotlib.pyplot as plt


from nature import get_dss


if __name__ == '__main__':


    fn = get_dss() +'backtest/fut/c/' + 'day_c2001.csv'
    #fn = get_dss() +'backtest/fut/MA/' + 'day_MA001.csv'
    #fn = get_dss() +'backtest/fut/m/' + 'day_m2001.csv'
    #fn = get_dss() +'backtest/fut/y/' + 'day_y2001.csv'
    df = pd.read_csv(fn)
    print(df.head(3))
    df['wave'] =df.close.diff(1)
    print(df.head(3))

    print( df.wave.describe() )

    # df.wave.hist(bins=100)
    # plt.show()
    #df_symbol.to_csv(fn_pz, mode='a', index=False, header=False)
