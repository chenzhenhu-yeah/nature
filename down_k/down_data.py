# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
import tushare as ts
import io

from nature.down_k.down_inx import down_inx_all
from nature.down_k.down_stk_hfq import down_stk_hfq_all
from nature.down_k.down_stk_bfq import down_stk_bfq_all
from nature.down_k.down_daily import down_daily_run
from nature.down_k.down_fut import down_fut_all


def down_data(dss):
    try:
        down_inx_all(dss)
    except Exception as e:
        print('error')
        print(e)

    try:
        down_stk_hfq_all(dss)
    except Exception as e:
        print('error')
        print(e)

    try:
        down_stk_bfq_all(dss)
    except Exception as e:
        print('error')
        print(e)

    try:
        down_daily_run(dss)
    except Exception as e:
        print('error')
        print(e)

    try:
        down_fut_all(dss)        
    except Exception as e:
        print('error')
        print(e)


if __name__ == '__main__':
    down_data(r'../../data/')
