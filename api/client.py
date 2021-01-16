import time
from datetime import datetime
import numpy as np
import pandas as pd
import csv
import requests
import traceback
from pprint import pprint

from nature import get_dss

ip = 'http://114.116.190.167:8000/'

def get_api_fut_bar(minx, symbol):
    try:
        response = requests.get(ip+'api_fut_bar/1?minx='+minx+'&symbol='+symbol)
        result = response.json()

        if 'detail' in result:
            print('error')
        else:
            cols = eval(result['0'])
            # print(cols)
            n = len(result)
            rows = []
            for i in range(1,n):
                rows.append( eval(result[str(i)]) )
            # print(rows[:3])
            # pprint(rows)
            df = pd.DataFrame(rows, columns=cols)
            # print(df.tail())
            return df
    except:
        s = traceback.format_exc()
        print(s)


    return None

if __name__ == '__main__':

    minx = 'day'
    # minx = 'min5'
    symbol = 'm2105'

    df = get_api_fut_bar(minx, symbol)
    print(df.tail())
