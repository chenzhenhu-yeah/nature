import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, to_log, get_contract

import json
import tushare as ts

def disperse_duo(a1):
    duo_list = [212,33,55.6]
    n = len(duo_list)
    A = sum(duo_list)
    x = int( (A-n*a1)/(0.5*n*(n-1)) )
    print(x)
    r = []
    for i in range(n):
        ai = a1 + i*x
        if i == n-1:
            ai = A - sum(r)
        r.append(ai)
    print(r)


def disperse_kong(b1):
    kong_list = [212]
    n = len(kong_list)
    B = sum(kong_list)
    x = int( (n*b1-B)/(0.5*n*(n-1)) )
    print(x)
    r = []
    for i in range(n):
        bi = b1 - i*x
        if i == n-1:
            bi = B - sum(r)
        r.append(bi)
    print(r)

###################################################################################

def gather_duo():
    gap = 20
    num = 3
    duo_list = [212,33,55.6,72,98]
    duo_list = sorted(duo_list)
    n = len(duo_list)
    A = sum(duo_list)
    assert n > num
    print(duo_list)
    print(A)

    r = []
    for i in range(num):
        a = duo_list[0] + i*gap
        r.append(a)


    A_part = sum(r)
    A = A - A_part
    n = n - num
    avg = int(A/n)
    a1 = r[-1] + gap*3
    gap = avg - a1
    for i in range(n-1):
        a = a1 + i*gap
        r.append(a)
    r.append(sum(duo_list)-sum(r))
    print(r)
    print(sum(r))


def gather_kong():
    gap = 5
    num = 3
    kong_list = [2634.0, 2622.0, 2610.0, 2598.0, 2583.0]
    kong_list = sorted(kong_list, reverse=True)
    n = len(kong_list)
    A = sum(kong_list)
    assert n > num
    print(kong_list)
    print(A)

    r = []
    for i in range(num):
        a = kong_list[0] - i*gap
        r.append(a)


    A_part = sum(r)
    A = A - A_part
    n = n - num
    avg = int(A/n)
    a1 = r[-1] - gap*3
    gap = avg - a1
    for i in range(n-1):
        a = a1 - i*gap
        r.append(a)
    r.append(sum(kong_list)-sum(r))
    print(r)
    print(sum(r))



###################################################################################

def balance_point():

    for price in [1694]:
        # duo_list  = [1749.0, 1799.0, 1849.0, 1899.0, 1900, 1900, 1949.0, 1999.0, 2049.0, 2099.0, 2149.0, 2199.0, 2249.0, 2299.0, 2349.0, 2399.0, 2399.0, 2449.0, 2479.0, 2499.0, 2549.0, 2599.0, 2649.0]
        # kong_list = [1600, 1600, 1680.0, 1703.0, 1727.0]

        duo_list  = [1749.0, 1789.0, 1829.0, 1869.0, 1909.0, 1949.0, 1989.0, 2029.0, 2069.0, 2109.0, 2149.0, 2189.0, 2229.0, 2269.0, 2309.0, 2349.0, 2389.0, 2429.0, 2469.0, 2509.0, 2549.0, 2589.0, 2741.0]
        kong_list = [1727.0, 1694.0, 1661.0, 1628.0, 1600.0]

        print('\n', price )
        duo_list = [price-x for x in duo_list]
        print(sum(duo_list))
        kong_list = [x-price for x in kong_list]
        print(sum(kong_list))

        print( sum(duo_list)+sum(kong_list) )



def suo():
    price = 2315
    duo_list  = [2378.0, 2386.0, 2394.0, 2402.0, 2410.0, 2418.0, 2426.0, 2434.0, 2442.0, 2450.0, 2458.0, 2466.0, 2516.0]
    kong_list = [2201.0, 2180.0, 2159.0, 2138.0, 2117.0, 2096.0, 2075.0, 2054.0, 2033.0, 2012.0, 1991.0, 1970.0, 1916.0]

    duo_list = [price-x for x in duo_list]
    print(sum(duo_list))
    kong_list = [x-price for x in kong_list]
    print(sum(kong_list))

    print( sum(duo_list)+sum(kong_list) )

def jia():
    duo_list  = [1707.0, 1753.0, 1799.0, 1845.0, 1891.0, 1937.0, 1983.0, 2029.0, 2075.0, 2121.0, 2167.0, 2213.0, 2259.0, 2305.0, 2351.0, 2397.0, 2443.0, 2489.0, 2535.0, 2581.0, 2690.0]
    kong_list = [1709.0, 1655.0, 1601.0]

    # print(np.array(duo_list) + 84)
    # print(np.array(kong_list) + 84)

    # r = list( np.array(duo_list) + 75 )
    r = list( np.array(kong_list) + 75 )
    df = pd.DataFrame([r])
    df.to_csv('a1.csv', index=False)

###################################################################################

if __name__ == '__main__':
    pass

    # kong_adjust(200)
    # gather_duo()
    balance_point()
    # suo()
    # jia()
