import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.family']=['simhei']              # 宋体

import os
import time
from datetime import datetime, timedelta
import talib
import fitz

from nature import get_dss, get_inx, get_contract
from nature import bsm_call_imp_vol, bsm_put_imp_vol

def show_11(dirname, df_list, title, xticks_filter=True, filename=None):
    """
    """
    plt.figure(figsize=(15,9))

    for df in df_list:
        df =  df.sort_index()
        plt.plot(df['value'], label=df.index.name)
        # title += '_' + df.index.name

    plt.title(title)
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()
    if xticks_filter == True:
        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::30]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

    if filename is None:
        fn = dirname + title + '.jpg'
    else:
        fn = dirname + filename + '.jpg'

    plt.savefig(fn)
    plt.cla()

def show_21(dirname, df_list, df2_list, code, xticks_filter=True, filename=None):
    """
    """
    plt.figure(figsize=(15,12))

    plt.subplot(2,1,1)
    title1 = code
    for df in df_list:
        df =  df.sort_index()
        plt.plot(df['value'], label=df.index.name)
        title1 += '_' + df.index.name

    plt.title(title1)
    plt.xticks([])
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()

    plt.subplot(2,1,2)
    title2 = code
    for df in df2_list:
        df =  df.sort_index()
        plt.plot(df['value'], label=df.index.name)
        title2 += '_' + df.index.name

    plt.title(title2)
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()
    if xticks_filter == True:
        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::30]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

    if filename is None:
        fn = dirname + title1 +'_' + title2 + '.jpg'
    else:
        fn = dirname + filename + '.jpg'

    plt.savefig(fn)
    plt.cla()

def img2pdf(dirname, pdfname, img_list):
    doc = fitz.open()
    for img in img_list:
        img = os.path.join(dirname, img)
        if os.path.exists(img):
            imgdoc = fitz.open(img)
            pdfbytes = imgdoc.convertToPDF()
            imgpdf = fitz.open("pdf", pdfbytes)
            doc.insertPDF(imgpdf)

    doc.save(os.path.join(dirname, pdfname))
    doc.close()

def hold_product(indicator, symbol):
    dirname = get_dss() + 'info/hold/img/'
    fn = os.path.join(get_dss(), 'info/hold/hold_'+indicator+'.csv')
    df = pd.read_csv(fn)
    date_list = sorted(set(df.date))
    date_list = date_list[-30:]
    # print(date_list)
    date_end = date_list[-1]

    df = df[(df.date.isin(date_list)) & (df.symbol == symbol) ]
    df = df.set_index('date')
    # print(df.head())

    # 生成图片
    for type in ['deal', 'long', 'short']:
        rec = df[(df.type == type) & (df.index==date_end) & (df.seq == 1)].iloc[0,:]
        name1 = rec['name']
        rec = df[(df.type == type) & (df.index==date_end) & (df.seq == 2)].iloc[0,:]
        name2 = rec['name']
        rec = df[(df.type == type) & (df.index==date_end) & (df.seq == 3)].iloc[0,:]
        name3 = rec['name']

        df0 = df[(df.type == type) & (df.seq == 0)]
        df0.index.name = '总计'
        # show_11([df0], '总计', False, type+'_total')
        df1 = df[(df.type == type) & (df.name == name1)]
        df1.index.name = name1
        df2 = df[(df.type == type) & (df.name == name2)]
        df2.index.name = name2
        df3 = df[(df.type == type) & (df.name == name3)]
        df3.index.name = name3
        # show_11([df1,df2,df3], type, False, type+'_name')
        show_21(dirname, [df0], [df1,df2,df3], symbol+'_'+type, False, indicator+'_'+type)

    # 生成pdf
    listfile = os.listdir(dirname)
    img_list = []
    for fn in listfile:
        if fn.startswith(indicator):
            img_list.append(fn)
    img2pdf(dirname, indicator+'.pdf', img_list)

def warehouse_product(kind, p_list):
    dirname = get_dss() + 'info/warehouse/img/'
    fn = os.path.join(get_dss(), 'info/warehouse/warehouse_'+kind+'.csv')
    df_a = pd.read_csv(fn)
    date_list = sorted(set(df_a.date))
    date_list = date_list[-60:]
    # print(date_list)

    for p in p_list:
        df = df_a[(df_a.date.isin(date_list)) & (df_a.symbol == p) ]
        df = df.set_index('date')
        df = df.sort_index()
        # print(df.head())
        df0 = df.copy()
        df0['value'] = df0['receipt']
        df0.index.name = 'receipt'

        if kind == 'czce':
            df1 = df.copy()
            df1['value'] = df1['forecast']
            df1.index.name = 'forecast'
            show_21(dirname, [df0], [df1], p, False, p)
        else:
            show_11(dirname, [df0], p, False, p)

    # 生成pdf
    listfile = os.listdir(dirname)
    img_list = []
    for fn in listfile:
        img_list.append(fn)
    img2pdf(dirname, 'warehouse_'+kind+'.pdf', img_list)

if __name__ == '__main__':
    # hold_product('shfe', 'al2101')
    # warehouse_product('czce', ['SR', 'CF'])
    # warehouse_product('dce', ['玉米', '豆粕'])
    warehouse_product('shfe', ['铝', '锌'])
