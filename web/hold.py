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

dirname = get_dss() + 'info/NBS/img/'

def show_11(df_list, title, xticks_filter=True, filename=None):
    """
    """
    plt.figure(figsize=(15,9))

    for df in df_list:
        plt.plot(df['value'], label=df.index.name)
        # title += '_' + df.index.name

    plt.title(title)
    # plt.xticks(rotation=90)
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

def img2pdf(pdfname, img_list):
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
    fn = os.path.join(get_dss(), 'info/hold/hold_'+indicator+'.csv')
    df = pd.read_csv(fn)
    date_list = sorted(set(df.date))
    date_list = date_list[-2:]
    print(date_list)


    df = df[(df.date.isin(date_list)) & (df.seq.isin([0,1,2,3])) & (df.symbol == symbol) ]
    # print(df.head())

    for type in ['deal', 'long', 'short']:
        df0 = df[(df.type == type) & (df.seq == 0)]
        print(df0.head())



    # # 生成图片
    # for product in k_list:
    #     df0 = df[df['product'] == product]
    #     year_list = sorted(set(list(df0.year)))[-5:]
    #     for value in ['value_cur', 'value_cum']:
    #         df0['value'] = df0[value]
    #         df_list = []
    #         df2 = pd.DataFrame([[np.nan]]*12, index=['01M','02M','03M','04M','05M','06M','07M','08M','09M','10M','11M','12M'], columns=['value'])
    #         df2.index.name = ''
    #         df_list.append(df2)
    #         for year in year_list:
    #             df1 = df0[df0['year'] == year]
    #             df1 = df1.set_index('month')
    #             df1 = df1.sort_index()
    #             df1.index.name = str(year)
    #             df_list.append(df1)
    #             # print(df1)
    #         show_11(df_list, product+' '+value, xticks_filter=False, filename=indicator+'_'+product+value)
    #
    # # 生成pdf
    # listfile = os.listdir(dirname)
    # img_list = []
    # for fn in listfile:
    #     if fn.startswith(indicator):
    #         img_list.append(fn)
    # img2pdf(indicator+'.pdf', img_list)

if __name__ == '__main__':
    hold_product('shfe', 'al2101')
