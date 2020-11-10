
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import os
import fitz

from nature import get_dss

def draw(commodity, use_col, country, col_labels, df_list, legend_list):
    plt.figure(figsize=(15,7))
    plt.title(commodity + '_' + use_col + '_' + country)

    for i in range(len(df_list)):
        df = df_list[i]
        df = df.iloc[:52,:]
        df = df.reset_index()
        plt.plot(df[use_col],label=legend_list[i])

    row_labels = []
    table_vals = []
    for i in range(len(df)-3, len(df)):
        row_labels.append(df.at[i,'Date'])
        table_vals.append(df.loc[i,col_labels])
    my_table = plt.table(cellText=table_vals, rowLabels=row_labels, colLabels=col_labels, loc='bottom')
    # print(df.head())

    # plt.subplots_adjust(left=0.2, bottom=0.2)

    plt.grid(True, axis='y')
    plt.xticks([])
    plt.legend()

    fn =commodity + '_' + use_col + '_' + country + '.jpg'
    plt.savefig(fn)

#------------------------------------------------------------------------------
def commoditys(df):
    my_dict = {}
    my_dict['Soybeans'] = '-09-01'
    my_dict['Corn'] = '-09-01'
    my_dict['All Upland Cotton'] = '-08-01'
    year_list = ['2021','2020','2019','2018','2017','2016','2015']

    for commodity in ['Soybeans', 'Corn', 'All Upland Cotton']:
        df0 = df[df.Commodity == commodity]
        df0 = df0.set_index('Date')

        df_dict = {}
        df_dict['CHINA'] = df0[df0.Country.isin(['CHINA, PEOPLES REPUBLIC OF',])]
        df_dict['UNKNOWN'] = df0[df0.Country.isin(['UNKNOWN',])]
        df_dict['TOTAL'] = df0[df0.Country.isin(['GRAND TOTAL',])]
        # df_dict['CHINA_UNKNOWN'] = df_dict['CHINA'] + df_dict['UNKNOWN']
        # df_dict['CHINA_UNKNOWN']['MarketYear'] = df_dict['CHINA']['MarketYear']
        # print(df_dict['CHINA_UNKNOWN'].tail())

        for country in ['CHINA', 'UNKNOWN', 'TOTAL']:
            df1 = df_dict[country]
            df1.AccumulatedExports = df1.AccumulatedExports / 1E4
            df1.AccumulatedExports = df1.AccumulatedExports.astype('int')
            df1.GrossNewSales = df1.GrossNewSales / 1E4
            df1.GrossNewSales = df1.GrossNewSales.astype('int')
            df1.NetSales = df1.NetSales / 1E4
            df1.NetSales = df1.NetSales.astype('int')
            df1.TotalCommitment = df1.TotalCommitment / 1E4
            df1.TotalCommitment = df1.TotalCommitment.astype('int')
            df1.NMY_OutstandingSales = df1.NMY_OutstandingSales / 1E4
            df1.NMY_OutstandingSales = df1.NMY_OutstandingSales.astype('int')

            df1_T = df1[(df1.index < year_list[0]+my_dict[commodity]) & (df1.index >= year_list[1]+my_dict[commodity]) & (df1.MarketYear != 'ENDING MY')]
            df1_T_1 = df1[(df1.index < year_list[1]+my_dict[commodity]) & (df1.index >= year_list[2]+my_dict[commodity]) & (df1.MarketYear != 'ENDING MY')]
            df1_T_2 = df1[(df1.index < year_list[2]+my_dict[commodity]) & (df1.index >= year_list[3]+my_dict[commodity]) & (df1.MarketYear != 'ENDING MY')]
            df1_T_3 = df1[(df1.index < year_list[3]+my_dict[commodity]) & (df1.index >= year_list[4]+my_dict[commodity]) & (df1.MarketYear != 'ENDING MY')]
            df1_T_4 = df1[(df1.index < year_list[4]+my_dict[commodity]) & (df1.index >= year_list[5]+my_dict[commodity]) & (df1.MarketYear != 'ENDING MY')]

            # df1 = df1[df1.Country.isin(['GRAND TOTAL'])]

            # print(df1_T)
            col_labels = ['AccumulatedExports', 'GrossNewSales', 'NetSales', 'TotalCommitment', 'NMY_OutstandingSales']
            df_list = [df1_T_4, df1_T_3, df1_T_2, df1_T_1, df1_T]
            legend_list = ['2016','2017','2018','2019','2020']
            draw(commodity, 'TotalCommitment', country, col_labels, df_list, legend_list)
            draw(commodity, 'NetSales', country,  col_labels, df_list, legend_list)


#------------------------------------------------------------------------------
def entry():
    fn = 'ExportSalesDataByCommodity_soybeans_corn_cotton.xls'
    # fn2 = get_dss() +'backtest/bar/day_m2009.csv'

    df1 = pd.read_excel(fn, sheet_name='Sheet1')
    df1 = df1.dropna(how='all')                     # 该行全部元素为空时，删除该行
    df1 = df1.dropna(axis=1, how='all')             # 该列全部元素为空时，删除该列
    df1 = df1.reset_index()

    # print(df1.head())
    # print(dir( df1.head().style ))
    # df1.head().style.to_excel('e1.xls')
    # plt.show()
    # print(df1.tail())
    # print(len(df1))

    df2 = pd.read_excel(fn, sheet_name='Sheet2')
    df2 = df2.dropna(how='all')
    df2 = df2.dropna(axis=1, how='all')
    df2 = df2.reset_index()

    # print(df2.head())
    # print(df2.tail())
    # print(len(df2))


    df = pd.concat([df1,df2], sort=False)
    df = df[df.Country.isin(['CHINA, PEOPLES REPUBLIC OF', 'UNKNOWN', 'GRAND TOTAL'])]
    # print(df.head())
    # print(df.tail())
    # print(len(df))
    # print(df.dtypes)

    commoditys(df)

def jpg2pdf():
    img_dict = {}
    img_dict['soybeans'] = []
    img_dict['cotton'] = []
    img_dict['corn'] = []

    listfile = os.listdir()
    for fn in listfile:
        if fn.endswith('.jpg') and fn.startswith('Soybeans'):
            img_dict['soybeans'].append(fn)
        if fn.endswith('.jpg') and fn.startswith('All Upland Cotton'):
            img_dict['cotton'].append(fn)
        if fn.endswith('.jpg') and fn.startswith('Corn'):
            img_dict['corn'].append(fn)

    for c in img_dict.keys():
        doc = fitz.open()
        for img in img_dict[c]:
            imgdoc = fitz.open(img)
            pdfbytes = imgdoc.convertToPDF()
            imgpdf = fitz.open("pdf", pdfbytes)
            doc.insertPDF(imgpdf)

        doc.save(c+'.pdf')
        doc.close()

entry()
jpg2pdf()
