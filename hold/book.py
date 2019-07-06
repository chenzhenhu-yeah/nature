import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import tushare as ts

from nature import to_log
from nature import get_trading_dates, get_stk_bfq, is_price_time, get_adj_factor


class Tactic(object):
    def __init__(self,dss,name,df):
        to_log('in Tactic.__init__')

        self.dss = dss
        self.tacticName = name
        self.hold_Array = self._load_hold(df)

    def _load_hold(self, df):
        to_log('in Tactic._load_hold')

        r = []
        if df is not None:
            for i, row in df.iterrows():
                #print(row[0],row[1],row[2],row[3])
                r.append([row[1],row[2],row[3]])
        return r

    # 计算Tactic包含的code
    def get_codes(self):
        to_log('in Tactic.get_codes')

        r = []
        for item in self.hold_Array:
            code = item[0]
            r.append(code)
        return r

    # 计算Tactic当前的市值
    def get_cost_cap(self):
        to_log('in Tactic.get_cost_cap')

        r_cost, r_cap = 0, 0

        for item in self.hold_Array:
            code = item[0]
            cost = int(item[1])
            num  = int(item[2])

            df = ts.get_realtime_quotes(code)
            price = float(df.at[0,'price'])
            cap = int(price*num)

            r_cost += cost
            r_cap  += cap

        return r_cost, r_cap

    def daily_result(self, today):
        to_log('in Tactic.daily_result')

        r = []
        for item in self.hold_Array:
            code = item[0]

            df = ts.get_realtime_quotes(code)
            name = df.at[0,'name']
            price = df.at[0,'price']

            #当日停牌
            if price == 0:
                df1 = self.hold_Dict[code]
                df1 = df1[df1.date<=today]
                df1 = df1.sort_values('date',ascending=False)
                assert(df1.empty == False)
                price = df1.iat[0,3]

            cap = int(float(price)*item[2])
            r.append( [today,self.tacticName,code,name,price,cap] )
        return r

class Book(object):
    def __init__(self,dss,filename='csv/hold.csv'):
        to_log('in Book.__init__')

        self.dss = dss
        self.holdFilename = dss + filename
        self.cash = self._loadCash()
        self.tactic_List = self._loadHold()

    def _loadCash(self):
        to_log('in Book._loadCash')

        df1 = pd.read_csv(self.holdFilename);#print(df1)
        df1 = df1[df1.agent=='pingan']
        df2 = df1[df1.portfolio=='cash']
        cash = df2.iat[0,2]
        return cash

    def _loadHold(self):
        to_log('in Book._loadHold')

        df1 = pd.read_csv(self.holdFilename);#print(df1)
        df1 = df1[df1.agent=='pingan']
        df1 = df1[~df1.code.isin(['cash01','profit'])]
        #print(df1)
        pfSet = set(df1.portfolio)
        r = []
        for pfName in pfSet:
            df2 = df1[df1.portfolio==pfName]
            tactic = Tactic(self.dss, pfName, df2)
            r.append(tactic)

        return  r

    # 计算Book包含的code
    def get_codes(self):
        to_log('in Book.get_codes')

        codes = []
        for tactic in self.tactic_List:
            codes += tactic.get_codes()
        return set(codes)

    # 计算Book当前的市值
    def get_cost_cap(self):
        to_log('in Book.get_cost_cap')

        cost, cap = 0, 0
        for tactic in self.tactic_List:
            cost1, cap1 = tactic.get_cost_cap()
            cost += cost1
            cap  += cap1
        return cost, cap

    # 处理指令
    def deal_ins(self, ins_dict2):
        ins_dict = ins_dict2.copy()
        if ins_dict['ins'] == 'bonus_interest':
            pass
        elif ins_dict['ins'] == 'buy_order':
            self._record_buy_order(ins_dict)
        elif ins_dict['ins'] == 'sell_order':
            self._record_sell_order(ins_dict)
        elif ins_dict['ins'] == 'open_portfolio':
            self._open_tactic_in_file(ins_dict)
        elif ins_dict['ins'] == 'close_portfolio':
            self._close_tactic_in_file(ins_dict)

    # 20190522&{'ins': 'buy_order', 'portfolio': 'redian', 'code': '002482', 'num': 3700, 'price': 5.4, 'cost': 19980, 'agent': 'pingan', 'name': '广田集团'}
    def _record_buy_order(self, ins_dict):
        to_log('in Book._record_buy_order')

        df = pd.read_csv(self.holdFilename, dtype={'code':'str'})

        portfolio_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code=='profit') & (df.agent==ins_dict['agent'])].index.tolist()
        if len(portfolio_index) == 1:   #已开组合
            #获得原来的cash, 并减少cash
            pre_cash_index = df[(df.portfolio=='cash') & (df.agent==ins_dict['agent'])].index.tolist()
            df.loc[pre_cash_index[0],'cost'] -= ins_dict['cost']

            #文件中不需要保存这两个字段
            if 'ins' in ins_dict:
                ins_dict.pop('ins')
            if 'price' in ins_dict:
                ins_dict.pop('price')

            #新增一条记录
            df_dict = pd.DataFrame([ins_dict])
            df = df.append(df_dict, sort=False)
            #print(df)

            df = df[['portfolio','code','cost','num','agent']]
            df.to_csv(self.holdFilename,index=False)
        else:
            to_log('组合未开，buy_order无法处理')

    # 20190522&{'ins': 'sell_order', 'portfolio': 'redian', 'code': '300199', 'num': 1800, 'price': 11.46, 'cost': 20628, 'agent': 'pingan', 'name': '翰宇药业'}
    def _record_sell_order(self, ins_dict):
        to_log('in record_sell_order')

        df = pd.read_csv(self.holdFilename, dtype={'code':'str'})

        #获得原来的stock
        pre_stock_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code==ins_dict['code']) & (df.num==ins_dict['num']) & (df.agent==ins_dict['agent'])].index.tolist()
        if len(pre_stock_index) > 0:
            pre_row = df.loc[pre_stock_index[0]]

            #更新组合的profit
            profit = ins_dict['cost']*(1-0.0015) - pre_row['cost']
            profit_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code=='profit') & (df.agent==ins_dict['agent'])].index.tolist()
            df.loc[profit_index[0],'cost'] += profit

            #获得原来的cash, 并增加cash
            pre_cash_index = df[(df.portfolio=='cash') & (df.agent==ins_dict['agent'])].index.tolist()
            df.loc[pre_cash_index[0],'cost'] += ins_dict['cost']*(1-0.0015)

            #删除原来的记录
            df = df.drop(index=[pre_stock_index[0]])

            #print(df)
            df = df[['portfolio','code','cost','num','agent']]
            df.to_csv(self.holdFilename,index=False)
        else:
            to_log('原持仓记录不存在，sell_order无法处理')

    #{'ins':'open_portfolio', 'portfolio':'5G','code':'profit','cost':0,'num':0,'agent':'pingan'}
    def _open_tactic_in_file(self, ins_dict):
        df = pd.read_csv(self.holdFilename, dtype={'code':str})

        #验证此组合不存在
        df1 = df[df.agent == ins_dict['agent']]
        df1 = df1[df1.portfolio == ins_dict['portfolio']]
        if df1.empty:
            #组合开仓，增加一条profit记录
            ins_dict.pop('ins')
            df_dict = pd.DataFrame([ins_dict])
            df = df.append(df_dict,sort=False)
            df = df.loc[:,['portfolio','code','cost','num','agent']]
            df.to_csv(file_hold_security, index=False)
        else:
            to_log('组合已存在！！！')

    #{'ins':'close_portfolio', 'portfolio':'5G','agent':'pingan'}
    def _close_tactic_in_file(self, ins_dict):
        df = pd.read_csv(self.holdFilename, dtype={'code':str})
        #获得组合相关的记录
        portfolio_index = df[(df.portfolio==ins_dict['portfolio']) & (df.agent==ins_dict['agent'])].index.tolist()
        if len(portfolio_index) == 1:
            profit_row = df.loc[portfolio_index[0]]
            if profit_row['code'] == 'profit':
                #获得cash, 并增加cash金额
                cash_index = df[(df.portfolio=='cash') & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
                df.loc[cash_index[0], 'cost'] += profit_row['cost']

                #删除此记录
                df = df.drop(index=portfolio_index)
                df = df.loc[:,['portfolio','code','cost','num','agent']]
                df.to_csv(self.holdFilename, index=False)
                to_log('close_portfolio success '+ str(ins_dict) )
            else:
                to_log('close_portfolio failed, 组合中无profit字段')
        else:
            to_log('close_portfolio failed, 组合中还有标的')

    def _validate_order(self, ins_dict):
        r = False
        df = pd.read_csv(self.holdFilename,dtype={'code':str})

        if ins_dict['ins'][:3] == 'buy':        #对于buy_order
            portfolio_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
            if len(portfolio_index)>0:   #是否已开组合
                # cash_index = df[(df.portfolio=='cash') & (df.code=='cash01') & (df.agent==ins_dict['agent'])].index.tolist()
                # if len(cash_index)>0:  #券商现金是否足够
                #     row = df.loc[cash_index[0]]
                #     if row.at['cost'] > ins_dict['cost']*1.001:
                r = True
        elif ins_dict['ins'][:4] == 'sell':     #对于sell_order，是否已存在？
            #获得原来的stock
            stock_index = df[(df.portfolio==ins_dict['portfolio']) & (df.code==ins_dict['code']) & (df.agent==ins_dict['agent'])].index.tolist()
            if len(stock_index)>0:
                row = df.loc[stock_index[0]]
                if row.at['num'] >= ins_dict['num']:
                    r = True
        r = True    #暂不校验!
        return r


    # 盘点组合
    def _pandian_p(self,idx,df):
        df1 = df.copy()
        df1 = df1.rename(columns = {'code':'代码','portfolio':'名称','num':'数量','cost':'成本','agent':'市值'})
        df1['市值'] = 0
        df1['名称'] = ''

        # 补充名称、市值
        cost, value = 0, 0
        for i,row in df1.iterrows():
            if row['代码'] in ('cash01'):
                df1.at[i,'市值'] = row['成本']
            elif row['代码'] in ('profit'):
                pass
            else:
                df_q = ts.get_realtime_quotes(row['代码'])
                df1.at[i,'名称'] = df_q.at[0,'name']
                price = float(df_q.at[0,'price'])
                df1.at[i,'市值'] = row['数量'] * price
                cost  += df1.at[i,'成本']
                value += df1.at[i,'市值']

        # 汇总
        df2 = pd.DataFrame([['     ',idx,0,cost,value]],columns=['代码','名称','数量','成本','市值'])
        df1 = df2.append(df1, sort=False)
        # 排序
        df1 = df1.sort_values(by='代码', ascending=False)
        #print(df1)
        return df1


    # 盘点持仓
    def pandian(self, agent=None):
        df_r = pd.DataFrame([['','现金','','',0],['','货基','','',0],['','持仓','',0,0],['','总计','','',0]],
        columns=['代码','名称','数量','成本','市值'])

        agent = 'pingan'
        #agent = 'gtja'
        df1 = pd.read_csv(self.holdFilename);#print(df1)
        df2 = df1[(df1.agent==agent)]
        #df2 = df1[(df1.agent==agent) & (~df1.portfolio.isin(['cash','money_fond']))]

        g = df2.groupby('portfolio').agg({'cost':np.sum})
        for idx in list(g.index):
            #print(g.loc[idx]['cost'])
            if idx == 'cash':
                df_r.iat[0,4] = g.loc[idx]['cost']
            elif idx == 'money_fond':
                df_r.iat[1,4] = g.loc[idx]['cost']
            else:
                df9 = pd.DataFrame([['','','','',''],],columns=['代码','名称','数量','成本','市值'])
                df_r = df_r.append(df9,sort=False)
                df9 = df2[df2.portfolio==idx]
                df9 = self._pandian_p(idx,df9)
                df_r = df_r.append(df9,sort=False)
                df_r.iat[2,4] += df9.iat[-1,4]
                df_r.iat[2,3] += df9.iat[-1,3];#print(df9)
        df_r.iat[3,4] = df_r.iat[0,4] + df_r.iat[1,4] + df_r.iat[2,4]

        #print(df_r)
        df_r.to_excel('e1.xlsx',index=False)

###########################################################################
def has_factor(dss):
    to_log('in has_factor')

    r = []
    b1 = Book(dss)
    codes = b1.get_codes()

    for code in codes:
        df = get_adj_factor(dss, code)
        #print(df.head(2))
        if df.at[0,'adj_factor'] != df.at[1,'adj_factor']:
            r.append(code)
    return r

def stk_report(dss):
    to_log('in stk_report')

    b1 = Book(dss)
    codes = b1.get_codes()

    r = []
    for code in codes:
        df = ts.get_realtime_quotes(code)
        name = df.at[0,'name']
        price = float(df.at[0,'price'])
        pre_close = float(df.at[0,'pre_close'])

        df = get_stk_bfq(dss,code)
        df = df.loc[:30,]
        #print(df)
        one_change = round((price/pre_close - 1)*100, 2)
        five_change = round((df.at[0,'close']/df.at[5,'close'] - 1)*100, 2)
        ten_change = round((df.at[0,'close']/df.at[10,'close'] - 1)*100, 2)
        r.append( str([name,one_change,five_change,ten_change]) )

    return r

if __name__ == '__main__':
    dss = '../../data/'
    #stk_warn(dss)
    #print(stk_report(dss))
    print(has_factor(dss))

    # book = Book(dss)
    # print(book.get_cost_cap())


    pass
    #daily_report(dss)
    #tactic_signal(dss)
