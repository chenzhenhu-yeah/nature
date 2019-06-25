
import pandas as pd
import tushare as ts

pro = ts.pro_api('e7d81e40fb30b0e7f7f8d420a81700f401ddd382d82b84c473afd854')

# df = pro.fut_basic(exchange='CFFEX')
# df = df[df.ts_code.str.contains('IC')]

df = pro.fut_basic(exchange='CFFEX')
# df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')

#df = pro.fut_daily(ts_code='IC1905.CFX', start_date='20190501', end_date='20190513')
#
# df = pro.fut_daily(ts_code='IC.CFX', start_date='20190501',)
#df = pro.fut_daily(ts_code='ICL.CFX', start_date='20190501')
#df = pro.fut_daily(ts_code='ICL1.CFX', start_date='20190501')
# df = pro.fut_daily(ts_code='ICL2.CFX', start_date='20190501', end_date='20190513')
# df = pro.fut_daily(ts_code='ICL3.CFX', start_date='20190501', end_date='20190513')

#
# df = pro.fut_basic(exchange='CFFEX', fut_type='1', fields='ts_code,last_ddate')
# df = df[df.ts_code.str.contains('IC18')].sort_values('last_ddate')

print(df)
df.to_excel('e1.xls')
