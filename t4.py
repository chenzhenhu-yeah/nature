import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time
import sys
import json
import tushare as ts
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import ctypes
import os
import platform
import sys

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date
from nature import rc_file, get_symbols_quote, get_tick, send_order
#
# symbol = 'm2101-C-3200'
# symbol = 'p2105'
#
# r = get_tick(symbol)
#
# print(r)
#
# ins = {'vtSymbol':'p2105',
#        'direction':'Buy',
#        'offset':'Open',
#        'price':6336,
#        'volume':1,
#        'pfName':'web'
#        }
# send_order(ins)

# send_email(get_dss(), 'test3', '')

# df = pd.DataFrame(np.random.randn(3, 3), columns=list('ABC'))
#
# # fig, ax = plt.subplots()
# # hide axes
# # fig.patch.set_visible(False)
# # ax.axis('off')
# # ax.axis('tight')
# # ax.table(cellText=df.values, colLabels=df.columns, loc='center')
# # fig.tight_layout()
#
#
# plt.subplot(2,1,1)
# plt.axis('off')
# plt.table(cellText=df.values, colLabels=df.columns, loc='center')
#
# plt.subplot(2,1,2)
# plt.axis('off')
# plt.table(cellText=df.values, colLabels=df.columns, loc='center')
#
# row_colors = ['red','gold','green']
#
# plt.figure(figsize=(15,3))
# plt.title('test')
# plt.axis('off')
# my_table = plt.table(cellText=df.values, colLabels=df.columns, colWidths=[0.3]*3,
#                      colColours=row_colors, cellColours=[row_colors,row_colors,row_colors],
#                      cellLoc='left', loc='center',
#                      bbox=[0, 0, 1, 1])
#
# my_table.set_fontsize(18)
# my_table.scale(1, 5)
# plt.show()
