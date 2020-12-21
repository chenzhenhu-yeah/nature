
import warnings
warnings.filterwarnings("ignore")

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
import zipfile

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date
from nature import rc_file, get_symbols_quote, get_tick, send_order

#
# fn_setting_fut = get_dss() + 'fut/cfg/setting_pz.csv'
# df = pd.read_csv(fn_setting_fut)
# df['sp'] = ''
# # print(df)
# df.to_csv(fn_setting_fut, index=False)

# code = 'm2105&m2109'
# get_contract(code)
