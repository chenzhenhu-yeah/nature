
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

# t = get_tick('m2105')
# t = get_tick('al2101')
# # t = get_tick('FG105')
# # t = get_tick('CF105')
# # t = get_tick('IF2101')
# # t = get_tick('IO2101-C-5000')
# print(t)

# c = get_contract('m2105')
# c = get_contract('al2105')
c = get_contract('CF105C16000')
print(c.mature)
