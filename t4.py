
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

s = pd.Series([1, 2])

print(s)
deep = s.copy()
s[0] = 10
print(s)
print(deep)
