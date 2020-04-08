import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time

import json
import tushare as ts

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date

import sys

duo  = [2805.0, 2848.0, 2891.0, 2934.0, 2977.0, 3020.0, 3063.0, 3106.0, 3135.0]
kong = [2787.0, 2751.0, 2715.0, 2679.0, 2643.0, 2608.0]


# [2805.0, 2848.0, 2891.0, 2934.0, 2977.0, 3020.0, 3063.0, 3106.0, 3135.0]
# [2787.0, 2751.0, 2715.0, 2679.0, 2643.0, 2608.0]

# [2805.0, 2828.0, 2851.0, 2874.0, 2897.0, 2920.0, 2943.0, 2966.0, 2975.0]
# [2797.0, 2771.0, 2755.0, 2739.0, 2723.0, 2708.0]

print(sum(duo))
print(sum(kong))
