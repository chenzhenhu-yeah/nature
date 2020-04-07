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

duo  = [2783.0, 2820.0, 2851.0, 2888.0, 2919.0, 2956.0, 2987.0, 3024.0, 3055.0, 3092.0]
kong = [2773.0, 2732.0, 2689.0, 2648.0, 2595.0, 2574.0]

# # duo  = [2736.0, 2811.0, 2887.0]
# # kong = [2675.0, 2651.0, 2627.0, 2603.0, 2579.0, 2555.0, 2516.0]

# [2776.0, 2793.0, 2810.0, 2827.0, 2844.0, 2861.0, 2878.0, 2895.0, 2912.0, 2929.0, 2946.0, 2963.0, 2980.0, 2997.0, 3014.0, 3031.0, 3048.0, 3065.0, 3082.0, 3099.0]
# [2784.0, 2763.0, 2742.0, 2721.0, 2700.0, 2679.0, 2658.0, 2637.0, 2616.0, 2595.0, 2574.0, 2553.0]

# [2793.0, 2810.0, 2861.0, 2878.0, 2929.0, 2946.0, 2997.0, 3014.0, 3065.0, 3082.0]
# [2763.0, 2742.0, 2679.0, 2658.0, 2595.0, 2574.0]

# [2783.0, 2820.0, 2851.0, 2888.0, 2919.0, 2956.0, 2987.0, 3024.0, 3055.0, 3092.0]
# [2773.0, 2732.0, 2689.0, 2648.0, 2595.0, 2574.0]

print(sum(duo))
print(sum(kong))
