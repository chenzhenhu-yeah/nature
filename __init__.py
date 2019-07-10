
SOCKET_LOGGER = 9000
SOCKET_FILER  = 9001
SOCKET_ORDER  = 9002
SOCKET_BAR    = 9003

from nature.logger import to_log, read_log_today

from nature.tools import send_email, is_trade_time, is_price_time,is_trade_day

from nature.down_k.get_trading_dates import get_trading_dates
from nature.down_k.get_stk import get_stk_hfq, get_stk_bfq, get_adj_factor
from nature.down_k.get_inx import get_inx
from nature.down_k.get_daily import get_daily, get_stk_codes
from nature.down_k.get_fut import get_fut

from nature.hu_signal.k import K
from nature.hu_signal.hu_talib import MA
from nature.hu_signal.macd import init_signal_macd, signal_macd_sell, signal_macd_buy
from nature.hu_signal.k_pattern import signal_k_pattern

from nature.hold.book import Book, has_factor, stk_report

from nature.auto_trade.place_order import send_instruction
from nature.filer import rc_file, a_file

from nature.engine.vtUtility import VtBarData, ArrayManager, GatewayPingan
from nature.engine.vtUtility import (DIRECTION_LONG, DIRECTION_SHORT,
                                     OFFSET_OPEN, OFFSET_CLOSE,
                                     OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY)
from nature.engine.strategy import Signal, Portfolio

from nature.engine.nearboll.nearBollStrategy import NearBollPortfolio
from nature.engine.nearboll.upBollStrategy import UpBollPortfolio
from nature.engine.cci.cciStrategy import CciPortfolio
from nature.engine.backtestEngine import BacktestingEngine

from nature.engine.tradeEngine import TradeEngine
from nature.engine.cci.cciEngine import CciEngine
from nature.engine.nearboll.bollEngine import BollEngine

from nature.future.py_ctp.trade import CtpTrade
from nature.future.py_ctp.quote import CtpQuote
#from py_ctp.enums import *
from nature.future.py_ctp.structs import Tick
