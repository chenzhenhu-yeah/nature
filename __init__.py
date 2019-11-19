
SOCKET_LOGGER = 9000
SOCKET_FILER  = 9001
SOCKET_ORDER  = 9002
SOCKET_BAR    = 9003
SOCKET_KQ_SIMNOW = 9004
SOCKET_KQ_HT  = 9005

from nature.logger import to_log, read_log_today

from nature.tools import send_email, is_trade_time, is_price_time,is_trade_day
from nature.tools import get_ts_code, get_dss, get_nature_day, get_contract

from nature.down_k.get_trading_dates import get_trading_dates
from nature.down_k.get_stk import get_stk_hfq, get_stk_bfq, get_adj_factor, get_hfq_factor
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

from nature.strategy import VtBarData, ArrayManager, GatewayPingan, BarGenerator
from nature.strategy import (DIRECTION_LONG, DIRECTION_SHORT,
                                     OFFSET_OPEN, OFFSET_CLOSE,
                                     OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY)
from nature.strategy import Signal, Portfolio, TradeData, SignalResult

from nature.engine.stk.nearboll.stk_nearBollStrategy import stk_NearBollPortfolio
from nature.engine.stk.stkEngine import StkEngine

from nature.engine.fut.engine.fut_strategyAtrRsi import Fut_AtrRsiPortfolio
from nature.engine.fut.engine.fut_strategyRsiBoll import Fut_RsiBollPortfolio
from nature.engine.fut.engine.fut_strategyCci import Fut_CciPortfolio
from nature.engine.fut.engine.fut_strategyAberration import Fut_AberrationPortfolio
from nature.engine.fut.engine.fut_strategyTurtle import Fut_TurtlePortfolio
from nature.engine.fut.engine.fut_strategyDonchian import Fut_DonchianPortfolio
from nature.engine.fut.engine.fut_strategyCciBoll import Fut_CciBollPortfolio

from nature.engine.fut.py_ctp.trade import CtpTrade
from nature.engine.fut.py_ctp.quote import CtpQuote
from nature.engine.fut.py_ctp.structs import Tick
from nature.engine.fut.py_ctp.enums import DirectType, OffsetType

from nature.engine.fut.ctp_ht.gateway_ht_ctp import Gateway_Ht_CTP
