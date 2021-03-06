
SOCKET_LOGGER = 9000
SOCKET_FILER  = 9001
SOCKET_ORDER  = 9002
SOCKET_BAR    = 9003
SOCKET_KQ_SIMNOW = 9004
SOCKET_KQ_HT  = 9005
SOCKET_GET_TICK  = 9006
SOCKET_ORDER  = 9007

from nature.logger import to_log, read_log_today

from nature.tools import send_email, is_trade_time, is_price_time,is_trade_day
from nature.tools import append_symbol, set_symbol, get_symbols_quote, get_symbols_trade, get_symbols_setting
from nature.tools import get_ts_code, get_dss, get_repo, get_nature_day, get_contract, is_market_date, get_trade_preday
from nature.tools import bsm_call_value, bsm_put_value, bsm_call_imp_vol, bsm_put_imp_vol

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
from nature.filer import rc_file, r_file, a_file, get_file_lock, release_file_lock

from nature.strategy import Tick, VtBarData, ArrayManager, GatewayPingan, BarGenerator, DailyResult
from nature.strategy import (DIRECTION_LONG, DIRECTION_SHORT,
                                     OFFSET_OPEN, OFFSET_CLOSE,
                                     OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY)
from nature.strategy import Signal, Portfolio, TradeData, SignalResult

from nature.engine.stk.nearboll.stk_nearBollStrategy import stk_NearBollPortfolio
from nature.engine.stk.stkEngine import StkEngine

from nature.engine.fut.engine.fut_strategyRsiBoll import Fut_RsiBollPortfolio
#from nature.engine.fut.engine.fut_strategyCci import Fut_CciPortfolio
from nature.engine.fut.engine.fut_strategyAberration import Fut_AberrationPortfolio
from nature.engine.fut.engine.fut_strategyTurtle import Fut_TurtlePortfolio
from nature.engine.fut.engine.fut_strategyCciBoll import Fut_CciBollPortfolio
from nature.engine.fut.engine.fut_strategyDaLi import Fut_DaLiPortfolio
from nature.engine.fut.engine.fut_strategyDaLicta import Fut_DaLictaPortfolio
from nature.engine.fut.engine.fut_strategyOwl import Fut_OwlPortfolio
from nature.engine.fut.engine.fut_strategyAberration_Raw import Fut_Aberration_RawPortfolio
from nature.engine.fut.engine.fut_strategyAberration_Enhance import Fut_Aberration_EnhancePortfolio
from nature.engine.fut.engine.fut_strategyCci_Raw import Fut_Cci_RawPortfolio
from nature.engine.fut.engine.fut_strategyCci_Enhance import Fut_Cci_EnhancePortfolio
from nature.engine.fut.engine.fut_strategyIc import Fut_IcPortfolio
from nature.engine.fut.engine.fut_strategyYue import Fut_YuePortfolio
from nature.engine.fut.engine.fut_strategyAvenger import Fut_AvengerPortfolio
from nature.engine.fut.engine.fut_strategyFollow import Fut_FollowPortfolio
from nature.engine.fut.engine.fut_strategyRatio import Fut_RatioPortfolio
from nature.engine.fut.engine.fut_strategyStraddle import Fut_StraddlePortfolio
from nature.engine.fut.engine.fut_strategySdiffer import Fut_SdifferPortfolio
from nature.engine.fut.engine.fut_strategySkew_Strd import Fut_Skew_StrdPortfolio
from nature.engine.fut.engine.fut_strategySkew_Bili import Fut_Skew_BiliPortfolio
from nature.engine.fut.engine.fut_strategyArbitrage import Fut_ArbitragePortfolio
from nature.engine.fut.engine.fut_strategySpread import Fut_SpreadPortfolio

from nature.engine.fut.rd.fut_strategyAtrRsi import Fut_AtrRsiPortfolio
from nature.engine.fut.rd.opt_short_put import Opt_Short_PutPortfolio
from nature.engine.fut.rd.fut_strategyMa import Fut_MaPortfolio
from nature.engine.fut.rd.fut_strategyDualBand import Fut_DualBandPortfolio
from nature.engine.fut.rd.fut_strategyDonchian import Fut_DonchianPortfolio

from nature.engine.fut.py_ctp.trade import CtpTrade
from nature.engine.fut.py_ctp.quote import CtpQuote
from nature.engine.fut.py_ctp.enums import DirectType, OffsetType

from nature.engine.fut.ctp_ht.subscribe_quote import get_tick
from nature.engine.fut.ctp_ht.gateway_ht_ctp import Gateway_Ht_CTP
from nature.engine.fut.risk.pandian import pandian_run
from nature.engine.fut.risk.book_opt import book_opt_run
from nature.engine.fut.risk.book import extract_trade
from nature.engine.fut.backtest.backtest_result import Backtest_Result
from nature.engine.fut.engine.futEngine import send_order

from nature.web.check_web import del_blank
from nature.web.check_web import check_symbols_p
from nature.web import draw_web
from nature.web.draw_web_plot import ic_show, ip_show, smile_show, opt, dali_show, yue, mates, iv_ts, star, focus 
from nature.web.draw_web_plot import hs300_spread_show, hv_show, skew_show, book_min5_show, book_min5_now_show
from nature.web.draw_web_plot import open_interest_show, iv_straddle_show, straddle_diff_show, iv_show, iv_min5_show
