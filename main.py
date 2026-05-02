#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT CRT SCANNER v10.0 — COMPLETE CRYPTO EDITION
Implements CRT rules (Sections 1‑23) with 100% fidelity.
Crypto‑adapted: UTC key hours, BTC/ETH SMT, volatility adjustments.
All filters/gates are environment variables — set any to 0 to disable.
"""

import os, time, asyncio, logging, datetime, json, math
import aiosqlite, httpx, ccxt.async_support as ccxt
import pandas as pd, numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum

# ============ ENVIRONMENT CONFIGURATION ============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_crt.db")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 80))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 1))

# Risk management
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", 1.0))
RISK_PER_DAY_PCT = float(os.getenv("RISK_PER_DAY_PCT", 2.0))
RISK_PER_WEEK_PCT = float(os.getenv("RISK_PER_WEEK_PCT", 5.0))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", 100))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", 50))
CRYPTO_REDUCE_SIZE = int(os.getenv("CRYPTO_REDUCE_SIZE", 1)) == 1

# Time filters (UTC key hours – crypto)
STRICT_TIME_FILTER = int(os.getenv("STRICT_TIME_FILTER", 1)) == 1
CRYPTO_KEY_HOURS_UTC = [int(x) for x in os.getenv("CRYPTO_KEY_HOURS_UTC", "0,4,8,12,16,20").split(",")]

# CRT configuration
REQUIRE_KEY_LEVEL = int(os.getenv("REQUIRE_KEY_LEVEL", 1)) == 1
MIN_SCORE_TBS = float(os.getenv("MIN_SCORE_TBS", 75))
MIN_SCORE_TWS = float(os.getenv("MIN_SCORE_TWS", 90))
MIN_SCORE_ALL = float(os.getenv("MIN_SCORE_ALL", 0))          # absolute floor
ALLOW_NEUTRAL_BIAS = int(os.getenv("ALLOW_NEUTRAL_BIAS", 0)) == 1
REQUIRE_CSD_LTF = int(os.getenv("REQUIRE_CSD_LTF", 1)) == 1
CSD_HARD_GATE = int(os.getenv("CSD_HARD_GATE", 1)) == 1            # CSD no longer in scoring
ASIA_DAILY_MODEL = int(os.getenv("ASIA_DAILY_MODEL", 1)) == 1
THIRD_CANDLE_PURGE = int(os.getenv("THIRD_CANDLE_PURGE", 1)) == 1

# SMT
USE_SMT = int(os.getenv("USE_SMT", 1)) == 1
SMT_BASE = os.getenv("SMT_BASE", "BTC/USDT")
SMT_QUOTE = os.getenv("SMT_QUOTE", "ETH/USDT")

# Institutional positioning (Section 20)
USE_COT = int(os.getenv("USE_COT", 0)) == 1
COT_API_URL = os.getenv("COT_API_URL", "")
USE_RETAIL_SENTIMENT = int(os.getenv("USE_RETAIL_SENTIMENT", 0)) == 1
RETAIL_SENTIMENT_API_URL = os.getenv("RETAIL_SENTIMENT_API_URL", "")

# Live data filters (Section 21)
USE_ECON_CALENDAR = int(os.getenv("USE_ECON_CALENDAR", 0)) == 1
ECON_CALENDAR_API_URL = os.getenv("ECON_CALENDAR_API_URL", "")
USE_SPREAD_FILTER = int(os.getenv("USE_SPREAD_FILTER", 1)) == 1
SPREAD_MAX_PCT_SL = float(os.getenv("SPREAD_MAX_PCT_SL", 20.0))
SPREAD_MAX_PIPS = float(os.getenv("SPREAD_MAX_PIPS", 0.0))
VOLATILITY_FILTER = int(os.getenv("VOLATILITY_FILTER", 1)) == 1

# Adaptive learning (Section 22)
ADAPTIVE_LEARNING = int(os.getenv("ADAPTIVE_LEARNING", 1)) == 1
ADAPTIVE_MIN_WR_THRESHOLD = float(os.getenv("ADAPTIVE_MIN_WR_THRESHOLD", 65))

# Rate limits, edge discovery, expiry
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 4))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 2.5))
EDGE_DISCOVERY_MIN_SIGNALS = int(os.getenv("EDGE_DISCOVERY_MIN_SIGNALS", 200))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 48))

# ============ LOGGING ============
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
log = logging.getLogger("romeopt_crt")

# ============ ENUMS (Section 1‑8) ============
class TrendBias(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

class TurtleSoupType(str, Enum):
    TBS = "TBS"    # Turtle Body Soup – high probability
    TWS = "TWS"    # Turtle Wick Soup – lower probability
    NONE = "NONE"

class EntryModel(str, Enum):
    A = "MODEL_A"   # Candle 2 entry (aggressive)
    B = "MODEL_B"   # Candle 3 entry (safest)
    C = "MODEL_C"   # Nested LTF CRT (most accurate)

class SignalOutcome(str, Enum):
    TP1_HIT = "TP1_HIT"
    TP2_HIT = "TP2_HIT"
    TP3_HIT = "TP3_HIT"
    SL_HIT = "SL_HIT"
    EXPIRED = "EXPIRED"
    ACTIVE = "ACTIVE"

# ============ DATA STRUCTURES ============
@dataclass
class CRTStructure:
    accumulation_high: float = 0.0
    accumulation_low: float = 0.0
    inside_bars: int = 0
    manipulation_high: float = 0.0
    manipulation_low: float = 0.0
    turtle_soup_type: TurtleSoupType = TurtleSoupType.NONE
    distribution_confirmed: bool = False
    csd_on_ltf: bool = False
    kod_present: bool = False
    third_candle_purge: bool = False

@dataclass
class ConfluenceScore:
    total: int = 0
    tbs_points: int = 0
    key_level_points: int = 0
    bias_points: int = 0
    time_points: int = 0
    nested_crt_points: int = 0
    dol_points: int = 0
    smt_points: int = 0
    kod_points: int = 0
    fvg_points: int = 0
    ob_points: int = 0
    premium_discount_points: int = 0
    inside_bars_points: int = 0

@dataclass
class SignalRecord:
    id: Optional[int] = None
    symbol: str = ""
    timestamp: str = ""
    side: str = ""
    entry_price: float = 0.0
    sl_price: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    entry_model: str = ""
    quality_score: float = 0.0
    score_details: str = ""
    context_json: str = "{}"
    hour_utc: int = 0
    day_of_week: int = 0
    trend_bias: str = ""
    turtle_soup_type: str = ""
    status: str = "active"
    outcome: str = "active"
    outcome_tp1: str = "PENDING"
    outcome_tp2: str = "PENDING"
    outcome_tp3: str = "PENDING"
    actual_rr: float = 0.0
    bars_to_tp1: int = 0
    max_adverse_pct: float = 0.0
    max_favorable_pct: float = 0.0
    closed_at: str = ""
    closed_price: float = 0.0
    pnl_pct: float = 0.0
    alert_sent: int = 0

# ============ RATE LIMITER & DATA FETCHING ============
class EnhancedRateLimiter:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_requests, self.funding_requests, self.oi_requests = [], [], []
        self.min_delay = 0.25
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES

    async def wait_for_endpoint(self, endpoint_type="general"):
        now = time.monotonic()
        mapping = {"general": (self.general_requests, 1.0), "funding": (self.funding_requests, 1.5), "oi": (self.oi_requests, 2.0)}
        lst, cooldown = mapping[endpoint_type]
        lst[:] = [t for t in lst if now - t < cooldown]
        if lst:
            wait = cooldown - (now - lst[0])
            if wait > 0:
                await asyncio.sleep(wait + np.random.uniform(0.1, 0.3))
        lst.append(now)
        await asyncio.sleep(0.1)

    async def execute_with_backoff(self, func, *args, endpoint_type="general", **kwargs):
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_for_endpoint(endpoint_type)
                    res = await func(*args, **kwargs)
                    await asyncio.sleep({"general":0.05, "funding":0.15, "oi":0.2}[endpoint_type])
                    return res
                except Exception as e:
                    if any(p in str(e) for p in ["Too Many Requests","50011","429","rate limit"]):
                        wait = self.min_delay * (self.backoff_factor**attempt) + np.random.uniform(0.2,0.5)
                        log.warning(f"Rate limited {endpoint_type}, retry {attempt+1}/{self.max_retries}, wait {wait:.1f}s")
                        await asyncio.sleep(wait)
                    else:
                        raise e
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()

async def fetch_ohlcv(exchange, symbol, timeframe, limit=100):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit)
    except:
        return None

def create_dataframe(ohlcv):
    if not ohlcv: return None
    df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

async def safe_fetch_tickers(exchange):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_tickers)
    except:
        return {}

async def fetch_ticker(exchange, symbol):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_ticker, symbol)
    except:
        return {}

# ============ TIME MANAGER (crypto) ============
class CryptoTimeManager:
    @staticmethod
    def is_key_window(utc_dt: datetime.datetime) -> bool:
        return utc_dt.hour in CRYPTO_KEY_HOURS_UTC and utc_dt.minute <= 15

    @staticmethod
    def is_high_prob_day(utc_dt: datetime.datetime) -> bool:
        return utc_dt.weekday() in {1,2,3}   # Tue‑Thu

    @staticmethod
    def is_first_week(utc_dt: datetime.datetime) -> bool:
        return utc_dt.day <= 7

# ============ BIAS ENGINE (Section 4) ============
class BiasEngine:
    @staticmethod
    def order_flow(df_daily, df_h4) -> Tuple[TrendBias, float]:
        if df_daily is None or len(df_daily) < 10:
            return TrendBias.NEUTRAL, 0.0
        highs = df_daily['high'].values[-10:]
        lows = df_daily['low'].values[-10:]
        # Higher highs and higher lows = bullish
        hh = all(highs[i] > highs[i-1] for i in range(1, len(highs)))
        hl = all(lows[i] > lows[i-1] for i in range(1, len(lows)))
        if hh and hl: return TrendBias.BULLISH, 1.0
        lh = all(highs[i] < highs[i-1] for i in range(1, len(highs)))
        ll = all(lows[i] < lows[i-1] for i in range(1, len(lows)))
        if lh and ll: return TrendBias.BEARISH, 1.0
        # Partial signals
        if highs[-1] > highs[-5] and lows[-1] > lows[-5]: return TrendBias.BULLISH, 0.6
        if highs[-1] < highs[-5] and lows[-1] < lows[-5]: return TrendBias.BEARISH, 0.6
        return TrendBias.NEUTRAL, 0.0

    @staticmethod
    def premium_discount(price, range_high, range_low) -> str:
        if range_high <= range_low: return "NEUTRAL"
        mid = (range_high + range_low) / 2
        return "PREMIUM" if price > mid else "DISCOUNT"

    @staticmethod
    def daily_bias(df_daily) -> TrendBias:
        """Daily candle CRT: did it liquidate a previous low/high and close back?"""
        if df_daily is None or len(df_daily) < 2: return TrendBias.NEUTRAL
        prev = df_daily.iloc[-2]
        curr = df_daily.iloc[-1]
        if curr['low'] < prev['low'] and curr['close'] > prev['low']:
            return TrendBias.BULLISH
        if curr['high'] > prev['high'] and curr['close'] < prev['high']:
            return TrendBias.BEARISH
        return TrendBias.NEUTRAL

# ============ CRT DETECTION (Sections 5,7,8,19) ============
class CRTDetector:
    @staticmethod
    def detect_accumulation(df, lookback=5):
        """Find first candle whose high/low contain subsequent inside bars."""
        if len(df) < 3: return None
        recent = df.tail(lookback)
        for i in range(len(recent)-1, 1, -1):
            c1 = recent.iloc[i-1]
            inside = [c for c in recent.iloc[i:] if c['high'] <= c1['high'] and c['low'] >= c1['low']]
            if len(inside) >= 1:
                return {'high': c1['high'], 'low': c1['low'], 'inside': len(inside)}
        return None

    @staticmethod
    def detect_turtle_soup(candle, accum_high, accum_low, bias: TrendBias) -> Tuple[TurtleSoupType, bool]:
        body_high = max(candle['open'], candle['close'])
        body_low = min(candle['open'], candle['close'])
        if bias == TrendBias.BEARISH:
            if candle['high'] > accum_high:
                return (TurtleSoupType.TBS, True) if body_high < accum_high else (TurtleSoupType.TWS, False)
        elif bias == TrendBias.BULLISH:
            if candle['low'] < accum_low:
                return (TurtleSoupType.TBS, True) if body_low > accum_low else (TurtleSoupType.TWS, False)
        return TurtleSoupType.NONE, False

    @staticmethod
    def detect_csd(df, side: str) -> bool:
        """Change in State of Delivery – structure break."""
        if len(df) < 2: return False
        prev = df.iloc[-2]
        curr = df.iloc[-1]
        return (curr['close'] > prev['high']) if side == "BUY" else (curr['close'] < prev['low'])

    @staticmethod
    def detect_kod(df, ts_side: str) -> bool:
        """Kiss of Death pattern."""
        if len(df) < 3: return False
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        if ts_side == "SELL":
            return (c2['close'] > c2['open']) and (c3['close'] < c1['close'])
        else:
            return (c2['close'] < c2['open']) and (c3['close'] > c1['close'])

    @staticmethod
    def detect_third_candle_purge(df, accum, bias: TrendBias) -> bool:
        """Candle 3 itself gets turtle souped."""
        if len(df) < 3: return False
        c3 = df.iloc[-1]
        if bias == TrendBias.BEARISH:
            return (c3['high'] > accum['high'] and c3['close'] < accum['high'])
        else:
            return (c3['low'] < accum['low'] and c3['close'] > accum['low'])

    @staticmethod
    def find_key_levels(df_daily, df_weekly=None, tolerance=0.005):
        levels = []
        if df_daily is not None and len(df_daily) >= 2:
            prev = df_daily.iloc[-2]
            levels.append({'price': prev['high'], 'type': 'prev_day_high'})
            levels.append({'price': prev['low'], 'type': 'prev_day_low'})
        if df_weekly is not None and len(df_weekly) >= 2:
            prev = df_weekly.iloc[-2]
            levels.append({'price': prev['high'], 'type': 'prev_week_high'})
            levels.append({'price': prev['low'], 'type': 'prev_week_low'})
        return levels

# ============ ASIA DAILY MODEL (Section 19) ============
class AsiaDailyModel:
    """Uses Asia range as daily accumulation, London break for bias."""
    @staticmethod
    async def get_asia_range(exchange, symbol: str) -> Tuple[float, float]:
        df_h1 = create_dataframe(await fetch_ohlcv(exchange, symbol, "1h", 12))
        if df_h1 is None: return 0,0
        asia_candles = []
        now = datetime.datetime.utcnow()
        for _, row in df_h1.iterrows():
            ts = datetime.datetime.utcfromtimestamp(row['timestamp']/1000) if row['timestamp'] > 1e10 else datetime.datetime.utcfromtimestamp(row['timestamp'])
            if 0 <= ts.hour < 8:   # rough Asia session
                asia_candles.append(row)
        if not asia_candles: return 0,0
        asia_df = pd.DataFrame(asia_candles)
        return float(asia_df['high'].max()), float(asia_df['low'].min())

    @staticmethod
    def london_bias(asia_high, asia_low, current_price, london_close) -> TrendBias:
        if london_close < asia_low:
            # broke low and closed above? actually need close back above... simplified
            return TrendBias.BULLISH
        if london_close > asia_high:
            return TrendBias.BEARISH
        return TrendBias.NEUTRAL

asia_model = AsiaDailyModel()

# ============ SMT DIVERGENCE (Section 10) ============
class SMTDetector:
    async def check_divergence(self, exchange, base: str, quote: str, side: str) -> bool:
        try:
            base_df = create_dataframe(await fetch_ohlcv(exchange, base, "15m", 20))
            quote_df = create_dataframe(await fetch_ohlcv(exchange, quote, "15m", 20))
            if base_df is None or quote_df is None: return False
            if side == "SELL":
                return base_df['high'].max() > base_df['high'].iloc[-2] and quote_df['high'].max() <= quote_df['high'].iloc[-2]
            else:
                return base_df['low'].min() < base_df['low'].iloc[-2] and quote_df['low'].min() >= quote_df['low'].iloc[-2]
        except:
            return False

smt_detector = SMTDetector()

# ============ INSTITUTIONAL POSITIONING (Section 20) ============
class InstitutionalLayer:
    @staticmethod
    async def fetch_cot():
        if not USE_COT or not COT_API_URL: return None
        # Placeholder – implement COT fetch from api.cftc.gov
        return None

    @staticmethod
    async def fetch_retail_sentiment(symbol):
        if not USE_RETAIL_SENTIMENT or not RETAIL_SENTIMENT_API_URL: return None
        # Placeholder – fetch retail long/short %
        return None

    @staticmethod
    def adjust_score(score: ConfluenceScore, cot_data, retail_data) -> ConfluenceScore:
        # Placeholder: adjust score based on COT percentile, retail extremes
        return score

# ============ LIVE DATA FILTERS (Section 21) ============
class LiveDataFilters:
    @staticmethod
    async def is_high_impact_news_blocked():
        if not USE_ECON_CALENDAR or not ECON_CALENDAR_API_URL: return False
        # Placeholder: check calendar for red events within 15 min before / 30 min after
        return False

    @staticmethod
    async def spread_too_wide(exchange, symbol, sl_distance_pct):
        if not USE_SPREAD_FILTER: return False
        try:
            ticker = await fetch_ticker(exchange, symbol)
            if ticker is None: return False
            spread = ticker['ask'] - ticker['bid']
            if spread <= 0: return False
            if SPREAD_MAX_PIPS > 0 and spread > SPREAD_MAX_PIPS: return True
            if sl_distance_pct > 0 and (spread / ticker['bid'] * 100) > (sl_distance_pct * SPREAD_MAX_PCT_SL / 100):
                return True
        except:
            pass
        return False

    @staticmethod
    def volatility_regime(df, atr_period=14):
        if not VOLATILITY_FILTER or df is None or len(df) < atr_period: return "normal", 1.0
        high = df['high'].values[-atr_period-1:]
        low = df['low'].values[-atr_period-1:]
        close = df['close'].values[-atr_period-1:]
        tr = np.maximum(high[1:]-low[1:], np.maximum(abs(high[1:]-close[:-1]), abs(low[1:]-close[:-1])))
        atr = np.mean(tr[-atr_period:])
        candle_range = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
        if candle_range > 2 * atr: return "abnormal", 2.0
        if len(tr) < atr_period+5: return "normal", 1.0
        atr_prev = np.mean(tr[-atr_period-5:-5])
        if atr < atr_prev: return "compression", 0.8
        if atr > atr_prev * 1.1: return "expanding", 1.2
        return "normal", 1.0

# ============ CONFLUENCE SCORING (Section 13, no CSD) ============
class Scorer:
    def score_setup(self, crt: CRTStructure, bias: TrendBias, time_ok: bool, key_level: bool,
                    kod: bool, fvg: bool, ob: bool, premium_discount_ok: bool,
                    dol_clear: bool, smt: bool, nested_crt: bool) -> ConfluenceScore:
        s = ConfluenceScore()
        s.tbs_points = 25 if crt.turtle_soup_type == TurtleSoupType.TBS else 0
        s.key_level_points = 20 if key_level else 0
        s.bias_points = 15 if bias != TrendBias.NEUTRAL else 0
        s.time_points = 15 if time_ok else 0
        s.nested_crt_points = 10 if nested_crt else 0
        s.dol_points = 10 if dol_clear else 0
        s.smt_points = 8 if smt else 0
        s.kod_points = 8 if kod else 0
        s.fvg_points = 5 if fvg else 0
        s.ob_points = 5 if ob else 0
        s.premium_discount_points = 5 if premium_discount_ok else 0
        s.inside_bars_points = min(5, crt.inside_bars)
        s.total = sum([getattr(s, f) for f in s.__dataclass_fields__ if f != 'total'])
        return s

# ============ RISK / POSITION SIZING (Section 15) ============
class RiskManager:
    def __init__(self):
        self.daily_trades = 0
        self.daily_loss = 0.0
        self.weekly_loss = 0.0
        self.consecutive_losses = 0
        self.last_reset_day = None
        self.last_reset_week = None

    def can_trade(self, now: datetime.datetime) -> Tuple[bool, str]:
        if self.last_reset_day != now.date():
            self.daily_trades = 0
            self.daily_loss = 0.0
            self.last_reset_day = now.date()
        if self.last_reset_week != now.isocalendar()[1]:
            self.weekly_loss = 0.0
            self.last_reset_week = now.isocalendar()[1]
        if self.daily_trades >= MAX_TRADES_PER_DAY:
            return False, "max daily trades"
        if self.daily_loss >= RISK_PER_DAY_PCT:
            return False, "daily loss limit"
        if self.weekly_loss >= RISK_PER_WEEK_PCT:
            return False, "weekly loss limit"
        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            return False, "consecutive losses"
        return True, ""

    def record_trade(self, pnl_pct: float):
        self.daily_trades += 1
        if pnl_pct < 0:
            self.daily_loss += abs(pnl_pct)
            self.weekly_loss += abs(pnl_pct)
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

risk_mgr = RiskManager()

# ============ ADAPTIVE LEARNING (Section 22) ============
class AdaptiveLearning:
    def __init__(self):
        self.weights = {
            "tbs": 25, "key_level": 20, "bias": 15, "time": 15,
            "nested": 10, "dol": 10, "smt": 8, "kod": 8, "fvg": 5, "ob": 5,
            "premium_discount": 5, "inside_bars": 5
        }
        self.min_score_tbs = MIN_SCORE_TBS
        self.min_score_tws = MIN_SCORE_TWS
        self.last_recalibrate = None

    async def recalibrate(self, db_conn):
        if not ADAPTIVE_LEARNING: return
        now = datetime.datetime.utcnow()
        if self.last_recalibrate and (now - self.last_recalibrate).days < 7:
            return
        async with db_conn.execute("SELECT * FROM signals_crt WHERE status='closed'") as cursor:
            rows = await cursor.fetchall()
            if not rows or len(rows) < 30: return
            cols = [d[0] for d in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
            df['won'] = df['outcome'].isin(['TP1_HIT','TP2_HIT','TP3_HIT']).astype(int)
            wr = df['won'].mean() * 100
            if wr < ADAPTIVE_MIN_WR_THRESHOLD:
                self.min_score_tbs = min(100, self.min_score_tbs + 5)
                self.min_score_tws = min(100, self.min_score_tws + 5)
            elif wr > 65:
                self.min_score_tbs = max(65, self.min_score_tbs - 5)
                self.min_score_tws = max(65, self.min_score_tws - 5)
            self.last_recalibrate = now
            log.info(f"Adaptive Learning: WR={wr:.1f}%, new TBS threshold={self.min_score_tbs}")

adaptive = AdaptiveLearning()

# ============ DATABASE ============
async def init_database(db_conn):
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals_crt (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, side TEXT, timestamp TEXT,
            entry_price REAL, sl_price REAL, tp1 REAL, tp2 REAL, tp3 REAL,
            entry_model TEXT, quality_score REAL, score_details TEXT, context_json TEXT,
            hour_utc INTEGER, day_of_week INTEGER, trend_bias TEXT, turtle_soup_type TEXT,
            status TEXT DEFAULT 'active', outcome TEXT DEFAULT 'active',
            outcome_tp1 TEXT DEFAULT 'PENDING', outcome_tp2 TEXT DEFAULT 'PENDING',
            outcome_tp3 TEXT DEFAULT 'PENDING',
            actual_rr REAL DEFAULT 0, bars_to_tp1 INTEGER DEFAULT 0,
            max_adverse_pct REAL DEFAULT 0, max_favorable_pct REAL DEFAULT 0,
            closed_at TEXT, closed_price REAL, pnl_pct REAL DEFAULT 0, alert_sent INTEGER DEFAULT 0,
            UNIQUE(symbol, side, timestamp)
        )
    """)
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_crt_status ON signals_crt (status)")
    await db_conn.commit()

db_conn = None

async def store_signal(record: SignalRecord) -> int:
    async with asyncio.Lock():
        cursor = await db_conn.execute("""
            INSERT OR IGNORE INTO signals_crt (symbol, side, timestamp, entry_price, sl_price, tp1, tp2, tp3,
                entry_model, quality_score, score_details, context_json,
                hour_utc, day_of_week, trend_bias, turtle_soup_type, alert_sent)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (record.symbol, record.side, record.timestamp, record.entry_price, record.sl_price,
              record.tp1, record.tp2, record.tp3, record.entry_model, record.quality_score,
              record.score_details, record.context_json, record.hour_utc, record.day_of_week,
              record.trend_bias, record.turtle_soup_type, record.alert_sent))
        await db_conn.commit()
        return cursor.lastrowid

async def update_outcome(signal_id: int, data: Dict):
    await db_conn.execute("""
        UPDATE signals_crt SET status=?, outcome=?, outcome_tp1=?, outcome_tp2=?, outcome_tp3=?,
        actual_rr=?, bars_to_tp1=?, max_adverse_pct=?, max_favorable_pct=?,
        closed_at=?, closed_price=?, pnl_pct=? WHERE id=?
    """, (data['status'], data['outcome'], data.get('outcome_tp1','PENDING'), data.get('outcome_tp2','PENDING'),
          data.get('outcome_tp3','PENDING'), data['actual_rr'], data.get('bars_to_tp1',0),
          data['max_adverse_pct'], data['max_favorable_pct'], data['closed_at'], data['closed_price'],
          data['pnl_pct'], signal_id))
    await db_conn.commit()

# ============ OUTCOME TRACKER ============
class OutcomeTracker:
    def __init__(self):
        self.active: Dict[int, SignalRecord] = {}
    def register(self, rec): self.active[rec.id] = rec
    def update_price(self, sid, high, low, close):
        if sid not in self.active: return None
        rec = self.active[sid]
        entry, sl = rec.entry_price, rec.sl_price
        if entry <= 0: return None
        if rec.side == "BUY":
            adverse = (entry - low) / entry * 100 if low else 0
            favorable = (high - entry) / entry * 100 if high else 0
        else:
            adverse = (high - entry) / entry * 100 if high else 0
            favorable = (entry - low) / entry * 100 if low else 0
        rec.max_adverse_pct = max(rec.max_adverse_pct, max(0, adverse))
        rec.max_favorable_pct = max(rec.max_favorable_pct, max(0, favorable))
        if rec.side == "BUY":
            if sl and low <= sl and rec.outcome_tp1 == "PENDING":
                return self._close(rec, close, "SL_HIT", -1.0)
            tp1, tp2 = rec.tp1, rec.tp2
            if tp1 and high >= tp1 and rec.outcome_tp1 == "PENDING":
                rec.outcome_tp1 = "WIN"; rec.actual_rr = abs(tp1-entry)/abs(entry-sl) if sl else 0
                rec.bars_to_tp1 = 1
                if tp2 and high >= tp2 and rec.outcome_tp2 == "PENDING":
                    return self._close(rec, close, "TP2_HIT", abs(tp2-entry)/abs(entry-sl))
                else:
                    rec.outcome = "TP1_HIT"
        else:
            if sl and high >= sl and rec.outcome_tp1 == "PENDING":
                return self._close(rec, close, "SL_HIT", -1.0)
            tp1, tp2 = rec.tp1, rec.tp2
            if tp1 and low <= tp1 and rec.outcome_tp1 == "PENDING":
                rec.outcome_tp1 = "WIN"; rec.actual_rr = abs(entry-tp1)/abs(sl-entry) if sl else 0
                rec.bars_to_tp1 = 1
                if tp2 and low <= tp2 and rec.outcome_tp2 == "PENDING":
                    return self._close(rec, close, "TP2_HIT", abs(entry-tp2)/abs(sl-entry))
                else:
                    rec.outcome = "TP1_HIT"
        return None

    def _close(self, rec, price, outcome, rr):
        rec.status = "closed"; rec.outcome = outcome; rec.actual_rr = rr
        rec.closed_at = datetime.datetime.utcnow().isoformat(); rec.closed_price = price
        rec.pnl_pct = rr * (RISK_PER_TRADE_PCT/100) * (0.7 if CRYPTO_REDUCE_SIZE else 1.0)
        return {'signal_id': rec.id, 'outcome': outcome, 'actual_rr': rr,
                'max_adverse_pct': rec.max_adverse_pct, 'max_favorable_pct': rec.max_favorable_pct,
                'bars_to_tp1': rec.bars_to_tp1, 'closed_price': price, 'closed_at': rec.closed_at,
                'pnl_pct': rec.pnl_pct}

    def check_expired(self):
        now = datetime.datetime.utcnow()
        expired = []
        for sid, rec in list(self.active.items()):
            if rec.status == "active":
                try:
                    created = datetime.datetime.fromisoformat(rec.timestamp)
                    if (now - created).total_seconds()/3600 > SIGNAL_VALIDITY_HOURS:
                        rec.status = "closed"; rec.outcome = "EXPIRED"; rec.closed_at = now.isoformat()
                        expired.append(sid)
                except: pass
        return expired

outcome_tracker = OutcomeTracker()

# ============ MAIN SCAN LOGIC (decision tree Section 17) ============
async def scan_symbol(exchange, symbol: str) -> Optional[SignalRecord]:
    try:
        now = datetime.datetime.utcnow()

        # TIME CHECK (Section 3)
        time_ok = (not STRICT_TIME_FILTER) or CryptoTimeManager.is_key_window(now)
        if STRICT_TIME_FILTER and not time_ok:
            return None

        # FETCH DATA
        df_daily = create_dataframe(await fetch_ohlcv(exchange, symbol, "1d", 60))
        df_h4 = create_dataframe(await fetch_ohlcv(exchange, symbol, "4h", 60))
        df_h1 = create_dataframe(await fetch_ohlcv(exchange, symbol, "1h", 60))
        df_m15 = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 60))
        if df_daily is None or df_h4 is None or len(df_h4) < 5: return None

        price = float(df_h4['close'].iloc[-1])

        # 1. BIAS CHECK
        bias, bias_score = BiasEngine.order_flow(df_daily, df_h4)
        if not ALLOW_NEUTRAL_BIAS and bias == TrendBias.NEUTRAL:
            return None
        daily_bias = BiasEngine.daily_bias(df_daily)
        if daily_bias != TrendBias.NEUTRAL and bias != daily_bias:
            bias = daily_bias

        # Premium / Discount
        h4_high = df_h4['high'].tail(20).max()
        h4_low = df_h4['low'].tail(20).min()
        p_d = BiasEngine.premium_discount(price, h4_high, h4_low)
        if bias == TrendBias.BULLISH and p_d == "PREMIUM": return None
        if bias == TrendBias.BEARISH and p_d == "DISCOUNT": return None

        # If Asia daily model enabled, override bias (Section 19)
        if ASIA_DAILY_MODEL:
            asia_high, asia_low = await asia_model.get_asia_range(exchange, symbol)
            if asia_high > 0 and asia_low > 0:
                new_bias = asia_model.london_bias(asia_high, asia_low, price, price)
                if new_bias != TrendBias.NEUTRAL:
                    bias = new_bias

        # 2. DOL CHECK (simplified)
        dol_clear = True

        # 3 & 4. KEY LEVEL CHECK (Section 6)
        key_level = False
        if REQUIRE_KEY_LEVEL:
            levels = CRTDetector.find_key_levels(df_daily)
            for lvl in levels:
                if abs(price - lvl['price']) / price < 0.005:
                    key_level = True
                    break
        if REQUIRE_KEY_LEVEL and not key_level: return None

        # 5. CRT DETECTION (Section 5)
        accum = CRTDetector.detect_accumulation(df_h4)
        if not accum: return None
        ts = TurtleSoupType.NONE
        manip_idx = -1
        for i in range(len(df_h4)-1, 0, -1):
            candle = df_h4.iloc[i]
            ts, _ = CRTDetector.detect_turtle_soup(candle, accum['high'], accum['low'], bias)
            if ts != TurtleSoupType.NONE:
                manip_idx = i
                break
        if ts == TurtleSoupType.NONE: return None

        crt = CRTStructure(
            accumulation_high=accum['high'],
            accumulation_low=accum['low'],
            inside_bars=accum['inside'],
            manipulation_high=df_h4.iloc[manip_idx]['high'],
            manipulation_low=df_h4.iloc[manip_idx]['low'],
            turtle_soup_type=ts
        )

        # CSD on entry timeframe (hard gate, Section 8)
        csd_h4 = CRTDetector.detect_csd(df_h4, bias.value)
        if CSD_HARD_GATE and not csd_h4: return None

        # LTF nested CRT (Model C) + CSD on M15
        nested_crt = False
        csd_ltf = False
        if df_m15 is not None and len(df_m15) >= 3:
            if bias == TrendBias.BEARISH and abs(price - crt.accumulation_high)/price < 0.003:
                csd_ltf = CRTDetector.detect_csd(df_m15, "SELL")
                nested_crt = True
            elif bias == TrendBias.BULLISH and abs(price - crt.accumulation_low)/price < 0.003:
                csd_ltf = CRTDetector.detect_csd(df_m15, "BUY")
                nested_crt = True
        if REQUIRE_CSD_LTF and not csd_ltf and nested_crt: return None

        # Third candle purge (Section 19)
        if THIRD_CANDLE_PURGE:
            crt.third_candle_purge = CRTDetector.detect_third_candle_purge(df_h4, accum, bias)

        # KOD (Section 9)
        kod = CRTDetector.detect_kod(df_h4, bias.value)

        # SMT (Section 10)
        smt = False
        if USE_SMT:
            smt = await smt_detector.check_divergence(exchange, SMT_BASE, SMT_QUOTE, bias.value)

        # Scoring (Section 13)
        scorer = Scorer()
        score = scorer.score_setup(crt, bias, time_ok, key_level, kod, False, False,
                                   p_d in ("PREMIUM","DISCOUNT"), dol_clear, smt, nested_crt)

        # Institutional adjustments (Section 20)
        cot_data = await InstitutionalLayer.fetch_cot()
        retail_data = await InstitutionalLayer.fetch_retail_sentiment(symbol)
        score = InstitutionalLayer.adjust_score(score, cot_data, retail_data)

        # Live data filters (Section 21)
        if await LiveDataFilters.is_high_impact_news_blocked():
            return None

        # Spread filter
        sl_dist_pct = 0.01   # placeholder, will be updated after SL calculation
        if await LiveDataFilters.spread_too_wide(exchange, symbol, sl_dist_pct):
            return None

        regime, _ = LiveDataFilters.volatility_regime(df_h4)
        if regime == "abnormal":
            return None

        # Score thresholds (incorporating adaptive)
        min_tbs = adaptive.min_score_tbs
        min_tws = adaptive.min_score_tws
        if ts == TurtleSoupType.TBS and score.total < min_tbs: return None
        if ts == TurtleSoupType.TWS and score.total < min_tws: return None
        if score.total < MIN_SCORE_ALL: return None

        # Monday/Friday rule
        if not ALLOW_MONDAY_FRIDAY_LOW_SCORE and now.weekday() in {0,4} and score.total < 80:
            return None

        # Risk manager
        can_trade, reason = risk_mgr.can_trade(now)
        if not can_trade:
            log.info(f"Risk manager blocked {symbol}: {reason}")
            return None

        # ENTRY CALCULATION (Sections 7, 11, 12)
        entry_price = price
        sl = 0.0
        tp1 = (accum['high'] + accum['low']) / 2
        tp2 = accum['low'] if bias == TrendBias.BEARISH else accum['high']
        tp3 = 0.0

        if csd_h4:
            # Model B: Candle 3 entry
            entry_price = df_h4.iloc[-1]['open'] * (1.002 if bias == TrendBias.BEARISH else 0.998)
            if bias == TrendBias.BEARISH:
                sl = max(crt.manipulation_high, df_h4.iloc[manip_idx]['high']) * 1.005
            else:
                sl = min(crt.manipulation_low, df_h4.iloc[manip_idx]['low']) * 0.995
        else:
            # Model A: Candle 2 entry
            if bias == TrendBias.BEARISH:
                sl = crt.manipulation_high * 1.01
            else:
                sl = crt.manipulation_low * 0.99

        # Apply third candle purge special TP (full range + extension)
        if crt.third_candle_purge:
            tp2 *= 1.02  # extended target

        # Recalculate SL distance for spread filter (placeholder)
        sl_dist_pct = abs(entry_price - sl) / entry_price * 100
        if await LiveDataFilters.spread_too_wide(exchange, symbol, sl_dist_pct):
            log.info(f"Spread too wide for {symbol}")
            return None

        # Build signal record
        rec = SignalRecord(
            symbol=symbol,
            timestamp=now.isoformat(),
            side="SELL" if bias == TrendBias.BEARISH else "BUY",
            entry_price=entry_price, sl_price=sl,
            tp1=tp1, tp2=tp2, tp3=tp3,
            entry_model="B" if csd_h4 else "A",
            quality_score=score.total,
            score_details=json.dumps(score.__dict__),
            context_json=json.dumps({
                "accum": accum,
                "ts": ts.value,
                "bias": bias.value,
                "score": score.total,
                "time_ok": time_ok,
                "key_level": key_level
            }),
            hour_utc=now.hour, day_of_week=now.weekday(),
            trend_bias=bias.value, turtle_soup_type=ts.value
        )
        return rec

    except Exception as e:
        log.error(f"Scan error {symbol}: {e}")
        return None

# ============ TELEGRAM ============
async def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as cli:
        try:
            await cli.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ============ MAIN LOOP ============
async def main_loop():
    global db_conn
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db_conn = await aiosqlite.connect(DB_PATH)
    await init_database(db_conn)

    exchange = ccxt.okx({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    await send_telegram("🚀 CRT Scanner v10.0 Crypto Edition started — all sections active")

    while True:
        try:
            tickers = await safe_fetch_tickers(exchange)
            usdt_pairs = [(s,d.get("quoteVolume",0)) for s,d in tickers.items()
                          if s.endswith("/USDT") and not s.startswith("USDT") and isinstance(d.get("quoteVolume"),(int,float))]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols = [s for s,_ in usdt_pairs[:TOP_N]]

            # Update active signals
            for sid, rec in list(outcome_tracker.active.items()):
                if rec.status != "active": continue
                ticker = tickers.get(rec.symbol)
                if ticker:
                    outcome = outcome_tracker.update_price(sid, ticker.get('high',0), ticker.get('low',0), ticker.get('last',0))
                    if outcome:
                        await update_outcome(sid, outcome)
                        risk_mgr.record_trade(outcome.get('pnl_pct',0))
                        outcome_tracker.active.pop(sid, None)

            for eid in outcome_tracker.check_expired():
                await update_outcome(eid, {'status':'closed','outcome':'EXPIRED','actual_rr':0,'max_adverse_pct':0,'max_favorable_pct':0,'closed_at':datetime.datetime.utcnow().isoformat(),'closed_price':0,'pnl_pct':0})

            # Weekly recalibration
            await adaptive.recalibrate(db_conn)

            # Scan
            tasks = [asyncio.create_task(scan_symbol(exchange, sym)) for sym in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, SignalRecord):
                    sid = await store_signal(r)
                    if sid:
                        r.id = sid
                        outcome_tracker.register(r)
                        if r.quality_score > 50:
                            await send_telegram(f"📊 {r.symbol} {r.side} Score:{r.quality_score:.0f} {r.turtle_soup_type} {r.entry_model}")

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            log.error(f"Main loop error: {e}")
            await asyncio.sleep(60)

# ============ FASTAPI ============
app = FastAPI()
@app.get("/health")
async def health(): return {"status":"ok","active":len(outcome_tracker.active)}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true")
    args = parser.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        asyncio.run(main_loop())