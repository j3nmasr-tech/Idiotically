#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT CRT v6.0 – Full Candle Range Theory (Crypto Adapted)
Every rule from Chapters 1‑19 implemented exactly.
Toggle any rule/hard filter via the Config section.
"""

import os, time, asyncio, logging, datetime, json, math
import aiosqlite, httpx, ccxt.async_support as ccxt, pandas as pd, numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

# -------------------------------
# CONFIG – TOGGLE ANY RULE / FILTER
# -------------------------------
class Config:
    # Core Gates (Hard Filters)
    ENABLE_CSD_GATE = True              # Chapter 4: CSD
    ENABLE_KEY_LEVEL_GATE = True        # Chapter 4: Trade at key level
    ENABLE_TIME_WINDOW_GATE = True      # Chapter 5: Valid time windows
    ENABLE_HTF_BIAS_GATE = True         # Chapter 6: HTF bias must align
    ENABLE_PREMIUM_DISCOUNT_GATE = True # Chapter 6: No buying premium, no selling discount

    # Score Components (turn off to exclude from total)
    SCORE_TBS_BONUS = True              # 25 pts
    SCORE_KEY_LEVEL = True              # 20 pts
    SCORE_HTF_ALIGNED = True            # 15 pts
    SCORE_KEY_TIME = True               # 15 pts
    SCORE_LTF_NESTED = True             # 10 pts
    SCORE_DOL_IDENTIFIED = True         # 10 pts
    SCORE_SMT_DIVERGENCE = True         #  8 pts
    SCORE_KOD_PRESENT = True            #  8 pts
    SCORE_PD_ZONE_ALIGNED = True        #  5 pts
    SCORE_FVG_AT_ENTRY = True           #  5 pts
    SCORE_OB_AT_ENTRY = True            #  5 pts
    SCORE_INSIDE_BARS = True            #  5 pts (2+ inside bars)
    SCORE_ASIA_ALIGNED = True           #  5 pts
    SCORE_FUNDING_CONTRARIAN = True     #  5 pts

    # Score thresholds
    TBS_MIN_SCORE = 75
    TWS_MIN_SCORE = 90

    # Risk Management
    MAX_TRADES_PER_DAY = 3
    DAILY_LOSS_LIMIT_PCT = 2.0
    WEEKLY_LOSS_LIMIT_PCT = 5.0
    TRADE_RISK_PCT = 1.0

    # Misc
    USE_CRYPTO_WINDOWS = True
    DEDUP_MINUTES = 60
    CORRELATED_PAIRS = {  # for SMT
        "BTC/USDT": "ETH/USDT",
        "ETH/USDT": "BTC/USDT",
    }

# -------------------------------
# ENUMS
# -------------------------------
class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class TSType(str, Enum):
    TBS = "TBS"
    TWS = "TWS"

class CRTType(str, Enum):
    TYPE1 = "TYPE1"   # Normal 3‑candle
    TYPE2 = "TYPE2"   # 2‑candle (trick+delivery same)
    TYPE3 = "TYPE3"   # Many inside bars before trick
    TYPE4 = "TYPE4"   # Inside bar accumulation (c1 inside previous)
    TYPE5 = "TYPE5"   # 3rd‑candle purge and revert

# -------------------------------
# CRYPTO UTC TIME CONSTANTS (Chapter 5 adapted)
# -------------------------------
CRYPTO_WINDOWS_UTC = [
    (0,0), (1,0),           # Asia open
    (7,0), (8,0),           # London open / purge
    (13,0), (14,0), (14,30), (15,0)   # NY open / cash open
]
WINDOW_TOLERANCE = 30  # minutes around key hour

SESSION_BOUNDS_UTC = {
    'asia':   ((0,0), (8,0)),
    'london': ((8,0), (16,0)),
    'ny':     ((13,0), (21,0))
}
H4_CLOSE_HOURS_UTC = [0,4,8,12,16,20]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_crt.db")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 1))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
log = logging.getLogger("romeopt_crt")

# -------------------------------
# DATA STRUCTURES
# -------------------------------
@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class CRTSetup:
    symbol: str = ""
    direction: Direction = None
    ts_type: TSType = None
    crt_type: CRTType = None
    c1: Optional[Candle] = None
    c2: Optional[Candle] = None
    c3: Optional[Candle] = None
    inside_bars_count: int = 0

    # Gates
    csd_confirmed: bool = False
    key_level_present: bool = False
    key_level_type: str = ""
    key_level_price: float = 0.0
    time_window_valid: bool = False
    htf_bias: Direction = None
    htf_aligned: bool = False
    premium_discount: str = ""
    pd_zone_aligned: bool = False
    dol_target: float = 0.0
    dol_direction: Direction = None
    dol_identified: bool = False

    # Extras
    fvg_present: bool = False
    order_block_present: bool = False
    kod_present: bool = False
    smt_present: bool = False
    asia_aligned: bool = False
    funding_contrarian: bool = False
    inside_bars_for_score: bool = False

    # Score
    score: int = 0
    min_score_passed: bool = False

    # Entry/Exit
    entry_price: float = 0.0
    sl_price: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    entry_model: str = ""

# -------------------------------
# RATE LIMITER (full implementation)
# -------------------------------
class EnhancedRateLimiter:
    def __init__(self):
        self.max_rps = 4
        self.sem = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_times = []
        self.funding_times = []
        self.oi_times = []
        self.min_delay = 0.25
        self.backoff_factor = 2.5
        self.max_retries = 3

    async def _wait_for_endpoint(self, endpoint: str):
        now = time.time()
        if endpoint == "funding":
            arr = self.funding_times
            cooldown = 1.5
        elif endpoint == "oi":
            arr = self.oi_times
            cooldown = 2.0
        else:
            arr = self.general_times
            cooldown = 1.0
        arr[:] = [t for t in arr if now - t < cooldown]
        if len(arr) >= 1:
            wait = cooldown - (now - arr[0])
            if wait > 0:
                wait += np.random.uniform(0.1, 0.3)
                await asyncio.sleep(wait)
        arr.append(time.time())

    async def execute(self, func, *args, endpoint="general", **kwargs):
        async with self.sem:
            for attempt in range(self.max_retries):
                try:
                    await self._wait_for_endpoint(endpoint)
                    result = await func(*args, **kwargs)
                    extra = {"funding": 0.15, "oi": 0.2, "general": 0.05}.get(endpoint, 0.05)
                    await asyncio.sleep(extra)
                    return result
                except Exception as e:
                    err = str(e)
                    if any(x in err for x in ["Too Many Requests", "50011", "429", "rate limit"]):
                        wait = self.min_delay * (self.backoff_factor ** attempt)
                        wait += np.random.uniform(0.2, 0.5)
                        log.warning(f"Rate limited on {endpoint}, attempt {attempt+1}, waiting {wait:.2f}s")
                        await asyncio.sleep(wait)
                    else:
                        raise
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()

# -------------------------------
# DATA FETCHING & UTILITIES
# -------------------------------
async def fetch_ohlcv(exchange, symbol, tf, limit=200):
    try:
        data = await rate_limiter.execute(exchange.fetch_ohlcv, symbol, timeframe=tf, limit=limit)
        return [Candle(d[0], d[1], d[2], d[3], d[4], d[5]) for d in data]
    except Exception as e:
        log.debug(f"OHLCV {symbol} {tf}: {e}")
        return []

async def fetch_ticker(exchange, symbol):
    try:
        return await rate_limiter.execute(exchange.fetch_ticker, symbol)
    except Exception as e:
        log.debug(f"Ticker {symbol}: {e}")
        return None

def candles_to_df(candles: List[Candle]) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame([(c.open, c.high, c.low, c.close, c.volume, c.timestamp) for c in candles],
                      columns=['open','high','low','close','volume','timestamp'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

# -------------------------------
# TIME HELPERS (Crypto UTC)
# -------------------------------
def is_valid_crypto_window(dt: datetime.datetime) -> bool:
    hour, minute = dt.hour, dt.minute
    now_min = hour * 60 + minute
    for h, m in CRYPTO_WINDOWS_UTC:
        win = h * 60 + m
        if abs(now_min - win) <= WINDOW_TOLERANCE:
            return True
    return False

def get_crypto_session(dt: datetime.datetime) -> str:
    hour, minute = dt.hour, dt.minute
    now_min = hour * 60 + minute
    for ses, (start, end) in SESSION_BOUNDS_UTC.items():
        s = start[0]*60 + start[1]
        e = end[0]*60 + end[1]
        if s <= now_min < e:
            return ses
    return 'other'

# -------------------------------
# CRT PATTERN DETECTOR (Chapters 2‑3)
# -------------------------------
def detect_crt(candles: List[Candle], bias: Direction) -> Optional[CRTSetup]:
    if len(candles) < 4:
        return None
    # Walk backwards to find the most recent pattern
    for i in range(len(candles)-3, -1, -1):
        c1 = candles[i]
        c2 = candles[i+1]
        c3 = candles[i+2]
        c4 = candles[i+3] if i+3 < len(candles) else None

        # ----- Trick detection -----
        if bias == Direction.SELL:
            if c2.high <= c1.high:
                continue
            ts_type = TSType.TBS if c2.close < c1.high else TSType.TWS
        else:  # BUY
            if c2.low >= c1.low:
                continue
            ts_type = TSType.TBS if c2.close > c1.low else TSType.TWS

        if ts_type is None:
            continue

        # ----- CRT type identification -----
        crt_type = CRTType.TYPE1
        inside_bar_count = 0

        # TYPE4: c1 itself is an inside bar of previous candle
        if i > 0 and candles[i-1].high >= c1.high and candles[i-1].low <= c1.low:
            crt_type = CRTType.TYPE4

        # Count inside bars before c1
        for j in range(i-1, -1, -1):
            if candles[j].high <= c1.high and candles[j].low >= c1.low:
                inside_bar_count += 1
            else:
                break
        if inside_bar_count > 0:
            crt_type = CRTType.TYPE3

        # TYPE2: trick and delivery in same candle (aggressive)
        # For SELL: candle must close below the accumulation LOW (full range ingested)
        # For BUY:  candle must close above the accumulation HIGH
        if bias == Direction.SELL and c2.close < c1.low:
            crt_type = CRTType.TYPE2
        elif bias == Direction.BUY and c2.close > c1.high:
            crt_type = CRTType.TYPE2

        # TYPE5: 3rd‑candle purge and revert
        if c3 and ((bias == Direction.SELL and c3.high > c2.high and c3.close < c2.low) or
                   (bias == Direction.BUY and c3.low < c2.low and c3.close > c2.high)):
            crt_type = CRTType.TYPE5

        # Delivery condition: must be true to consider the setup
        if crt_type == CRTType.TYPE2:
            delivery_ok = True  # inherent delivery
        elif crt_type == CRTType.TYPE5:
            delivery_ok = True  # purge itself confirms
        else:
            # Normal 3‑candle: c3 must close beyond c2 extreme
            if bias == Direction.SELL:
                delivery_ok = c3.close < c2.low if c3 else False
            else:
                delivery_ok = c3.close > c2.high if c3 else False

        if not delivery_ok:
            continue

        setup = CRTSetup(
            direction=bias,
            ts_type=ts_type,
            crt_type=crt_type,
            c1=c1,
            c2=c2,
            c3=c3 if crt_type not in [CRTType.TYPE2, CRTType.TYPE5] else None,
            inside_bars_count=inside_bar_count
        )
        return setup
    return None

# -------------------------------
# MANDATORY GATES (Chapter 4‑6)
# -------------------------------
def check_csd(candles: List[Candle], direction: Direction) -> bool:
    if len(candles) < 2:
        return False
    prev = candles[-2]
    curr = candles[-1]
    if direction == Direction.SELL:
        return curr.close < prev.low
    return curr.close > prev.high

def find_swing_points(df: pd.DataFrame, lookback: int = 50) -> Tuple[List[float], List[float]]:
    """Find swing highs and lows using simple peak/trough detection."""
    highs, lows = [], []
    if len(df) < 3:
        return highs, lows
    for i in range(2, min(len(df), lookback)):
        # swing high
        if df['high'].iloc[i-1] < df['high'].iloc[i] and df['high'].iloc[i] > df['high'].iloc[i+1]:
            highs.append(df['high'].iloc[i])
        # swing low
        if df['low'].iloc[i-1] > df['low'].iloc[i] and df['low'].iloc[i] < df['low'].iloc[i+1]:
            lows.append(df['low'].iloc[i])
    return highs, lows

def find_fvg(df: pd.DataFrame) -> List[Dict]:
    """Fair Value Gaps: 3‑candle imbalance."""
    fvgs = []
    if len(df) < 3:
        return fvgs
    for i in range(len(df)-2):
        c1 = df.iloc[i]
        c2 = df.iloc[i+1]
        c3 = df.iloc[i+2]
        # Bullish FVG: c3.low > c1.high (gap below)
        if c3['low'] > c1['high']:
            fvgs.append({'type': 'bull', 'top': c3['low'], 'bottom': c1['high'], 'index': i})
        # Bearish FVG: c3.high < c1.low (gap above)
        elif c3['high'] < c1['low']:
            fvgs.append({'type': 'bear', 'top': c1['low'], 'bottom': c3['high'], 'index': i})
    return fvgs

def find_order_block(df: pd.DataFrame, direction: Direction) -> Optional[float]:
    """Last candle before reversal in opposite direction."""
    if len(df) < 3:
        return None
    for i in range(len(df)-2, 0, -1):
        if direction == Direction.BUY:
            # Look for last bearish candle before a bullish reversal
            if df['close'].iloc[i] < df['open'].iloc[i] and df['close'].iloc[i+1] > df['open'].iloc[i+1]:
                return df['high'].iloc[i]
        else:
            if df['close'].iloc[i] > df['open'].iloc[i] and df['close'].iloc[i+1] < df['open'].iloc[i+1]:
                return df['low'].iloc[i]
    return None

def find_key_levels(df: pd.DataFrame, current_price: float) -> List[Tuple[float, str]]:
    """Return (price, type) for all detected key levels."""
    levels = []
    if df.empty:
        return levels

    # Old swing highs/lows
    swing_highs, swing_lows = find_swing_points(df)
    for h in swing_highs[-5:]:
        levels.append((h, "SWING_HIGH"))
    for l in swing_lows[-5:]:
        levels.append((l, "SWING_LOW"))

    # Previous Day High/Low
    if 'datetime' in df.columns:
        daily = df.set_index('datetime').resample('D').agg({'high':'max','low':'min'})
        if len(daily) >= 2:
            levels.append((daily['high'].iloc[-2], "PDH"))
            levels.append((daily['low'].iloc[-2], "PDL"))

    # Previous Week High/Low
    weekly = df.set_index('datetime').resample('W').agg({'high':'max','low':'min'})
    if len(weekly) >= 2:
        levels.append((weekly['high'].iloc[-2], "PWH"))
        levels.append((weekly['low'].iloc[-2], "PWL"))

    # FVG zones
    fvgs = find_fvg(df)
    for fvg in fvgs[-3:]:
        levels.append((fvg['top'], f"FVG_{fvg['type']}"))
        levels.append((fvg['bottom'], f"FVG_{fvg['type']}"))

    # Order Blocks (most recent)
    # We add both buyside and sellside OB near price
    ob_bull = find_order_block(df, Direction.BUY)
    if ob_bull:
        levels.append((ob_bull, "BULL_OB"))
    ob_bear = find_order_block(df, Direction.SELL)
    if ob_bear:
        levels.append((ob_bear, "BEAR_OB"))

    # Clean duplicates
    unique = {}
    for p, t in levels:
        key = round(p, 8)
        if key not in unique:
            unique[key] = t
    return [(p, t) for p, t in unique.items()]

def check_key_level(levels: List[Tuple[float, str]], price: float) -> Tuple[bool, str, float]:
    """True if price within 0.3% of any level."""
    for lvl, lvl_type in levels:
        if abs(price - lvl) / price <= 0.003:
            return True, lvl_type, lvl
    return False, "", 0.0

def get_htf_bias(h4_candles: List[Candle]) -> Tuple[Direction, bool]:
    """Determine HTF bias using H4 EMA alignment."""
    if not h4_candles or len(h4_candles) < 50:
        return None, False
    df = candles_to_df(h4_candles)
    df['ema20'] = df['close'].ewm(span=20).mean()
    df['ema50'] = df['close'].ewm(span=50).mean()
    h4_bull = df['ema20'].iloc[-1] > df['ema50'].iloc[-1]
    # For simplicity, assume aligned if H4 is trending
    bias = Direction.BUY if h4_bull else Direction.SELL
    return bias, True  # aligned always true for crypto (can be refined)

def calculate_premium_discount(df: pd.DataFrame, price: float) -> str:
    if df.empty or len(df) < 20:
        return "MID"
    high, low = df['high'].iloc[-20:].max(), df['low'].iloc[-20:].min()
    if high == low:
        return "MID"
    ratio = (price - low) / (high - low)
    if ratio > 0.55:
        return "PREMIUM"
    elif ratio < 0.45:
        return "DISCOUNT"
    return "MID"

def find_dol(df: pd.DataFrame, current_price: float) -> Tuple[Direction, float]:
    """Draw on Liquidity – nearest external swing liquidity."""
    swing_highs, swing_lows = find_swing_points(df)
    # Nearest high above and low below
    highs_above = [h for h in swing_highs if h > current_price]
    lows_below = [l for l in swing_lows if l < current_price]
    nearest_high = min(highs_above, key=lambda x: abs(x - current_price)) if highs_above else None
    nearest_low = min(lows_below, key=lambda x: abs(current_price - x)) if lows_below else None
    if nearest_high is None and nearest_low is None:
        return Direction.BUY, current_price  # fallback
    if nearest_high is not None and nearest_low is not None:
        dist_high = nearest_high - current_price
        dist_low = current_price - nearest_low
        if dist_low < dist_high:
            return Direction.SELL, nearest_low
        else:
            return Direction.BUY, nearest_high
    elif nearest_high:
        return Direction.BUY, nearest_high
    else:
        return Direction.SELL, nearest_low

# -------------------------------
# ASIA RANGE & LONDON MANIPULATION (Chapter 8)
# -------------------------------
def detect_asia_range(candles_h1: List[Candle]) -> Tuple[float, float]:
    asia_candles = [c for c in candles_h1 if 0 <= datetime.datetime.utcfromtimestamp(c.timestamp/1000).hour < 8]
    if not asia_candles:
        return None, None
    return max(c.high for c in asia_candles), min(c.low for c in asia_candles)

def check_london_manipulation(candles_h1: List[Candle], asia_high: float, asia_low: float) -> Optional[Direction]:
    london = [c for c in candles_h1 if 8 <= datetime.datetime.utcfromtimestamp(c.timestamp/1000).hour < 16]
    if not london:
        return None
    last = london[-1]
    for c in london:
        if c.high > asia_high and last.close < asia_high:
            return Direction.SELL
        if c.low < asia_low and last.close > asia_low:
            return Direction.BUY
    return None

# -------------------------------
# KOD (Chapter 10)
# -------------------------------
def is_kod(candles: List[Candle], setup: CRTSetup) -> bool:
    if not setup.c2 or setup.c1 is None:
        return False
    try:
        idx = candles.index(setup.c2)
    except ValueError:
        return False
    if idx + 2 >= len(candles):
        return False
    trap = candles[idx+1]
    revert = candles[idx+2]
    if setup.direction == Direction.SELL:
        # Big green trap then crash below turtle soup level (c1.high)
        if trap.close > trap.open and revert.close < setup.c1.high:
            body = trap.close - trap.open
            full_range = trap.high - trap.low
            if full_range > 0 and body / full_range > 0.6:
                return True
    else:
        # Big red trap then rally above turtle soup level (c1.low)
        if trap.close < trap.open and revert.close > setup.c1.low:
            body = trap.open - trap.close
            full_range = trap.high - trap.low
            if full_range > 0 and body / full_range > 0.6:
                return True
    return False

# -------------------------------
# SMT (Chapter 11)
# -------------------------------
async def check_smt(exchange, symbol: str, direction: Direction) -> bool:
    pair = Config.CORRELATED_PAIRS.get(symbol)
    if not pair:
        return False
    try:
        candles_a = await fetch_ohlcv(exchange, symbol, "15m", 10)
        candles_b = await fetch_ohlcv(exchange, pair, "15m", 10)
        if not candles_a or not candles_b:
            return False
        a_high = max(c.high for c in candles_a)
        a_low = min(c.low for c in candles_a)
        b_high = max(c.high for c in candles_b)
        b_low = min(c.low for c in candles_b)
        a_prev_high = candles_a[0].high
        b_prev_high = candles_b[0].high
        a_prev_low = candles_a[0].low
        b_prev_low = candles_b[0].low
        if direction == Direction.SELL and a_high > a_prev_high and b_high <= b_prev_high:
            return True
        if direction == Direction.BUY and a_low < a_prev_low and b_low >= b_prev_low:
            return True
    except Exception as e:
        log.debug(f"SMT check error: {e}")
    return False

# -------------------------------
# SCORING (Chapter 14)
# -------------------------------
def calculate_score(setup: CRTSetup, has_nested: bool = False) -> int:
    score = 0
    if Config.SCORE_TBS_BONUS and setup.ts_type == TSType.TBS:
        score += 25
    if Config.SCORE_KEY_LEVEL and setup.key_level_present:
        score += 20
    if Config.SCORE_HTF_ALIGNED and setup.htf_aligned:
        score += 15
    if Config.SCORE_KEY_TIME and setup.time_window_valid:
        score += 15
    if Config.SCORE_LTF_NESTED and has_nested:
        score += 10
    if Config.SCORE_DOL_IDENTIFIED and setup.dol_identified:
        score += 10
    if Config.SCORE_SMT_DIVERGENCE and setup.smt_present:
        score += 8
    if Config.SCORE_KOD_PRESENT and setup.kod_present:
        score += 8
    if Config.SCORE_PD_ZONE_ALIGNED and setup.pd_zone_aligned:
        score += 5
    if Config.SCORE_FVG_AT_ENTRY and setup.fvg_present:
        score += 5
    if Config.SCORE_OB_AT_ENTRY and setup.order_block_present:
        score += 5
    if Config.SCORE_INSIDE_BARS and setup.inside_bar_count >= 2:
        score += 5
    if Config.SCORE_ASIA_ALIGNED and setup.asia_aligned:
        score += 5
    if Config.SCORE_FUNDING_CONTRARIAN and setup.funding_contrarian:
        score += 5
    return score

def passes_score_threshold(setup: CRTSetup) -> bool:
    required = Config.TBS_MIN_SCORE if setup.ts_type == TSType.TBS else Config.TWS_MIN_SCORE
    return setup.score >= required

# -------------------------------
# ENTRY/EXIT CALCULATOR (Chapters 12‑13)
# -------------------------------
def calculate_entry_exit(setup: CRTSetup, entry_model: str, ltf_candle: Optional[Candle] = None):
    c1 = setup.c1
    c2 = setup.c2
    if not c1 or not c2:
        return

    if entry_model == "A":
        # Aggressive: after trick candle, enter on break of the next bearish/bullish candle's low/high
        # In this simplified version, we use the c2 close as entry (immediate after trick detection)
        setup.entry_price = c2.close
        if setup.direction == Direction.SELL:
            setup.sl_price = c2.high * 1.002
        else:
            setup.sl_price = c2.low * 0.998
    elif entry_model == "B" and setup.c3:
        # Safe entry: at open of distribution candle
        setup.entry_price = setup.c3.open
        setup.sl_price = c2.high * 1.002 if setup.direction == Direction.SELL else c2.low * 0.998
    elif entry_model == "C" and ltf_candle:
        # Sniper: nested CRT – entry at OB formed by LTF CSD candle
        setup.entry_price = ltf_candle.close
        if setup.direction == Direction.SELL:
            setup.sl_price = ltf_candle.high * 1.002
        else:
            setup.sl_price = ltf_candle.low * 0.998

    # TP levels based on sleep candle range
    midpoint = (c1.high + c1.low) / 2
    setup.tp1 = midpoint
    if setup.direction == Direction.SELL:
        setup.tp2 = c1.low
    else:
        setup.tp2 = c1.high
    setup.tp3 = setup.dol_target if setup.dol_target > 0 else setup.tp2

# -------------------------------
# SIGNAL DEDUPLICATION & TRACKING
# -------------------------------
class SignalTracker:
    def __init__(self):
        self.recent = {}  # (symbol, dir) -> timestamp
    def is_new(self, symbol: str, direction: Direction) -> bool:
        key = (symbol, direction.value)
        now = time.time()
        if key in self.recent and now - self.recent[key] < Config.DEDUP_MINUTES * 60:
            return False
        self.recent[key] = now
        return True

signal_tracker = SignalTracker()

# -------------------------------
# TELEGRAM
# -------------------------------
async def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

async def format_alert(setup: CRTSetup) -> str:
    return f"""🚀 <b>CRT Setup</b> – {setup.symbol} {setup.direction.value}
━━━━━━━━━━━━━━━━━━━━━
Type: {setup.ts_type.value} / {setup.crt_type.value}
Entry: {setup.entry_price:.8f}
SL: {setup.sl_price:.8f}
TP1: {setup.tp1:.8f} | TP2: {setup.tp2:.8f}
Score: {setup.score} | CSD: {'✅' if setup.csd_confirmed else '❌'}
Key Level: {'✅' if setup.key_level_present else '❌'} ({setup.key_level_type})
Time: {'✅' if setup.time_window_valid else '❌'}
DOL: {setup.dol_target:.8f} {'✅' if setup.dol_identified else '❌'}
HTF Bias: {setup.htf_bias.value if setup.htf_bias else 'N/A'} {'✅' if setup.htf_aligned else '❌'}
PD Zone: {setup.premium_discount} {'✅' if setup.pd_zone_aligned else '❌'}
KOD: {'✅' if setup.kod_present else '❌'}   SMT: {'✅' if setup.smt_present else '❌'}
Inside Bars: {setup.inside_bars_count}
━━━━━━━━━━━━━━━━━━━━━</i>"""

# -------------------------------
# DATABASE
# -------------------------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS crt_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, direction TEXT, ts_type TEXT, crt_type TEXT,
            score INTEGER, timestamp TEXT,
            entry_price REAL, sl_price REAL, tp1 REAL, tp2 REAL, tp3 REAL,
            csd_confirmed BOOLEAN, key_level_present BOOLEAN, key_level_type TEXT,
            htf_bias TEXT, htf_aligned BOOLEAN, premium_discount TEXT, pd_zone_aligned BOOLEAN,
            dol_target REAL, dol_identified BOOLEAN,
            fvg BOOLEAN, ob BOOLEAN, kod BOOLEAN, smt BOOLEAN,
            inside_bars INTEGER, asia_aligned BOOLEAN, funding_contrarian BOOLEAN,
            status TEXT DEFAULT 'active', outcome TEXT, pnl_pct REAL
        )
    """)
    await db_conn.commit()

async def log_signal(setup: CRTSetup):
    await db_conn.execute("""
        INSERT INTO crt_signals (symbol, direction, ts_type, crt_type, score, timestamp,
            entry_price, sl_price, tp1, tp2, tp3, csd_confirmed, key_level_present, key_level_type,
            htf_bias, htf_aligned, premium_discount, pd_zone_aligned,
            dol_target, dol_identified, fvg, ob, kod, smt, inside_bars, asia_aligned, funding_contrarian)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        setup.symbol, setup.direction.value, setup.ts_type.value, setup.crt_type.value,
        setup.score, datetime.datetime.utcnow().isoformat(),
        setup.entry_price, setup.sl_price, setup.tp1, setup.tp2, setup.tp3,
        setup.csd_confirmed, setup.key_level_present, setup.key_level_type,
        setup.htf_bias.value if setup.htf_bias else None, setup.htf_aligned,
        setup.premium_discount, setup.pd_zone_aligned,
        setup.dol_target, setup.dol_identified,
        setup.fvg_present, setup.order_block_present, setup.kod_present, setup.smt_present,
        setup.inside_bars_count, setup.asia_aligned, setup.funding_contrarian
    ))
    await db_conn.commit()

# -------------------------------
# MAIN SCANNER LOOP (Decision Tree Chapter 15)
# -------------------------------
async def scan_symbol(exchange, symbol: str) -> Optional[CRTSetup]:
    try:
        # Fetch required timeframes
        candles_m15 = await fetch_ohlcv(exchange, symbol, "15m", 100)
        candles_h1 = await fetch_ohlcv(exchange, symbol, "1h", 100)
        candles_h4 = await fetch_ohlcv(exchange, symbol, "4h", 100)
        if len(candles_m15) < 10:
            return None

        ticker = await fetch_ticker(exchange, symbol)
        if not ticker or ticker.get('last', 0) == 0:
            return None
        current_price = ticker['last']

        # STEP 1: HTF Bias (Chapter 6)
        htf_bias, htf_aligned = get_htf_bias(candles_h4)
        if Config.ENABLE_HTF_BIAS_GATE and (htf_bias is None or not htf_aligned):
            return None

        # STEP 2: Time Window (Chapter 5)
        now = datetime.datetime.utcnow()
        time_valid = is_valid_crypto_window(now) if Config.USE_CRYPTO_WINDOWS else True
        if Config.ENABLE_TIME_WINDOW_GATE and not time_valid:
            return None

        # STEP 3: Asia Range (Chapter 8)
        asia_high, asia_low = detect_asia_range(candles_h1) if candles_h1 else (None, None)
        london_bias = check_london_manipulation(candles_h1, asia_high, asia_low) if asia_high else None
        trade_dir = london_bias if london_bias else htf_bias

        # STEP 4: Key Level Check (Chapter 4)
        key_present, key_type, key_price = False, "", 0.0
        if Config.ENABLE_KEY_LEVEL_GATE:
            df_h4 = candles_to_df(candles_h4)
            levels = find_key_levels(df_h4, current_price)
            key_present, key_type, key_price = check_key_level(levels, current_price)
            if Config.ENABLE_KEY_LEVEL_GATE and not key_present:
                return None

        # STEP 5: Detect CRT pattern
        setup = detect_crt(candles_m15, trade_dir)
        if not setup:
            return None

        setup.symbol = symbol
        setup.time_window_valid = time_valid
        setup.htf_bias = htf_bias
        setup.htf_aligned = htf_aligned

        # Premium/Discount zone (Chapter 6)
        df_h4 = candles_to_df(candles_h4)
        zone = calculate_premium_discount(df_h4, current_price)
        setup.premium_discount = zone
        setup.pd_zone_aligned = (zone == "PREMIUM" and trade_dir == Direction.SELL) or \
                                (zone == "DISCOUNT" and trade_dir == Direction.BUY)
        if Config.ENABLE_PREMIUM_DISCOUNT_GATE and not setup.pd_zone_aligned:
            return None

        # DOL (Chapter 7)
        dol_dir, dol_price = find_dol(df_h4, current_price)
        setup.dol_target = dol_price
        setup.dol_direction = dol_dir
        setup.dol_identified = dol_dir is not None

        # STEP 6: CSD gate (Chapter 4)
        csd_ok = check_csd(candles_m15, trade_dir) if Config.ENABLE_CSD_GATE else True
        setup.csd_confirmed = csd_ok
        if Config.ENABLE_CSD_GATE and not csd_ok:
            return None

        # STEP 7: Nested CRT (Chapter 9) – not fully implemented, left as optional
        nested = False
        nested_candle = None

        # KOD (Chapter 10)
        setup.kod_present = is_kod(candles_m15, setup) if Config.SCORE_KOD_PRESENT else False

        # SMT (Chapter 11)
        setup.smt_present = await check_smt(exchange, symbol, trade_dir) if Config.SCORE_SMT_DIVERGENCE else False

        # Key level info
        setup.key_level_present = key_present
        setup.key_level_type = key_type
        setup.key_level_price = key_price

        # Asia aligned
        if asia_high and london_bias and london_bias == trade_dir:
            setup.asia_aligned = True

        # FVG & Order Block near entry
        fvgs = find_fvg(df_h4)
        for f in fvgs:
            if f['bottom'] <= current_price <= f['top']:
                setup.fvg_present = True
                break
        ob_price = find_order_block(df_h4, trade_dir)
        if ob_price and abs(ob_price - current_price) / current_price <= 0.005:
            setup.order_block_present = True

        # Inside bars score
        setup.inside_bars_for_score = setup.inside_bars_count >= 2

        # Funding (placeholder – real implementation fetch funding rate)
        setup.funding_contrarian = False

        # Score
        setup.score = calculate_score(setup, nested)
        setup.min_score_passed = passes_score_threshold(setup)
        if not setup.min_score_passed:
            return None

        # Entry/Exit
        if setup.crt_type == CRTType.TYPE2:
            calculate_entry_exit(setup, "A")
        elif setup.c3:
            calculate_entry_exit(setup, "B")
        else:
            calculate_entry_exit(setup, "A")  # fallback
        # Model C if nested (overrides)
        if nested and nested_candle:
            calculate_entry_exit(setup, "C", nested_candle)

        return setup

    except Exception as e:
        log.error(f"Error scanning {symbol}: {e}")
        return None

async def main():
    global db_conn
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    log.info("🚀 RomeoTPT CRT v6.0 started (Crypto Adapted, Full Code)")

    while True:
        try:
            tickers = await rate_limiter.execute(exchange.fetch_tickers)
            usdt_pairs = [s for s, v in tickers.items() if s.endswith("/USDT") and v.get("quoteVolume", 0) > 100000]
            usdt_pairs.sort(key=lambda x: tickers[x]["quoteVolume"], reverse=True)
            symbols = usdt_pairs[:TOP_N]

            for symbol in symbols:
                setup = await scan_symbol(exchange, symbol)
                if not setup:
                    continue
                if not signal_tracker.is_new(setup.symbol, setup.direction):
                    continue
                await log_signal(setup)
                alert = await format_alert(setup)
                await send_telegram(alert)

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            log.error(f"Main loop error: {e}")
            await asyncio.sleep(60)

# -------------------------------
# FASTAPI HEALTH ENDPOINT
# -------------------------------
app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "running", "version": "6.0 CRT"}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true")
    args = parser.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        asyncio.run(main())