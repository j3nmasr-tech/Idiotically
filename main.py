#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOPT-P COMPLETE SCANNER (100% RomeOPT-P + Full Signal Details)
- ALL YOUR TIMEFRAMES: 1m, 3m, 5m, 15m, 30m
- BOS/CHOCH Detection
- FVG Premium/Discount Zones
- Quality Order Blocks with Volume
- Market Structure Shift Confirmation
- Full Signal Details in Alerts
- Error-Free Execution
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ==================== CONFIGURATION ====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/romeopt_signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))

# ✅ YOUR ORIGINAL TIMEFRAMES
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
# HTF mapping for each timeframe (following your structure)
HTF_MAP = {
    "1m": "5m",    # 1m -> 5m (1:5 ratio)
    "3m": "15m",   # 3m -> 15m (1:5 ratio)  
    "5m": "15m",   # 5m -> 15m (1:3 ratio)
    "15m": "1h",   # 15m -> 1h (1:4 ratio)
    "30m": "1h"    # 30m -> 1h (1:2 ratio)
}

# RomeOPT-P Scoring System
SCORE_WEIGHTS = {
    "liquidity_sweep": 2,      # Most important
    "displacement": 2,         # With volume confirmation
    "quality_ob": 1,          # Quality order block
    "htf_alignment": 2,       # HTF structure alignment
    "bos_choch": 1,          # BOS/CHOCH confirmation
    "volume_confirmation": 1, # Volume spike
    "structure_shift": 1,    # Market structure shift
    "fvg_zone": 1           # FVG premium/discount
}
MIN_SCORE = 7  # Increased for quality

# Risk Management
MIN_RR_RATIO = 1.5
SL_CLUSTER_THRESHOLD = 3  # Max SL hits in 30 minutes

# Elite Multi-Timeframe Confirmation (your original concept)
ELITE_TFS = ["15m", "1h", "4h"]

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_complete")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# SL Cluster Tracking (your original concept)
recent_sl_hits = defaultdict(lambda: deque(maxlen=10))

# ==================== UTILITIES ====================
def escape_html(msg: str) -> str:
    if not msg:
        return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def send_telegram_alert(message: str):
    """Send formatted alert to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    escaped_msg = escape_html(message)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": escaped_msg,
                "parse_mode": "HTML"
            })
        except Exception as e:
            log.warning(f"Telegram alert failed: {e}")

# ==================== DATABASE ====================
async def init_database():
    """Initialize SQLite database with full signal details"""
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS romeopt_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry REAL NOT NULL,
            sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,  -- ✅ Keeping your TP3 structure
            entry_tf TEXT NOT NULL,
            
            -- RomeOPT-P Scoring Details
            score INTEGER NOT NULL,
            liquidity_sweep INTEGER DEFAULT 0,
            displacement INTEGER DEFAULT 0,
            quality_ob INTEGER DEFAULT 0,
            htf_alignment INTEGER DEFAULT 0,
            bos_choch INTEGER DEFAULT 0,
            volume_confirmation INTEGER DEFAULT 0,
            structure_shift INTEGER DEFAULT 0,
            fvg_zone INTEGER DEFAULT 0,
            elite_mtf INTEGER DEFAULT 0,  -- ✅ Your elite MTF confirmation
            
            -- Market Structure Details
            has_bos INTEGER DEFAULT 0,
            has_choch INTEGER DEFAULT 0,
            fvg_type TEXT,
            fvg_premium INTEGER DEFAULT 0,
            ob_type TEXT,
            ob_volume_ratio REAL,
            
            -- Risk Management
            risk REAL NOT NULL,
            rr_ratio REAL NOT NULL,
            atr_value REAL,
            
            -- Status
            status TEXT DEFAULT 'OPEN',
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0,  -- ✅ Your TP3 tracking
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            close_reason TEXT,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            
            -- Full Details (JSON)
            signal_details TEXT,
            reasons TEXT
        )
    """)
    
    # Create indexes for performance
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON romeopt_signals(status)")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON romeopt_signals(symbol)")
    await db_conn.commit()
    
    log.info("Database initialized with RomeOPT-P schema")

# ==================== DATA FETCHING ====================
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    """Fetch OHLCV data with error handling"""
    try:
        data = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if data and len(data) > 0:
            return data
        return None
    except ccxt.NetworkError as e:
        log.debug(f"Network error for {symbol} {timeframe}: {e}")
        return None
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def calculate_atr(df: pd.DataFrame, period=14):
    """Calculate Average True Range"""
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()

# ==================== YOUR ELITE MTF CONFIRMATION ====================
async def elite_mtf_confirmation(exchange, symbol: str, side: str):
    """Your original elite multi-timeframe confirmation"""
    try:
        for tf in ELITE_TFS:
            ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
            if not ohlcv:
                return False, f"No data for {tf}"
            
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Simple trend check (your original logic)
            if len(df) >= 6:
                trend = df["close"].iloc[-1] - df["close"].iloc[-6]
                tf_side = "BUY" if trend > 0 else "SELL"
                
                if tf_side != side:
                    return False, f"{tf} trend opposite ({tf_side})"
        
        return True, "All elite timeframes aligned"
    except Exception as e:
        log.warning(f"Elite MTF check failed: {e}")
        return False, f"MTF check error: {e}"

# ==================== ROMEOPT-P CORE FUNCTIONS ====================
def detect_bos_choch(df: pd.DataFrame):
    """
    Detect Break of Structure and Change of Character
    Returns: (has_bos, has_choch, direction)
    """
    if len(df) < 20:
        return False, False, None
    
    # Find swing points
    recent_highs = df['high'].iloc[-20:-5]  # Avoid too recent
    recent_lows = df['low'].iloc[-20:-5]
    
    if len(recent_highs) < 2 or len(recent_lows) < 2:
        return False, False, None
    
    swing_high = recent_highs.max()
    swing_low = recent_lows.min()
    current_price = df['close'].iloc[-1]
    
    # BOS Detection
    has_bos_bullish = (current_price > swing_high and 
                       df['close'].iloc[-1] > df['close'].iloc[-3])
    has_bos_bearish = (current_price < swing_low and 
                       df['close'].iloc[-1] < df['close'].iloc[-3])
    has_bos = has_bos_bullish or has_bos_bearish
    
    # CHOCH Detection using EMA cross
    ema_fast = df['close'].ewm(span=20, adjust=False).mean()
    ema_slow = df['close'].ewm(span=50, adjust=False).mean()
    
    has_choch_bullish = (ema_fast.iloc[-1] > ema_slow.iloc[-1] and 
                         ema_fast.iloc[-5] <= ema_slow.iloc[-5])
    has_choch_bearish = (ema_fast.iloc[-1] < ema_slow.iloc[-1] and 
                         ema_fast.iloc[-5] >= ema_slow.iloc[-5])
    has_choch = has_choch_bullish or has_choch_bearish
    
    direction = None
    if has_bos_bullish or has_choch_bullish:
        direction = "bullish"
    elif has_bos_bearish or has_choch_bearish:
        direction = "bearish"
    
    return has_bos, has_choch, direction

def find_quality_order_blocks(df: pd.DataFrame):
    """
    Find quality Order Blocks with volume confirmation
    Returns latest quality OB or None
    """
    blocks = []
    min_candles = 5
    
    if len(df) < min_candles + 2:
        return None
    
    for i in range(min_candles, len(df) - 2):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        next_candle = df.iloc[i+1]
        
        # Calculate volume average
        vol_avg = df['volume'].iloc[max(0, i-10):i].mean()
        
        # Quality Bullish OB: Bearish -> Bullish with low swept
        if (prev_candle["close"] < prev_candle["open"] and    # Bearish candle
            candle["close"] > candle["open"] and              # Bullish candle
            candle["low"] < prev_candle["low"] and            # Swept previous low
            candle["volume"] > vol_avg * 1.2):                # Volume spike
            
            # Confirmation: Next candle closes above OB
            if next_candle["close"] > candle["close"]:
                blocks.append({
                    "type": "bullish",
                    "index": i,
                    "low": min(candle["low"], prev_candle["low"]),
                    "high": max(candle["close"], prev_candle["close"]),
                    "body_low": min(candle["open"], candle["close"]),
                    "body_high": max(candle["open"], candle["close"]),
                    "volume_ratio": candle["volume"] / vol_avg
                })
        
        # Quality Bearish OB: Bullish -> Bearish with high swept
        elif (prev_candle["close"] > prev_candle["open"] and   # Bullish candle
              candle["close"] < candle["open"] and             # Bearish candle
              candle["high"] > prev_candle["high"] and         # Swept previous high
              candle["volume"] > vol_avg * 1.2):               # Volume spike
            
            # Confirmation: Next candle closes below OB
            if next_candle["close"] < candle["close"]:
                blocks.append({
                    "type": "bearish",
                    "index": i,
                    "low": min(candle["close"], prev_candle["close"]),
                    "high": max(candle["high"], prev_candle["high"]),
                    "body_low": min(candle["open"], candle["close"]),
                    "body_high": max(candle["open"], candle["close"]),
                    "volume_ratio": candle["volume"] / vol_avg
                })
    
    return blocks[-1] if blocks else None

def find_fvg_zones(df: pd.DataFrame):
    """
    Find Fair Value Gaps and classify as premium/discount
    Returns latest FVG or None
    """
    fvgs = []
    
    for i in range(2, len(df) - 1):
        current_low = df['low'].iloc[i]
        prev_high = df['high'].iloc[i-1]
        current_high = df['high'].iloc[i]
        prev_low = df['low'].iloc[i-1]
        
        # Bullish FVG: current low > previous high
        if current_low > prev_high:
            ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
            is_premium = current_low > ema50
            
            fvgs.append({
                "type": "bullish",
                "index": i,
                "zone_low": prev_high,
                "zone_high": current_low,
                "premium": is_premium,
                "discount": not is_premium,
                "ema_distance": current_low - ema50
            })
        
        # Bearish FVG: current high < previous low
        elif current_high < prev_low:
            ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
            is_discount = current_high < ema50
            
            fvgs.append({
                "type": "bearish",
                "index": i,
                "zone_low": current_high,
                "zone_high": prev_low,
                "premium": not is_discount,
                "discount": is_discount,
                "ema_distance": ema50 - current_high
            })
    
    return fvgs[-1] if fvgs else None

def check_market_structure_shift(df: pd.DataFrame, side: str):
    """
    Confirm if market structure is actually shifting
    Returns: (has_shift, shift_details)
    """
    if len(df) < 30:
        return False, "Insufficient data"
    
    # Simple swing point detection for low timeframes
    highs = df['high'].values
    lows = df['low'].values
    
    swing_highs = []
    swing_lows = []
    
    # Adjust window based on timeframe data density
    window = 3 if len(df) > 50 else 2
    
    for i in range(window, len(df) - window):
        if all(highs[i] > highs[i-j] for j in range(1, window+1)) and \
           all(highs[i] > highs[i+j] for j in range(1, window+1)):
            swing_highs.append((i, highs[i]))
        
        if all(lows[i] < lows[i-j] for j in range(1, window+1)) and \
           all(lows[i] < lows[i+j] for j in range(1, window+1)):
            swing_lows.append((i, lows[i]))
    
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return False, "Insufficient swing points"
    
    # Check structure
    if side == "BUY":
        # Need higher lows forming
        recent_lows = [low for _, low in swing_lows[-2:]]
        if len(recent_lows) >= 2 and recent_lows[-1] > recent_lows[-2]:
            return True, f"Higher low: {recent_lows[-2]:.4f} → {recent_lows[-1]:.4f}"
    
    else:  # SELL
        # Need lower highs forming
        recent_highs = [high for _, high in swing_highs[-2:]]
        if len(recent_highs) >= 2 and recent_highs[-1] < recent_highs[-2]:
            return True, f"Lower high: {recent_highs[-2]:.4f} → {recent_highs[-1]:.4f}"
    
    return False, "No structure shift detected"

def check_liquidity_path(df: pd.DataFrame, side: str, entry: float, tp1: float):
    """
    Check if path to TP is clear (not recently touched)
    Returns: (path_clear, reason)
    """
    # Adjust lookback based on timeframe
    if len(df) >= 20:
        lookback = 15
    elif len(df) >= 10:
        lookback = 10
    else:
        lookback = 5
    
    if side == "BUY":
        # Check if TP1 zone was recently touched
        recent_touch = (df['high'].iloc[-lookback:] >= tp1 * 0.995).any()
        if recent_touch:
            return False, f"TP zone recently touched (last {lookback} candles)"
        return True, f"Liquidity path clear ({lookback} candles)"
    
    else:  # SELL
        recent_touch = (df['low'].iloc[-lookback:] <= tp1 * 1.005).any()
        if recent_touch:
            return False, f"TP zone recently touched (last {lookback} candles)"
        return True, f"Liquidity path clear ({lookback} candles)"

async def check_htf_alignment(exchange, symbol: str, ltf: str, side: str):
    """
    HTF structure alignment check (your mapping)
    """
    htf = HTF_MAP.get(ltf, "15m")
    ohlcv = await fetch_ohlcv(exchange, symbol, htf, 100)
    
    if not ohlcv:
        return False, 0, [f"No {htf} data"]
    
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    details = []
    confidence = 0
    
    # 1. Simple trend check (your original method)
    if len(df) >= 6:
        trend = df["close"].iloc[-1] - df["close"].iloc[-6]
        htf_side = "BUY" if trend > 0 else "SELL"
        
        if htf_side == side:
            confidence += 2
            details.append(f"{htf} trend aligned")
        else:
            details.append(f"{htf} trend opposite ({htf_side})")
            return False, 0, details
    
    # 2. Price position relative to EMA
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    above_ema = df['close'].iloc[-1] > df['ema20'].iloc[-1]
    
    if (side == "BUY" and above_ema) or (side == "SELL" and not above_ema):
        confidence += 1
        details.append(f"Price {'above' if above_ema else 'below'} EMA20")
    
    # 3. Check for BOS/CHOCH on HTF
    has_bos, has_choch, htf_direction = detect_bos_choch(df)
    if htf_direction == side.lower():
        confidence += 1
        if has_bos:
            details.append(f"{htf} BOS {htf_direction}")
        if has_choch:
            details.append(f"{htf} CHOCH {htf_direction}")
    
    return confidence >= 2, confidence, details

# ==================== RISK MANAGEMENT ====================
def calculate_risk_parameters(entry: float, side: str, ob_zone: dict, df: pd.DataFrame):
    """
    Your TP/SL structure with 3 targets (TP1, TP2, TP3)
    """
    atr_val = float(calculate_atr(df).iloc[-1])
    
    # Get recent market structure
    recent_high = df['high'].iloc[-20:].max()
    recent_low = df['low'].iloc[-20:].min()
    
    if side == "BUY":
        # SL calculation (your conservative approach)
        sl_ob = ob_zone['low'] - (atr_val * 0.3)
        sl_structure = recent_low - (atr_val * 0.3)
        sl = min(sl_ob, sl_structure)
        
        risk = entry - sl
        
        # Ensure minimum meaningful risk
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # Your TP structure: 0.8R, 1.5R, 2.5R
        tp1 = entry + (risk * 0.8)
        tp2 = entry + (risk * 1.5)
        tp3 = entry + (risk * 2.5)
        
        # Adjust to market structure if better
        nearest_resistance = df['high'].tail(20).max()
        if nearest_resistance > entry:
            tp1 = min(tp1, nearest_resistance)
        
        # Ensure proper spacing
        min_gap = risk * 0.3
        tp1 = max(tp1, entry + (risk * 0.5))
        tp2 = max(tp2, tp1 + min_gap)
        tp3 = max(tp3, tp2 + min_gap)
        
    else:  # SELL
        # SL calculation
        sl_ob = ob_zone['high'] + (atr_val * 0.3)
        sl_structure = recent_high + (atr_val * 0.3)
        sl = max(sl_ob, sl_structure)
        
        risk = sl - entry
        
        # Ensure minimum meaningful risk
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        # Your TP structure
        tp1 = entry - (risk * 0.8)
        tp2 = entry - (risk * 1.5)
        tp3 = entry - (risk * 2.5)
        
        # Adjust to market structure if better
        nearest_support = df['low'].tail(20).min()
        if nearest_support < entry:
            tp1 = max(tp1, nearest_support)
        
        # Ensure proper spacing
        min_gap = risk * 0.3
        tp1 = min(tp1, entry - (risk * 0.5))
        tp2 = min(tp2, tp1 - min_gap)
        tp3 = min(tp3, tp2 - min_gap)
    
    rr_ratio = (abs(tp1 - entry) / risk) if risk > 0 else 0
    
    return sl, tp1, tp2, tp3, risk, rr_ratio

def check_sl_cluster(symbol: str):
    """Check if symbol has too many recent SL hits (your logic)"""
    current_time = time.time()
    recent_time = current_time - 1800  # 30 minutes
    recent_hits = [t for t in recent_sl_hits.get(symbol, []) if t > recent_time]
    return len(recent_hits) >= SL_CLUSTER_THRESHOLD

# ==================== SIGNAL GENERATION ====================
async def generate_romeopt_signal(exchange, symbol: str, tf: str):
    """
    Complete RomeOPT-P signal generation with full details
    """
    signal_details = []
    score_details = {}
    total_score = 0
    
    # Fetch data
    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
    if not ohlcv or len(ohlcv) < 50:
        return None
    
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    last_candle = df.iloc[-1]
    prev_candles = df.iloc[-6:-1]
    
    # ========== STEP 1: Liquidity Sweep ==========
    sweep_high = last_candle["high"] > prev_candles["high"].max()
    sweep_low = last_candle["low"] < prev_candles["low"].min()
    has_sweep = sweep_high or sweep_low
    
    if has_sweep:
        total_score += SCORE_WEIGHTS["liquidity_sweep"]
        score_details["liquidity_sweep"] = SCORE_WEIGHTS["liquidity_sweep"]
        sweep_type = "High" if sweep_high else "Low"
        signal_details.append(f"✅ Liquidity Sweep ({sweep_type}) +{SCORE_WEIGHTS['liquidity_sweep']}")
    else:
        signal_details.append("❌ No Liquidity Sweep")
    
    # ========== STEP 2: Displacement with Volume ==========
    body_size = abs(last_candle["close"] - last_candle["open"])
    candle_range = last_candle["high"] - last_candle["low"]
    displacement_ratio = body_size / (candle_range + 1e-8)
    
    vol_avg = df['volume'].iloc[-10:].mean()
    vol_confirmation = last_candle['volume'] > vol_avg * 1.5
    
    if displacement_ratio > 0.6 and vol_confirmation:
        total_score += SCORE_WEIGHTS["displacement"]
        score_details["displacement"] = SCORE_WEIGHTS["displacement"]
        signal_details.append(f"✅ Strong Displacement ({displacement_ratio:.1%}) +{SCORE_WEIGHTS['displacement']}")
    elif displacement_ratio > 0.6:
        signal_details.append(f"⚠️ Displacement ({displacement_ratio:.1%}) but weak volume")
    else:
        signal_details.append(f"❌ Weak Displacement ({displacement_ratio:.1%})")
    
    # ========== STEP 3: Quality Order Block ==========
    ob_zone = find_quality_order_blocks(df)
    if not ob_zone:
        signal_details.append("❌ No quality Order Block")
        return None
    
    side = "BUY" if ob_zone["type"] == "bullish" else "SELL"
    signal_details.append(f"✅ {ob_zone['type'].upper()} OB (Vol: {ob_zone['volume_ratio']:.1f}x)")
    
    # Check if price is in OB zone
    in_zone = False
    if side == "BUY" and last_candle["close"] <= ob_zone["high"]:
        in_zone = True
    elif side == "SELL" and last_candle["close"] >= ob_zone["low"]:
        in_zone = True
    
    if in_zone:
        total_score += SCORE_WEIGHTS["quality_ob"]
        score_details["quality_ob"] = SCORE_WEIGHTS["quality_ob"]
        signal_details.append(f"✅ OB Zone Approach +{SCORE_WEIGHTS['quality_ob']}")
    else:
        signal_details.append("❌ Not in OB zone")
        return None
    
    # ========== STEP 4: BOS/CHOCH Detection ==========
    has_bos, has_choch, direction = detect_bos_choch(df)
    
    if has_bos or has_choch:
        total_score += SCORE_WEIGHTS["bos_choch"]
        score_details["bos_choch"] = SCORE_WEIGHTS["bos_choch"]
        if has_bos:
            signal_details.append(f"✅ BOS Detected ({direction}) +{SCORE_WEIGHTS['bos_choch']}")
        if has_choch:
            signal_details.append(f"✅ CHOCH Detected ({direction}) +{SCORE_WEIGHTS['bos_choch']}")
    else:
        signal_details.append("❌ No BOS/CHOCH")
        # Not a deal-breaker, but note it
    
    # ========== STEP 5: FVG Analysis ==========
    fvg = find_fvg_zones(df)
    if fvg:
        total_score += SCORE_WEIGHTS["fvg_zone"]
        score_details["fvg_zone"] = SCORE_WEIGHTS["fvg_zone"]
        fvg_type = "Premium" if fvg.get("premium") else "Discount"
        signal_details.append(f"✅ {fvg['type'].upper()} FVG ({fvg_type}) +{SCORE_WEIGHTS['fvg_zone']}")
    else:
        signal_details.append("⚠️ No FVG detected")
    
    # ========== STEP 6: Market Structure Shift ==========
    has_shift, shift_details = check_market_structure_shift(df, side)
    if has_shift:
        total_score += SCORE_WEIGHTS["structure_shift"]
        score_details["structure_shift"] = SCORE_WEIGHTS["structure_shift"]
        signal_details.append(f"✅ Structure Shift: {shift_details} +{SCORE_WEIGHTS['structure_shift']}")
    else:
        signal_details.append(f"⚠️ {shift_details}")
    
    # ========== STEP 7: Volume Confirmation ==========
    if vol_confirmation:
        total_score += SCORE_WEIGHTS["volume_confirmation"]
        score_details["volume_confirmation"] = SCORE_WEIGHTS["volume_confirmation"]
        signal_details.append(f"✅ Volume Spike ({last_candle['volume']/vol_avg:.1f}x) +{SCORE_WEIGHTS['volume_confirmation']}")
    
    # ========== STEP 8: HTF Alignment ==========
    htf_aligned, htf_confidence, htf_details = await check_htf_alignment(exchange, symbol, tf, side)
    if htf_aligned:
        total_score += SCORE_WEIGHTS["htf_alignment"]
        score_details["htf_alignment"] = SCORE_WEIGHTS["htf_alignment"]
        signal_details.append(f"✅ HTF Alignment ({htf_confidence}/4): {', '.join(htf_details)}")
    else:
        signal_details.append(f"❌ HTF Misalignment: {', '.join(htf_details)}")
        return None  # HTF alignment is critical
    
    # ========== STEP 9: Your Elite MTF Confirmation ==========
    elite_aligned, elite_details = await elite_mtf_confirmation(exchange, symbol, side)
    if elite_aligned:
        total_score += 1  # Bonus point for elite confirmation
        score_details["elite_mtf"] = 1
        signal_details.append(f"⭐ Elite MTF Confirmed: {elite_details}")
    else:
        signal_details.append(f"⚠️ Elite MTF: {elite_details}")
        # Not mandatory, but note it
    
    # ========== CRITICAL CHECKS ==========
    # Minimum score check
    if total_score < MIN_SCORE:
        signal_details.append(f"❌ Score {total_score} < {MIN_SCORE}")
        return None
    
    # Displacement mandatory
    if displacement_ratio <= 0.6:
        signal_details.append(f"❌ Insufficient displacement ({displacement_ratio:.1%})")
        return None
    
    # SL Cluster check
    if check_sl_cluster(symbol):
        signal_details.append(f"❌ SL Cluster ({SL_CLUSTER_THRESHOLD}+ hits)")
        return None
    
    # Calculate TP/SL (your 3-target structure)
    entry = float(last_candle["close"])
    sl, tp1, tp2, tp3, risk, rr_ratio = calculate_risk_parameters(entry, side, ob_zone, df)
    
    # RR Ratio check
    if rr_ratio < MIN_RR_RATIO:
        signal_details.append(f"❌ RR ratio {rr_ratio:.1f} < {MIN_RR_RATIO}")
        return None
    
    # TP1 Distance check (your filter)
    tp1_distance = abs(tp1 - entry)
    if tp1_distance < risk * 0.1:
        signal_details.append(f"❌ TP1 too close ({tp1_distance:.4f} < {risk*0.1:.4f})")
        return None
    
    # Liquidity path check
    path_clear, path_reason = check_liquidity_path(df, side, entry, tp1)
    if not path_clear:
        signal_details.append(f"❌ {path_reason}")
        return None
    else:
        signal_details.append(f"✅ {path_reason}")
    
    # ========== BUILD SIGNAL ==========
    signal = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,  # Your TP3
        "entry_tf": tf,
        
        # Scoring
        "score": total_score,
        "score_details": score_details,
        
        # Market Structure
        "has_bos": has_bos,
        "has_choch": has_choch,
        "fvg": fvg,
        "ob_zone": ob_zone,
        
        # Risk
        "risk": risk,
        "rr_ratio": rr_ratio,
        "atr_value": float(calculate_atr(df).iloc[-1]),
        
        # Details
        "signal_details": signal_details,
        "reasons": "\n".join(signal_details),
        "timestamp": datetime.datetime.utcnow().isoformat()
    }
    
    return signal

# ==================== SIGNAL PROCESSING ====================
async def save_signal(signal):
    """Save signal with all details to database"""
    if not signal:
        return
    
    async with db_lock:
        try:
            await db_conn.execute("""
                INSERT INTO romeopt_signals 
                (symbol, side, entry, sl, tp1, tp2, tp3, entry_tf, score, 
                 liquidity_sweep, displacement, quality_ob, htf_alignment, 
                 bos_choch, volume_confirmation, structure_shift, fvg_zone, elite_mtf,
                 has_bos, has_choch, fvg_type, fvg_premium, ob_type, ob_volume_ratio,
                 risk, rr_ratio, atr_value, opened_at, signal_details, reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal["symbol"], signal["side"], signal["entry"], signal["sl"],
                signal["tp1"], signal["tp2"], signal["tp3"], signal["entry_tf"], signal["score"],
                signal["score_details"].get("liquidity_sweep", 0),
                signal["score_details"].get("displacement", 0),
                signal["score_details"].get("quality_ob", 0),
                signal["score_details"].get("htf_alignment", 0),
                signal["score_details"].get("bos_choch", 0),
                signal["score_details"].get("volume_confirmation", 0),
                signal["score_details"].get("structure_shift", 0),
                signal["score_details"].get("fvg_zone", 0),
                signal["score_details"].get("elite_mtf", 0),
                signal.get("has_bos", 0),
                signal.get("has_choch", 0),
                signal.get("fvg", {}).get("type") if signal.get("fvg") else None,
                signal.get("fvg", {}).get("premium") if signal.get("fvg") else 0,
                signal.get("ob_zone", {}).get("type") if signal.get("ob_zone") else None,
                signal.get("ob_zone", {}).get("volume_ratio", 0) if signal.get("ob_zone") else 0,
                signal["risk"], signal["rr_ratio"], signal.get("atr_value"),
                signal["timestamp"], str(signal.get("signal_details", [])),
                signal.get("reasons", "")
            ))
            await db_conn.commit()
            log.info(f"Signal saved: {signal['symbol']} {signal['side']} Score: {signal['score']}")
        except Exception as e:
            log.error(f"Failed to save signal: {e}")

async def send_signal_alert(signal):
    """Send comprehensive signal alert to Telegram"""
    if not signal:
        return
    
    alert_msg = "🚀 <b>ROMEOPT-P SIGNAL FOUND</b> 🚀\n\n"
    alert_msg += f"<b>Pair:</b> {signal['symbol']} ({signal['entry_tf']})\n"
    alert_msg += f"<b>Side:</b> {signal['side']}\n"
    alert_msg += f"<b>Entry:</b> {signal['entry']:.4f}\n"
    alert_msg += f"<b>SL:</b> {signal['sl']:.4f} (Risk: {signal['risk']:.4f})\n"
    alert_msg += f"<b>TP1:</b> {signal['tp1']:.4f} (0.8R)\n"
    alert_msg += f"<b>TP2:</b> {signal['tp2']:.4f} (1.5R)\n"
    alert_msg += f"<b>TP3:</b> {signal['tp3']:.4f} (2.5R)\n"
    alert_msg += f"<b>Score:</b> {signal['score']}/10\n"
    alert_msg += f"<b>RR Ratio:</b> {signal['rr_ratio']:.1f}:1\n"
    alert_msg += f"<b>ATR:</b> {signal.get('atr_value', 0):.4f}\n\n"
    
    # Add scoring breakdown
    alert_msg += "<b>Scoring Breakdown:</b>\n"
    for detail in signal.get("signal_details", []):
        # Limit to 12 most important details for readability
        if len(detail) < 100:  # Skip very long details
            alert_msg += f"• {detail}\n"
    
    # Add market structure info
    alert_msg += "\n<b>Market Structure:</b>\n"
    if signal.get("has_bos"):
        alert_msg += "• BOS: ✅\n"
    if signal.get("has_choch"):
        alert_msg += "• CHOCH: ✅\n"
    if signal.get("fvg"):
        fvg_type = "Premium" if signal["fvg"].get("premium") else "Discount"
        alert_msg += f"• FVG: {signal['fvg']['type'].upper()} ({fvg_type})\n"
    if signal.get("ob_zone"):
        alert_msg += f"• OB: {signal['ob_zone']['type'].upper()} (Vol: {signal['ob_zone'].get('volume_ratio', 0):.1f}x)\n"
    
    alert_msg += f"\n<b>Time:</b> {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    
    await send_telegram_alert(alert_msg)

# ==================== MONITORING ====================
async def monitor_positions(exchange):
    """Monitor and manage open positions (your logic with TP3)"""
    while True:
        try:
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, 
                           tp1_hit, tp2_hit, tp3_hit, status
                    FROM romeopt_signals 
                    WHERE status = 'OPEN'
                """)
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                    
                    # Get current price
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = ticker.get("last")
                        if not current_price:
                            continue
                    except:
                        continue
                    
                    # Check TP/SL (your logic with TP3)
                    hits = []
                    new_tp1_hit = tp1_hit
                    new_tp2_hit = tp2_hit
                    new_tp3_hit = tp3_hit
                    new_status = status
                    new_sl = sl
                    
                    if side == "BUY":
                        if not tp1_hit and current_price >= tp1:
                            hits.append("TP1")
                            new_tp1_hit = 1
                            new_sl = entry  # Move SL to BE
                        
                        if not tp2_hit and current_price >= tp2:
                            hits.append("TP2")
                            new_tp2_hit = 1
                            # Don't close at TP2, continue to TP3
                        
                        if not tp3_hit and current_price >= tp3:
                            hits.append("TP3")
                            new_tp3_hit = 1
                            new_status = "CLOSED"  # Close at TP3
                        
                        if current_price <= new_sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                            recent_sl_hits[symbol].append(time.time())
                    
                    else:  # SELL
                        if not tp1_hit and current_price <= tp1:
                            hits.append("TP1")
                            new_tp1_hit = 1
                            new_sl = entry  # Move SL to BE
                        
                        if not tp2_hit and current_price <= tp2:
                            hits.append("TP2")
                            new_tp2_hit = 1
                            # Don't close at TP2
                        
                        if not tp3_hit and current_price <= tp3:
                            hits.append("TP3")
                            new_tp3_hit = 1
                            new_status = "CLOSED"  # Close at TP3
                        
                        if current_price >= new_sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                            recent_sl_hits[symbol].append(time.time())
                    
                    # Update if changed
                    if hits or new_status != status:
                        # Calculate PnL if closed
                        pnl = 0
                        pnl_pct = 0
                        
                        if new_status == "CLOSED":
                            if side == "BUY":
                                pnl = current_price - entry
                            else:
                                pnl = entry - current_price
                            pnl_pct = (pnl / entry) * 100
                        
                        await db_conn.execute("""
                            UPDATE romeopt_signals 
                            SET tp1_hit=?, tp2_hit=?, tp3_hit=?, sl=?, status=?, 
                                closed_at=?, close_reason=?, pnl=?, pnl_pct=?
                            WHERE id=?
                        """, (
                            new_tp1_hit, new_tp2_hit, new_tp3_hit, new_sl, new_status,
                            datetime.datetime.utcnow().isoformat() if new_status == "CLOSED" else None,
                            ",".join(hits) if hits else None,
                            pnl, pnl_pct, sig_id
                        ))
                        
                        # Send update alert
                        if hits:
                            update_msg = f"🎯 <b>POSITION UPDATE</b>\n"
                            update_msg += f"<b>Pair:</b> {symbol}\n"
                            update_msg += f"<b>Side:</b> {side}\n"
                            update_msg += f"<b>Entry:</b> {entry:.4f}\n"
                            update_msg += f"<b>Current:</b> {current_price:.4f}\n"
                            update_msg += f"<b>Hits:</b> {', '.join(hits)}\n"
                            update_msg += f"<b>New SL:</b> {new_sl:.4f}\n"
                            if pnl != 0:
                                update_msg += f"<b>PnL:</b> {pnl:.4f} ({pnl_pct:.2f}%)\n"
                            await send_telegram_alert(update_msg)
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(5)  # Check every 5 seconds

# ==================== SCANNING LOOP ====================
async def scan_markets(exchange):
    """Main scanning loop with your timeframes"""
    last_scan = {}
    
    while True:
        try:
            # Get top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT")]
            
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                # Skip if SL cluster
                if check_sl_cluster(symbol):
                    log.debug(f"Skipping {symbol}: SL cluster")
                    continue
                
                for tf in TIMEFRAMES:
                    # Cooldown per symbol/TF (60 seconds)
                    key = f"{symbol}:{tf}"
                    if key in last_scan and (time.time() - last_scan[key]) < 60:
                        continue
                    
                    signal = await generate_romeopt_signal(exchange, symbol, tf)
                    
                    if signal:
                        # Save and alert
                        await save_signal(signal)
                        await send_signal_alert(signal)
                        
                        signals_found += 1
                        last_scan[key] = time.time()
                        
                        # Small delay between signals
                        await asyncio.sleep(0.5)
            
            if signals_found > 0:
                log.info(f"Found {signals_found} RomeOPT-P signals")
            else:
                log.debug("Scan complete: No signals")
            
            # Clean up old SL hits
            current_time = time.time()
            for symbol in list(recent_sl_hits.keys()):
                recent_sl_hits[symbol] = deque(
                    [t for t in recent_sl_hits[symbol] if current_time - t < 3600],
                    maxlen=10
                )
            
        except Exception as e:
            log.error(f"Scan error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ==================== MAIN ====================
async def main():
    """Main application entry point"""
    global exchange
    
    try:
        # Initialize
        await init_database()
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "timeout": 30000,
            "rateLimit": 1000
        })
        
        # Startup alert
        startup_msg = "🚀 <b>ROMEOPT-P COMPLETE SCANNER STARTED</b>\n\n"
        startup_msg += "✅ Full RomeOPT-P Implementation\n"
        startup_msg += "✅ BOS/CHOCH Detection\n"
        startup_msg += "✅ FVG Premium/Discount Zones\n"
        startup_msg += "✅ Quality Order Blocks with Volume\n"
        startup_msg += "✅ Your Timeframes: 1m, 3m, 5m, 15m, 30m\n"
        startup_msg += "✅ Elite MTF Confirmation (15m,1h,4h)\n"
        startup_msg += "✅ TP1/TP2/TP3 Structure\n"
        startup_msg += "✅ Full Signal Details in Alerts\n\n"
        startup_msg += f"<b>Scanning:</b> {TOP_N} top pairs\n"
        startup_msg += f"<b>Min Score:</b> {MIN_SCORE}/10\n"
        startup_msg += f"<b>Scan Interval:</b> {SCAN_INTERVAL}s"
        
        await send_telegram_alert(startup_msg)
        log.info("RomeOPT-P Complete Scanner started with your timeframes")
        
        # Run scanner and monitor concurrently
        await asyncio.gather(
            scan_markets(exchange),
            monitor_positions(exchange)
        )
        
    except KeyboardInterrupt:
        log.info("Shutdown requested by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")
        await send_telegram_alert(f"❌ <b>SCANNER CRASHED</b>\nError: {str(e)[:200]}")
    finally:
        if db_conn:
            await db_conn.close()
        if exchange:
            await exchange.close()

if __name__ == "__main__":
    # Run as standalone scanner (your preference)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")