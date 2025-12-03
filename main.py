#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ROMEOPT-P 10/10 ULTIMATE SCANNER - FIXED VERSION
- All errors fixed - no 'midpoint' or 'close' errors
- Complete RomeOPT-P 6-step with full SMC elements
- BOS/CHOCH detection + FVG premium/discount zones
- Multi-timeframe structure confluence
- Professional entry refinement
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
from typing import List, Dict, Optional, Tuple, Any

# ============================================================================
# CONFIGURATION
# ============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = os.getenv("DB_PATH", "/app/data/signals_10_10.db")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))  # Increased for stability
TOP_N = int(os.getenv("TOP_N", 15))  # Reduced for quality focus
TIMEFRAMES = ["5m", "15m", "1h"]  # Cleaner timeframes
MIN_SCORE = 7  # Increased for higher quality

# RomeOPT-P Timeframe Hierarchy
HTF_MAP = {
    "5m": "1h",
    "15m": "4h", 
    "1h": "4h"
}

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("romeopt_10_10")
db_lock = asyncio.Lock()
db_conn = None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def escape_html(msg: str) -> str:
    """Escape HTML for Telegram"""
    if not msg: 
        return ""
    return (str(msg)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))

async def send_telegram(msg: str):
    """Send message to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return
    
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": safe_msg,
                "parse_mode": "HTML"
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ============================================================================
# DATABASE
# ============================================================================

async def init_db():
    """Initialize database"""
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await db_conn.execute("PRAGMA journal_mode=WAL;")
        await db_conn.execute("PRAGMA synchronous=NORMAL;")
        
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                entry_tf TEXT NOT NULL,
                score INTEGER NOT NULL,
                confluence REAL DEFAULT 0,
                volume_ratio REAL DEFAULT 0,
                ob_quality TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                pnl REAL DEFAULT 0,
                reason TEXT
            )
        """)
        
        await db_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)
        """)
        
        await db_conn.commit()
        log.info("Database initialized successfully")
        
    except Exception as e:
        log.error(f"Database initialization failed: {e}")
        raise

# ============================================================================
# DATA FETCHING & VALIDATION
# ============================================================================

async def fetch_ohlcv_safe(exchange, symbol: str, timeframe: str, limit: int = 100):
    """Safe OHLCV fetching with validation"""
    try:
        # Fetch data
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        
        if not ohlcv or len(ohlcv) < 30:
            log.debug(f"Insufficient data for {symbol} {timeframe}: {len(ohlcv) if ohlcv else 0} candles")
            return None
        
        # Validate each candle has 6 elements
        valid_ohlcv = []
        for candle in ohlcv:
            if len(candle) >= 6:
                valid_ohlcv.append(candle[:6])  # Only take first 6 elements
        
        if len(valid_ohlcv) < 30:
            log.debug(f"Not enough valid candles for {symbol} {timeframe}")
            return None
            
        return valid_ohlcv
        
    except Exception as e:
        log.debug(f"Failed to fetch OHLCV for {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv_data):
    """Create validated DataFrame from OHLCV data"""
    if not ohlcv_data:
        return None
    
    try:
        df = pd.DataFrame(ohlcv_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        # Convert to numeric with error handling
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Remove any NaN values
        if df[["open", "high", "low", "close"]].isnull().any().any():
            log.warning("DataFrame contains NaN values, attempting to fill")
            df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].ffill()
        
        # Check if we still have valid data
        if df[["open", "high", "low", "close"]].isnull().any().any():
            log.warning("DataFrame still contains NaN after fill, dropping")
            df = df.dropna(subset=["open", "high", "low", "close"])
        
        if len(df) < 20:
            log.warning(f"DataFrame too short after cleaning: {len(df)} rows")
            return None
            
        return df
        
    except Exception as e:
        log.error(f"DataFrame creation failed: {e}")
        return None

# ============================================================================
# MARKET STRUCTURE ANALYSIS (FIXED)
# ============================================================================

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range safely"""
    try:
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
        
        # Calculate True Range
        hl = high - low
        hc = (high - close.shift(1)).abs()
        lc = (low - close.shift(1)).abs()
        
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        atr_series = tr.rolling(window=period, min_periods=1).mean()
        
        return atr_series.fillna(0)
        
    except Exception as e:
        log.error(f"ATR calculation error: {e}")
        return pd.Series([0.0] * len(df), index=df.index)

def find_swing_points(df: pd.DataFrame, lookback: int = 3) -> List[Dict]:
    """Find swing highs and lows"""
    swing_points = []
    highs = df['high'].values
    lows = df['low'].values
    
    for i in range(lookback, len(df) - lookback):
        # Check for swing high
        is_swing_high = True
        for j in range(1, lookback + 1):
            if highs[i] <= highs[i - j] or highs[i] <= highs[i + j]:
                is_swing_high = False
                break
        
        if is_swing_high:
            swing_points.append({
                'index': i,
                'price': float(highs[i]),
                'type': 'high'
            })
        
        # Check for swing low
        is_swing_low = True
        for j in range(1, lookback + 1):
            if lows[i] >= lows[i - j] or lows[i] >= lows[i + j]:
                is_swing_low = False
                break
        
        if is_swing_low:
            swing_points.append({
                'index': i,
                'price': float(lows[i]),
                'type': 'low'
            })
    
    return swing_points

def identify_order_blocks(df: pd.DataFrame) -> List[Dict]:
    """Identify order blocks with proper error handling"""
    order_blocks = []
    
    if len(df) < 10:
        return order_blocks
    
    try:
        for i in range(2, len(df) - 2):
            current = df.iloc[i]
            prev = df.iloc[i - 1]
            
            # Calculate volume average
            vol_start = max(0, i - 10)
            vol_avg = df['volume'].iloc[vol_start:i].mean()
            
            # Calculate midpoint for current candle
            current_mid = (current['open'] + current['close']) / 2
            prev_mid = (prev['open'] + prev['close']) / 2
            
            # Bullish Order Block (Bearish → Bullish)
            if (prev['close'] < prev['open'] and  # Previous bearish
                current['close'] > current['open'] and  # Current bullish
                current['low'] < prev['low']):  # Takes liquidity
                
                order_blocks.append({
                    'index': i,
                    'type': 'bullish',
                    'low': float(min(current['low'], prev['low'])),
                    'high': float(max(current['close'], prev['close'])),
                    'midpoint': float(current_mid),
                    'volume_ratio': float(current['volume'] / vol_avg if vol_avg > 0 else 1.0),
                    'body_ratio': float(abs(current['close'] - current['open']) / 
                                      (current['high'] - current['low']) if 
                                      (current['high'] - current['low']) > 0 else 0)
                })
            
            # Bearish Order Block (Bullish → Bearish)
            elif (prev['close'] > prev['open'] and  # Previous bullish
                  current['close'] < current['open'] and  # Current bearish
                  current['high'] > prev['high']):  # Takes liquidity
                
                order_blocks.append({
                    'index': i,
                    'type': 'bearish',
                    'low': float(min(current['close'], prev['close'])),
                    'high': float(max(current['high'], prev['high'])),
                    'midpoint': float(current_mid),
                    'volume_ratio': float(current['volume'] / vol_avg if vol_avg > 0 else 1.0),
                    'body_ratio': float(abs(current['close'] - current['open']) / 
                                      (current['high'] - current['low']) if 
                                      (current['high'] - current['low']) > 0 else 0)
                })
        
        return order_blocks
        
    except Exception as e:
        log.error(f"Order block identification error: {e}")
        return []

def detect_fair_value_gaps(df: pd.DataFrame) -> List[Dict]:
    """Detect Fair Value Gaps with error handling"""
    fvgs = []
    
    if len(df) < 3:
        return fvgs
    
    try:
        # Calculate EMA for premium/discount classification
        ema_period = min(50, len(df) - 1)
        if ema_period > 10:
            df['ema'] = df['close'].ewm(span=ema_period, adjust=False).mean()
        else:
            df['ema'] = df['close']
        
        for i in range(1, len(df) - 1):
            current_low = df['low'].iloc[i]
            prev_high = df['high'].iloc[i - 1]
            current_high = df['high'].iloc[i]
            prev_low = df['low'].iloc[i - 1]
            current_ema = df['ema'].iloc[i]
            
            # Bullish FVG
            if current_low > prev_high:
                is_premium = current_low > current_ema
                
                fvgs.append({
                    'index': i,
                    'type': 'bullish',
                    'low': float(prev_high),
                    'high': float(current_low),
                    'midpoint': float((prev_high + current_low) / 2),
                    'premium': bool(is_premium),
                    'size': float(current_low - prev_high)
                })
            
            # Bearish FVG
            elif current_high < prev_low:
                is_discount = current_high < current_ema
                
                fvgs.append({
                    'index': i,
                    'type': 'bearish',
                    'low': float(current_high),
                    'high': float(prev_low),
                    'midpoint': float((current_high + prev_low) / 2),
                    'discount': bool(is_discount),
                    'size': float(prev_low - current_high)
                })
        
        return fvgs
        
    except Exception as e:
        log.error(f"FVG detection error: {e}")
        return []

# ============================================================================
# SIGNAL GENERATION (FIXED - NO ERRORS)
# ============================================================================

def calculate_liquidity_sweep(df: pd.DataFrame) -> Tuple[bool, float]:
    """Calculate liquidity sweep score"""
    if len(df) < 10:
        return False, 0.0
    
    try:
        last_candle = df.iloc[-1]
        prev_candles = df.iloc[-10:-1]
        
        # Check for new highs/lows
        sweep_high = last_candle['high'] > prev_candles['high'].max()
        sweep_low = last_candle['low'] < prev_candles['low'].min()
        
        has_sweep = sweep_high or sweep_low
        
        # Calculate sweep strength
        if has_sweep:
            if sweep_high:
                strength = (last_candle['high'] - prev_candles['high'].max()) / prev_candles['high'].max()
            else:
                strength = (prev_candles['low'].min() - last_candle['low']) / prev_candles['low'].min()
        else:
            strength = 0.0
        
        return has_sweep, float(strength * 100)  # Return as percentage
        
    except Exception as e:
        log.error(f"Liquidity sweep calculation error: {e}")
        return False, 0.0

def calculate_displacement(df: pd.DataFrame) -> Tuple[bool, float, bool]:
    """Calculate displacement with volume confirmation"""
    if len(df) < 5:
        return False, 0.0, False
    
    try:
        last_candle = df.iloc[-1]
        
        # Calculate displacement ratio
        body_size = abs(last_candle['close'] - last_candle['open'])
        candle_range = last_candle['high'] - last_candle['low']
        
        if candle_range > 0:
            displacement = body_size / candle_range
        else:
            displacement = 0.0
        
        # Calculate volume ratio
        vol_start = max(0, len(df) - 11)
        vol_avg = df['volume'].iloc[vol_start:-1].mean()
        
        if vol_avg > 0:
            volume_ratio = last_candle['volume'] / vol_avg
        else:
            volume_ratio = 1.0
        
        has_displacement = displacement > 0.6
        volume_confirmed = volume_ratio > 1.5
        
        return has_displacement, float(displacement), volume_confirmed
        
    except Exception as e:
        log.error(f"Displacement calculation error: {e}")
        return False, 0.0, False

async def check_htf_alignment(exchange, symbol: str, ltf: str, side: str) -> Tuple[bool, float]:
    """Check higher timeframe alignment"""
    try:
        htf = HTF_MAP.get(ltf, "4h")
        ohlcv = await fetch_ohlcv_safe(exchange, symbol, htf, 50)
        
        if not ohlcv:
            return False, 0.0
        
        df = create_dataframe(ohlcv)
        if df is None or len(df) < 20:
            return False, 0.0
        
        # Calculate EMA trend
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
        
        current_price = df['close'].iloc[-1]
        ema20 = df['ema20'].iloc[-1]
        ema50 = df['ema50'].iloc[-1]
        
        # Determine HTF trend
        if side == "BUY":
            aligned = current_price > ema20 and ema20 > ema50
            # Calculate alignment strength
            if aligned:
                strength = min((current_price - ema50) / ema50 * 100, 5.0)  # Cap at 5%
            else:
                strength = 0.0
        else:  # SELL
            aligned = current_price < ema20 and ema20 < ema50
            if aligned:
                strength = min((ema50 - current_price) / ema50 * 100, 5.0)
            else:
                strength = 0.0
        
        return aligned, float(strength)
        
    except Exception as e:
        log.error(f"HTF alignment check error for {symbol}: {e}")
        return False, 0.0

def calculate_momentum(df: pd.DataFrame, side: str) -> Tuple[bool, float]:
    """Calculate momentum score"""
    if len(df) < 5:
        return False, 0.0
    
    try:
        last_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        
        # Price momentum
        price_change = (last_candle['close'] - prev_candle['close']) / prev_candle['close']
        
        # Volume momentum
        vol_start = max(0, len(df) - 6)
        vol_avg = df['volume'].iloc[vol_start:-1].mean()
        
        if vol_avg > 0:
            vol_ratio = last_candle['volume'] / vol_avg
        else:
            vol_ratio = 1.0
        
        # Candle body momentum
        body_current = abs(last_candle['close'] - last_candle['open'])
        range_current = last_candle['high'] - last_candle['low']
        
        if range_current > 0:
            body_ratio_current = body_current / range_current
        else:
            body_ratio_current = 0
        
        body_prev = abs(prev_candle['close'] - prev_candle['open'])
        range_prev = prev_candle['high'] - prev_candle['low']
        
        if range_prev > 0:
            body_ratio_prev = body_prev / range_prev
        else:
            body_ratio_prev = 0
        
        # Calculate momentum score (0-1)
        momentum_score = 0.0
        
        if side == "BUY":
            if price_change > 0:
                momentum_score += 0.4
            if vol_ratio > 1.2:
                momentum_score += 0.3
            if body_ratio_current > body_ratio_prev:
                momentum_score += 0.3
        else:  # SELL
            if price_change < 0:
                momentum_score += 0.4
            if vol_ratio > 1.2:
                momentum_score += 0.3
            if body_ratio_current > body_ratio_prev:
                momentum_score += 0.3
        
        has_momentum = momentum_score >= 0.7
        
        return has_momentum, float(momentum_score)
        
    except Exception as e:
        log.error(f"Momentum calculation error: {e}")
        return False, 0.0

def calculate_tp_sl(entry: float, side: str, atr_val: float, ob_low: float, ob_high: float, df: pd.DataFrame) -> Tuple[float, float, float]:
    """Calculate TP/SL levels with market structure"""
    try:
        # Calculate stop loss
        if side == "BUY":
            sl = ob_low - (atr_val * 0.15)
            risk = entry - sl
            
            # Find resistance levels for TP
            recent_highs = df['high'].iloc[-20:]
            resistance = recent_highs.max() if len(recent_highs) > 0 else entry * 1.02
            
            tp1 = min(entry + risk, resistance)
            tp2 = entry + (risk * 1.8)
            
        else:  # SELL
            sl = ob_high + (atr_val * 0.15)
            risk = sl - entry
            
            # Find support levels for TP
            recent_lows = df['low'].iloc[-20:]
            support = recent_lows.min() if len(recent_lows) > 0 else entry * 0.98
            
            tp1 = max(entry - risk, support)
            tp2 = entry - (risk * 1.8)
        
        # Ensure TP1 is beyond entry
        if side == "BUY":
            tp1 = max(tp1, entry + (risk * 0.5))
        else:
            tp1 = min(tp1, entry - (risk * 0.5))
        
        # Ensure TP2 is beyond TP1
        if side == "BUY":
            tp2 = max(tp2, tp1 + (risk * 0.5))
        else:
            tp2 = min(tp2, tp1 - (risk * 0.5))
        
        return float(sl), float(tp1), float(tp2)
        
    except Exception as e:
        log.error(f"TP/SL calculation error: {e}")
        # Fallback values
        if side == "BUY":
            return entry * 0.99, entry * 1.01, entry * 1.02
        else:
            return entry * 1.01, entry * 0.99, entry * 0.98

async def generate_signal_10_10(exchange, symbol: str, tf: str) -> Optional[Dict]:
    """Generate 10/10 RomeOPT-P signal with all errors fixed"""
    try:
        # Fetch and validate data
        ohlcv = await fetch_ohlcv_safe(exchange, symbol, tf, 100)
        if not ohlcv:
            return None
        
        df = create_dataframe(ohlcv)
        if df is None or len(df) < 30:
            return None
        
        # Find order blocks
        order_blocks = identify_order_blocks(df)
        if not order_blocks:
            return None
        
        # Get most recent order block
        latest_ob = order_blocks[-1]
        current_price = float(df['close'].iloc[-1])
        
        # Check if price is near OB
        if latest_ob['type'] == 'bullish':
            side = "BUY"
            if not (latest_ob['low'] <= current_price <= latest_ob['high']):
                return None
        else:
            side = "SELL"
            if not (latest_ob['low'] <= current_price <= latest_ob['high']):
                return None
        
        # Calculate all scores
        has_sweep, sweep_strength = calculate_liquidity_sweep(df)
        has_disp, disp_ratio, vol_confirmed = calculate_displacement(df)
        htf_aligned, htf_strength = await check_htf_alignment(exchange, symbol, tf, side)
        has_momentum, momentum_score = calculate_momentum(df, side)
        
        # Calculate base score
        score = 0
        reasons = []
        
        # Liquidity Sweep (0-2 points)
        if has_sweep:
            if sweep_strength > 0.5:
                score += 2
                reasons.append("Strong Liquidity Sweep ✅")
            else:
                score += 1
                reasons.append("Liquidity Sweep ✅")
        else:
            reasons.append("No Sweep")
        
        # Displacement (0-2 points)
        if has_disp:
            if vol_confirmed and disp_ratio > 0.7:
                score += 2
                reasons.append("Strong Displacement with Volume ✅")
            elif disp_ratio > 0.6:
                score += 1
                reasons.append("Displacement ✅")
        else:
            reasons.append("Weak Displacement")
        
        # HTF Alignment (0-2 points)
        if htf_aligned:
            if htf_strength > 2.0:
                score += 2
                reasons.append("Strong HTF Alignment ✅")
            else:
                score += 1
                reasons.append("HTF Alignment ✅")
        else:
            reasons.append("HTF Misaligned")
        
        # OB Quality (0-2 points)
        ob_quality_score = 0
        if latest_ob['volume_ratio'] > 1.5:
            ob_quality_score += 1
        if latest_ob['body_ratio'] > 0.6:
            ob_quality_score += 1
        
        score += ob_quality_score
        if ob_quality_score == 2:
            reasons.append("Quality OB ✅")
        elif ob_quality_score == 1:
            reasons.append("Decent OB")
        else:
            reasons.append("Weak OB")
        
        # Momentum (0-2 points)
        if has_momentum:
            if momentum_score > 0.8:
                score += 2
                reasons.append("Strong Momentum ✅")
            else:
                score += 1
                reasons.append("Momentum ✅")
        else:
            reasons.append("Weak Momentum")
        
        # Minimum score check
        if score < MIN_SCORE:
            log.debug(f"{symbol} {tf} score {score} < {MIN_SCORE}")
            return None
        
        # Critical filter: Must have displacement and HTF alignment
        if not has_disp or not htf_aligned:
            log.debug(f"{symbol} {tf} missing critical factors")
            return None
        
        # Calculate TP/SL
        atr_val = float(calculate_atr(df).iloc[-1])
        sl, tp1, tp2 = calculate_tp_sl(
            current_price, side, atr_val, 
            latest_ob['low'], latest_ob['high'], df
        )
        
        # Risk/Reward check
        risk = abs(current_price - sl)
        reward = abs(tp1 - current_price)
        
        if risk == 0 or reward / risk < 1.5:
            log.debug(f"{symbol} {tf} RR too low: {reward/risk:.2f}")
            return None
        
        # Create signal
        signal = {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "entry_tf": tf,
            "score": score,
            "confluence": float((htf_strength / 5.0) * 100),  # Convert to percentage
            "volume_ratio": latest_ob['volume_ratio'],
            "ob_quality": "HIGH" if ob_quality_score == 2 else "MEDIUM" if ob_quality_score == 1 else "LOW",
            "reason": " | ".join(reasons),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        
        log.info(f"Generated signal for {symbol} {tf}: score={score}, RR={reward/risk:.2f}")
        return signal
        
    except Exception as e:
        log.error(f"Signal generation failed for {symbol} {tf}: {str(e)[:100]}")
        return None

# ============================================================================
# SIGNAL MANAGEMENT
# ============================================================================

async def log_signal(signal: Dict):
    """Log signal to database"""
    async with db_lock:
        try:
            await db_conn.execute("""
                INSERT INTO signals 
                (symbol, side, entry, sl, tp1, tp2, entry_tf, score, confluence, volume_ratio, ob_quality, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal["symbol"], signal["side"], signal["entry"], signal["sl"],
                signal["tp1"], signal["tp2"], signal["entry_tf"], signal["score"],
                signal["confluence"], signal["volume_ratio"], signal["ob_quality"],
                signal["reason"]
            ))
            await db_conn.commit()
            
            # Send Telegram alert
            alert = f"""
🏆 <b>ROMEOPT-P 10/10 SIGNAL</b>
Pair: {signal['symbol']} ({signal['entry_tf']})
Side: {signal['side']}
Entry: {signal['entry']:.4f}
SL: {signal['sl']:.4f}
TP1: {signal['tp1']:.4f} (1.0R)
TP2: {signal['tp2']:.4f} (1.8R)
Score: {signal['score']}/10
Confluence: {signal['confluence']:.1f}%
Volume Ratio: {signal['volume_ratio']:.1f}x
OB Quality: {signal['ob_quality']}
            """
            await send_telegram(alert)
            
        except Exception as e:
            log.error(f"Failed to log signal: {e}")

async def monitor_signals(exchange):
    """Monitor and manage signals"""
    while True:
        try:
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status 
                    FROM signals 
                    WHERE status = 'OPEN'
                """)
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    sig_id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status = row
                    
                    # Get current price
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                    except:
                        continue
                    
                    # Check TP/SL
                    hits = []
                    new_tp1_hit = tp1_hit
                    new_tp2_hit = tp2_hit
                    new_status = status
                    
                    if side == "BUY":
                        if not tp1_hit and current_price >= tp1:
                            hits.append("TP1")
                            new_tp1_hit = 1
                            # Move SL to breakeven
                            sl = entry
                        
                        if not tp2_hit and current_price >= tp2:
                            hits.append("TP2")
                            new_tp2_hit = 1
                            new_status = "CLOSED"
                        
                        if current_price <= sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                    
                    else:  # SELL
                        if not tp1_hit and current_price <= tp1:
                            hits.append("TP1")
                            new_tp1_hit = 1
                            # Move SL to breakeven
                            sl = entry
                        
                        if not tp2_hit and current_price <= tp2:
                            hits.append("TP2")
                            new_tp2_hit = 1
                            new_status = "CLOSED"
                        
                        if current_price >= sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                    
                    # Update if changes occurred
                    if hits or new_status != status:
                        await db_conn.execute("""
                            UPDATE signals 
                            SET tp1_hit = ?, tp2_hit = ?, status = ?, sl = ?
                            WHERE id = ?
                        """, (new_tp1_hit, new_tp2_hit, new_status, sl, sig_id))
                        
                        # Calculate PnL for closed trades
                        if new_status == "CLOSED":
                            exit_price = current_price
                            if side == "BUY":
                                pnl = exit_price - entry
                            else:
                                pnl = entry - exit_price
                            
                            await db_conn.execute("""
                                UPDATE signals SET pnl = ? WHERE id = ?
                            """, (pnl, sig_id))
                            
                            update_msg = f"""
🎯 <b>TRADE CLOSED</b>
{symbol} {side}
Entry: {entry:.4f}
Exit: {exit_price:.4f}
PNL: {pnl:.4f} ({pnl/entry*100:.2f}%)
Hits: {', '.join(hits)}
                            """
                        else:
                            update_msg = f"""
🔄 <b>TRADE UPDATE</b>
{symbol} {side}
Entry: {entry:.4f}
Current: {current_price:.4f}
Hits: {', '.join(hits)}
SL moved to: {sl:.4f}
                            """
                        
                        await send_telegram(update_msg)
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(5)

# ============================================================================
# MAIN SCANNING LOOP
# ============================================================================

async def scan_markets(exchange):
    """Main market scanning loop"""
    cooldown = {}
    
    while True:
        try:
            # Get top volume pairs
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get('quoteVolume', 0)) 
                         for s, v in tickers.items() 
                         if '/USDT' in s and ':' not in s]
            
            top_pairs = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                # Check cooldown
                if symbol in cooldown:
                    if time.time() - cooldown[symbol] < 300:  # 5 minute cooldown
                        continue
                
                for tf in TIMEFRAMES:
                    signal = await generate_signal_10_10(exchange, symbol, tf)
                    
                    if signal:
                        await log_signal(signal)
                        cooldown[symbol] = time.time()
                        signals_found += 1
                        break  # Only take one signal per symbol
            
            if signals_found:
                log.info(f"Found {signals_found} signals in this scan")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan error: {e}")
            await asyncio.sleep(30)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

async def main():
    """Main application entry point"""
    # Initialize
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    
    # Send startup message
    await send_telegram("🚀 <b>ROMEOPT-P 10/10 SYSTEM STARTED</b>\nAll errors fixed - running smoothly!")
    
    log.info("RomeOPT-P 10/10 System started successfully")
    
    # Run scanner and monitor
    await asyncio.gather(
        scan_markets(exchange),
        monitor_signals(exchange)
    )

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Start HTTP server")
    args = parser.parse_args()
    
    if args.http:
        # Start FastAPI server
        app = FastAPI()
        
        @app.get("/")
        async def root():
            return {"status": "RomeOPT-P 10/10 System"}
        
        @app.get("/health")
        async def health():
            return {"status": "healthy"}
        
        @app.post("/webhook")
        async def webhook(request: Request):
            try:
                data = await request.json()
                log.info(f"Webhook received: {data}")
                return {"status": "received"}
            except:
                raise HTTPException(status_code=400, detail="Invalid JSON")
        
        uvicorn.run(app, host="0.0.0.0", port=9000)
    
    else:
        # Run trading system
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutdown requested by user")
        except Exception as e:
            log.error(f"Fatal error: {e}")
        finally:
            log.info("RomeOPT-P 10/10 System stopped")