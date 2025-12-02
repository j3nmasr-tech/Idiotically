#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features + Dynamic Regime Detection)
- Fully live early signals
- RomeOPT 6-step logic with elite confirmation
- Dynamic TP/SL updates (market-structure-based)
- Advanced ADX/DMI regime detection
- Regime-based signal filtering (eliminates counter-trend SL hits)
- Adaptive position sizing and stop adjustments
- Telegram alerts with regime info
- Async SQLite logging with regime tracking
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, HTF mandatory
- Elite multi-timeframe confirmation (15m,1h,4h)
- Regime monitoring and transition alerts
"""

import os, time, asyncio, logging, datetime, json
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = os.getenv("DB_PATH", "/app/data/signals.db")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# Regime detection parameters
ADX_TREND_THRESHOLD = 25
DI_SPREAD_THRESHOLD = 5
VOLUME_CONFIRMATION_RATIO = 1.2
ATR_HIGH_VOLATILITY_RATIO = 1.5
ATR_LOW_VOLATILITY_RATIO = 0.7

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# ---------------- TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg: return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- DATABASE ----------------
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0,
            latest_ob TEXT,
            regime TEXT,
            regime_adx REAL,
            regime_conf REAL,
            position_size_multiplier REAL DEFAULT 1.0,
            stop_multiplier REAL DEFAULT 1.0,
            regime_reason TEXT
        );
    """)
    await db_conn.commit()
    log.info("Database initialized with regime tracking")

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
        return None

# ---------------- ENHANCED INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    """Calculate Average True Range"""
    if len(df) < period:
        return pd.Series([0] * len(df))
    
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def calculate_dmi(df: pd.DataFrame, period=14):
    """Calculate +DI, -DI, and ADX for regime detection"""
    if len(df) < period * 2:
        return {
            'plus_di': pd.Series([50] * len(df)),
            'minus_di': pd.Series([50] * len(df)),
            'adx': pd.Series([0] * len(df))
        }
    
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)
    
    # True Range
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    atr_val = tr.rolling(period).mean().fillna(0)
    
    # Directional Movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    
    plus_dm = pd.Series(0, index=df.index)
    minus_dm = pd.Series(0, index=df.index)
    
    plus_dm[(up_move > down_move) & (up_move > 0)] = up_move
    minus_dm[(down_move > up_move) & (down_move > 0)] = down_move
    
    # Smoothed DM
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, 0.0001))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, 0.0001))
    
    # DX and ADX
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 0.0001))
    adx = dx.rolling(period).mean()
    
    return {
        'plus_di': plus_di.fillna(50),
        'minus_di': minus_di.fillna(50),
        'adx': adx.fillna(0)
    }

def calculate_atr_ratio(df: pd.DataFrame):
    """Calculate current ATR vs its 20-period average"""
    try:
        atr_val = atr(df, 14)
        if len(atr_val) < 20:
            return 1.0
        atr_ma = atr_val.rolling(20).mean()
        current_atr = atr_val.iloc[-1]
        current_atr_ma = atr_ma.iloc[-1]
        
        if pd.isna(current_atr) or pd.isna(current_atr_ma) or current_atr_ma == 0:
            return 1.0
        return current_atr / current_atr_ma
    except Exception as e:
        log.debug(f"Error calculating ATR ratio: {e}")
        return 1.0

def calculate_volume_ratio(df: pd.DataFrame):
    """Calculate current volume vs its 20-period average"""
    try:
        if 'vol' not in df.columns:
            return 1.0
            
        volume = df['vol'].astype(float)
        if len(volume) < 20:
            return 1.0
            
        volume_ma = volume.rolling(20).mean()
        current_volume = volume.iloc[-1]
        current_volume_ma = volume_ma.iloc[-1]
        
        if pd.isna(current_volume) or pd.isna(current_volume_ma) or current_volume_ma == 0:
            return 1.0
        return current_volume / current_volume_ma
    except Exception as e:
        log.debug(f"Error calculating volume ratio: {e}")
        return 1.0

# ---------------- DYNAMIC MARKET REGIME DETECTION ----------------
async def detect_market_regime(df: pd.DataFrame, symbol: str = ""):
    """
    Enhanced regime detection using ADX/DMI system
    Returns: dict with regime and confidence
    """
    if len(df) < 50:
        return {
            "regime": "RANGE",
            "confidence": 0.5,
            "trend_strength": 0,
            "adx": 0,
            "plus_di": 50,
            "minus_di": 50,
            "volume_ratio": 1.0,
            "atr_ratio": 1.0,
            "reason": "Insufficient data"
        }
    
    try:
        # Calculate DMI indicators
        dmi_data = calculate_dmi(df)
        
        current_adx = dmi_data['adx'].iloc[-1]
        plus_di = dmi_data['plus_di'].iloc[-1]
        minus_di = dmi_data['minus_di'].iloc[-1]
        
        if pd.isna(current_adx) or pd.isna(plus_di) or pd.isna(minus_di):
            return {
                "regime": "RANGE",
                "confidence": 0.5,
                "trend_strength": 0,
                "adx": 0,
                "plus_di": 50,
                "minus_di": 50,
                "volume_ratio": 1.0,
                "atr_ratio": 1.0,
                "reason": "Invalid indicator values"
            }
        
        di_spread = abs(plus_di - minus_di)
        
        # Calculate additional metrics
        atr_ratio_val = calculate_atr_ratio(df)
        volume_ratio_val = calculate_volume_ratio(df)
        
        # Price position relative to EMAs
        ema_20 = df['close'].ewm(span=20).mean().iloc[-1]
        ema_50 = df['close'].ewm(span=50).mean().iloc[-1]
        price = df['close'].iloc[-1]
        
        # Determine regime
        regime = "RANGE"
        confidence = 0.5
        trend_strength = min(current_adx / 100, 1.0)  # Normalized 0-1
        reason = ""
        
        if current_adx > ADX_TREND_THRESHOLD:
            # Strong trend regime
            if plus_di > minus_di + DI_SPREAD_THRESHOLD:
                regime = "STRONG_UPTREND"
                confidence = min(0.9, (current_adx - ADX_TREND_THRESHOLD) / 50)
                reason = f"ADX={current_adx:.1f}>25, +DI({plus_di:.1f}) > -DI({minus_di:.1f})"
                
                # Volume confirmation
                if volume_ratio_val > VOLUME_CONFIRMATION_RATIO and price > ema_20 > ema_50:
                    confidence = min(confidence + 0.1, 0.95)
                    reason += f", Volume {volume_ratio_val:.1f}x, Price>EMA20>EMA50"
                    
            elif minus_di > plus_di + DI_SPREAD_THRESHOLD:
                regime = "STRONG_DOWNTREND"
                confidence = min(0.9, (current_adx - ADX_TREND_THRESHOLD) / 50)
                reason = f"ADX={current_adx:.1f}>25, -DI({minus_di:.1f}) > +DI({plus_di:.1f})"
                
                # Volume confirmation
                if volume_ratio_val > VOLUME_CONFIRMATION_RATIO and price < ema_20 < ema_50:
                    confidence = min(confidence + 0.1, 0.95)
                    reason += f", Volume {volume_ratio_val:.1f}x, Price<EMA20<EMA50"
                    
            else:
                # ADX high but DIs not separated - volatile/chop
                regime = "HIGH_VOLATILITY"
                confidence = 0.7
                reason = f"ADX={current_adx:.1f}>25 but DI spread only {di_spread:.1f}"
        else:
            # Weak trend - check for range or low volatility
            if atr_ratio_val > ATR_HIGH_VOLATILITY_RATIO:
                regime = "HIGH_VOLATILITY_CHOPPY"
                confidence = 0.6
                reason = f"ATR ratio {atr_ratio_val:.1f}>1.5 (high volatility)"
            elif atr_ratio_val < ATR_LOW_VOLATILITY_RATIO:
                regime = "LOW_VOLATILITY_RANGE"
                confidence = 0.8
                reason = f"ATR ratio {atr_ratio_val:.1f}<0.7 (low volatility)"
            else:
                regime = "RANGE"
                confidence = 0.5
                reason = f"ADX={current_adx:.1f}<25, ATR ratio {atr_ratio_val:.1f} normal"
        
        return {
            "regime": regime,
            "confidence": round(float(confidence), 2),
            "trend_strength": round(float(trend_strength), 2),
            "adx": round(float(current_adx), 2),
            "plus_di": round(float(plus_di), 2),
            "minus_di": round(float(minus_di), 2),
            "volume_ratio": round(float(volume_ratio_val), 2),
            "atr_ratio": round(float(atr_ratio_val), 2),
            "reason": reason
        }
        
    except Exception as e:
        log.error(f"Error in detect_market_regime for {symbol}: {e}")
        return {
            "regime": "RANGE",
            "confidence": 0.5,
            "trend_strength": 0,
            "adx": 0,
            "plus_di": 50,
            "minus_di": 50,
            "volume_ratio": 1.0,
            "atr_ratio": 1.0,
            "reason": f"Error: {str(e)[:100]}"
        }

# ---------------- REGIME-BASED SIGNAL FILTER ----------------
def filter_by_regime(signal: dict, regime_info: dict):
    """
    Filter signals based on detected market regime
    Returns: dict with action and adjustments
    """
    regime = regime_info["regime"]
    confidence = regime_info["confidence"]
    signal_side = signal["side"]
    symbol = signal.get("symbol", "UNKNOWN")
    
    # Reject counter-trend signals in strong trends with high confidence
    if confidence > 0.7:
        if regime == "STRONG_UPTREND" and signal_side == "SELL":
            return {
                "action": "REJECT",
                "reason": f"Counter-trade in STRONG_UPTREND (ADX: {regime_info['adx']}, Conf: {confidence})",
                "regime": regime,
                "confidence": confidence
            }
        elif regime == "STRONG_DOWNTREND" and signal_side == "BUY":
            return {
                "action": "REJECT", 
                "reason": f"Counter-trade in STRONG_DOWNTREND (ADX: {regime_info['adx']}, Conf: {confidence})",
                "regime": regime,
                "confidence": confidence
            }
    
    # Adjust position sizing and stops based on regime
    position_size_multiplier = 1.0
    stop_multiplier = 1.0
    tp_multiplier = 1.0
    filter_reason = regime_info.get("reason", "")
    
    if regime == "STRONG_UPTREND" and signal_side == "BUY":
        position_size_multiplier = 1.2  # Increase size in trend direction
        stop_multiplier = 0.9  # Tighter stop (trend protects)
        tp_multiplier = 1.2  # Extend TP
        filter_reason += " | Trend-following BUY, size++"
        
    elif regime == "STRONG_DOWNTREND" and signal_side == "SELL":
        position_size_multiplier = 1.2
        stop_multiplier = 0.9
        tp_multiplier = 1.2
        filter_reason += " | Trend-following SELL, size++"
        
    elif regime == "HIGH_VOLATILITY_CHOPPY":
        position_size_multiplier = 0.5  # Reduce size in chop
        stop_multiplier = 1.5  # Wider stops
        tp_multiplier = 0.8  # Take profit sooner
        filter_reason += " | High volatility, size--"
        
    elif regime == "LOW_VOLATILITY_RANGE":
        position_size_multiplier = 0.8
        stop_multiplier = 0.8  # Tighter stops in range
        tp_multiplier = 1.0
        filter_reason += " | Low volatility range"
    
    # In range markets, allow both directions
    elif regime == "RANGE":
        position_size_multiplier = 1.0
        stop_multiplier = 1.0
        tp_multiplier = 1.0
        filter_reason += " | Range market"
    
    return {
        "action": "ACCEPT",
        "regime": regime,
        "confidence": confidence,
        "position_size_multiplier": position_size_multiplier,
        "stop_multiplier": stop_multiplier,
        "tp_multiplier": tp_multiplier,
        "reason": filter_reason
    }

# ---------------- MULTI-TIMEFRAME ELITE CONFIRM ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    """Elite multi-timeframe confirmation"""
    tfs = ["15m","1h","4h"]
    alignments = []
    
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv or len(ohlcv) < 10:
            alignments.append(False)
            continue
            
        df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
        df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].apply(pd.to_numeric, errors='coerce')
        
        # Simple trend detection
        if len(df) >= 5:
            recent_close = df["close"].iloc[-1]
            past_close = df["close"].iloc[-5]
            trend = recent_close - past_close
            trend_side = "BUY" if trend > 0 else "SELL"
            alignments.append(trend_side == side)
        else:
            alignments.append(False)
    
    # Require at least 2 out of 3 timeframes aligned
    true_count = sum(alignments)
    return true_count >= 2

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """Core RomeOPT 6-step signal generation"""
    if df is None or len(df) < 20:
        return None
    
    # Ensure we have required columns
    if not all(col in df.columns for col in ["open", "high", "low", "close"]):
        return None
        
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1] if len(df) >= 6 else df.iloc[-len(df):-1]
    score = 0
    reasons = []

    # Step 1: Liquidity Sweep
    if not prev5.empty:
        sweep_high = last["high"] > prev5["high"].max()
        sweep_low = last["low"] < prev5["low"].min()
    else:
        sweep_high = False
        sweep_low = False
        
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")

    # Step 2: Displacement
    candle_range = last["high"] - last["low"]
    if candle_range > 0:
        displacement = abs(last["close"] - last["open"]) / candle_range
    else:
        displacement = 0
        
    has_disp = displacement > 0.6
    if has_disp:
        score += 2
        reasons.append("Displacement +2")
    else:
        reasons.append("Displacement +0")

    # Step 3 & 4: Order Block & Zone
    ob_zone = None
    ob_type = None
    for i in range(max(1, len(df)-5), len(df)-1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        if candle["close"] > candle["open"] and prev_candle["close"] < prev_candle["open"]:
            ob_zone = {
                "type": "bullish",
                "low": min(candle["low"], prev_candle["low"]),
                "high": candle["close"]
            }
            ob_type = "bullish"
            break
        elif candle["close"] < candle["open"] and prev_candle["close"] > prev_candle["open"]:
            ob_zone = {
                "type": "bearish",
                "low": candle["close"],
                "high": max(candle["high"], prev_candle["high"])
            }
            ob_type = "bearish"
            break

    if ob_zone:
        if ob_type == "bullish" and last["close"] <= ob_zone["high"]:
            score += 1
            reasons.append("Zone Approach +1")
        elif ob_type == "bearish" and last["close"] >= ob_zone["low"]:
            score += 1
            reasons.append("Zone Approach +1")
        else:
            reasons.append("Zone Approach +0")
    else:
        reasons.append("Zone Approach +0")

    # Step 5: HTF Alignment
    tf_map = {"1m": "15m", "3m": "30m", "5m": "1h", "15m": "4h", "30m": "1h"}
    htf = tf_map.get(tf, "15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    
    if ohlcv_htf and len(ohlcv_htf) >= 10:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_htf[["open", "high", "low", "close", "volume"]] = df_htf[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors='coerce')
        
        if len(df_htf) >= 5:
            trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
            htf_dir = "bullish" if trend > 0 else "bearish"
            
            if ob_type and htf_dir == ob_type:
                score += 1
                htf_alignment = 1
                reasons.append("HTF Alignment +1")
            else:
                reasons.append("HTF Alignment +0")
        else:
            reasons.append("HTF Alignment +0")
    else:
        reasons.append("HTF Alignment ?")

    # Step 6: Momentum
    momentum_ratio = displacement  # Reuse displacement calculation
    if ob_type == "bullish" and momentum_ratio > 0.5 and last["close"] > last["open"]:
        score += 1
        reasons.append("Momentum +1")
    elif ob_type == "bearish" and momentum_ratio > 0.5 and last["close"] < last["open"]:
        score += 1
        reasons.append("Momentum +1")
    else:
        reasons.append("Momentum +0")

    if not ob_type:
        return None
        
    side = "BUY" if ob_type == "bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN:
        log.debug(f"{symbol} rejected: critical_score {critical_score} < {CRITICAL_FACTORS_MIN}")
        return None
        
    if score < MIN_SCORE:
        log.debug(f"{symbol} rejected: score {score} < {MIN_SCORE}")
        return None
        
    if not has_disp:
        log.debug(f"{symbol} rejected: no displacement")
        return None
    
    # ---------------- HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:
        log.debug(f"{symbol} rejected: HTF alignment {htf_alignment} != 1")
        return None

    # ---------------- DYNAMIC REGIME DETECTION & FILTERING ----------------
    regime_info = await detect_market_regime(df, symbol)
    regime_filter = filter_by_regime({"symbol": symbol, "side": side}, regime_info)
    
    if regime_filter["action"] == "REJECT":
        log.info(f"Signal rejected by regime filter: {symbol} {side} - {regime_filter['reason']}")
        return None

    # Check basic trend alignment (backward compatibility)
    if len(df) >= 20:
        trend_ma = df["close"].rolling(20).mean().iloc[-1]
        if (side == "BUY" and last["close"] < trend_ma) or (side == "SELL" and last["close"] > trend_ma):
            log.debug(f"{symbol} rejected: against basic trend MA")
            return None
    else:
        log.debug(f"{symbol} rejected: insufficient data for trend MA")
        return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        log.debug(f"{symbol} rejected: elite MTF alignment failed")
        return None
        
    reasons.append("Elite MTF Alignment ✅")

    # Create signal object
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "regime_info": regime_info,
        "regime_filter": regime_filter
    }
    
    # Generate TP/SL
    sig = update_tp_sl_live(sig, df)
    
    if not sig or "sl" not in sig or "tp1" not in sig:
        return None
    
    # Apply regime-based adjustments
    if "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        
        # Apply stop multiplier
        stop_mult = regime_filter.get("stop_multiplier", 1.0)
        if stop_mult != 1.0 and risk > 0:
            if side == "BUY":
                new_sl = sig["entry"] - (risk * stop_mult)
                # Don't move SL too far from original
                if abs(new_sl - sig["sl"]) / sig["entry"] < 0.02:  # Max 2% adjustment
                    sig["sl"] = new_sl
            else:
                new_sl = sig["entry"] + (risk * stop_mult)
                if abs(new_sl - sig["sl"]) / sig["entry"] < 0.02:
                    sig["sl"] = new_sl
        
        # Apply TP multiplier (carefully)
        tp_mult = regime_filter.get("tp_multiplier", 1.0)
        if tp_mult != 1.0:
            # Only adjust TP3 for trend-following, keep TP1/TP2 stable
            if tp_mult > 1.0 and "tp3" in sig:
                if side == "BUY":
                    tp3_distance = sig["tp3"] - sig["entry"]
                    sig["tp3"] = sig["entry"] + (tp3_distance * tp_mult)
                else:
                    tp3_distance = sig["entry"] - sig["tp3"]
                    sig["tp3"] = sig["entry"] - (tp3_distance * tp_mult)
    
    # ---------------- TP1 DISTANCE FILTER ----------------
    if "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        # Reject if TP1 is less than 20% of risk (meaningless profit)
        if risk > 0 and tp1_distance < risk * 0.2:
            log.debug(f"{symbol} rejected: TP1 distance {tp1_distance:.6f} < 20% of risk {risk:.6f}")
            return None
    
    return sig

# ---------------- TP/SL HELPERS ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """Optimized TP/SL using market structure + ATR"""
    if len(df) < 10:
        # Default values if insufficient data
        if side == "BUY":
            return entry * 0.995, entry * 1.005, entry * 1.01, entry * 1.02
        else:
            return entry * 1.005, entry * 0.995, entry * 0.99, entry * 0.98
    
    recent_high = df['high'].iloc[-10:].max()
    recent_low = df['low'].iloc[-10:].min()

    if side == "BUY":
        # Calculate SL
        sl_ob = ob_zone["low"] - (atr_val * 0.3)
        sl_structure = recent_low - (atr_val * 0.3)
        sl = min(sl_ob, sl_structure)
        
        risk = entry - sl
        
        # Ensure minimum meaningful risk
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        # Calculate TP levels
        base_tp1 = entry + (risk * 0.8)
        base_tp2 = entry + (risk * 1.5)
        base_tp3 = entry + (risk * 2.5)
        
        # Get market structure levels
        nearest_resistance = df['high'].tail(20).max()
        major_resistance = df['high'].tail(50).max()
        
        # Choose better profit level
        tp1 = min(base_tp1, nearest_resistance) if nearest_resistance > entry else base_tp1
        tp2 = min(base_tp2, major_resistance) if major_resistance > tp1 else base_tp2
        tp3 = base_tp3
        
        # Ensure proper ordering
        min_tp_gap = risk * 0.3
        
        tp1 = max(tp1, entry + (risk * 0.5))
        tp2 = max(tp2, tp1 + min_tp_gap)
        tp3 = max(tp3, tp2 + min_tp_gap)
        
    else:  # SELL
        # Calculate SL
        sl_ob = ob_zone["high"] + (atr_val * 0.3)
        sl_structure = recent_high + (atr_val * 0.3)
        sl = max(sl_ob, sl_structure)
        
        risk = sl - entry
        
        # Ensure minimum meaningful risk
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        # Calculate TP levels
        base_tp1 = entry - (risk * 0.8)
        base_tp2 = entry - (risk * 1.5)
        base_tp3 = entry - (risk * 2.5)
        
        # Get market structure levels
        nearest_support = df['low'].tail(20).min()
        major_support = df['low'].tail(50).min()
        
        # Choose better profit level
        tp1 = max(base_tp1, nearest_support) if nearest_support < entry else base_tp1
        tp2 = max(base_tp2, major_support) if major_support < tp1 else base_tp2
        tp3 = base_tp3
        
        # Ensure proper ordering
        min_tp_gap = risk * 0.3
        
        tp1 = min(tp1, entry - (risk * 0.5))
        tp2 = min(tp2, tp1 - min_tp_gap)
        tp3 = min(tp3, tp2 - min_tp_gap)

    return sl, tp1, tp2, tp3

def find_latest_ob(df: pd.DataFrame):
    """Find the latest order block"""
    for i in range(max(1, len(df)-5), len(df)-1):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        if candle["close"] > candle["open"] and prev_candle["close"] < prev_candle["open"]:
            return {
                "type": "bullish",
                "low": min(candle["low"], prev_candle["low"]),
                "high": candle["close"]
            }
        elif candle["close"] < candle["open"] and prev_candle["close"] > prev_candle["open"]:
            return {
                "type": "bearish",
                "low": candle["close"],
                "high": max(candle["high"], prev_candle["high"])
            }
    return None

def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """Update TP/SL based on latest market structure"""
    latest_ob = find_latest_ob(df)
    if not latest_ob:
        return sig
        
    atr_val = float(atr(df, 14).iloc[-1])
    entry = sig["entry"]
    side = sig["side"]
    
    sl, tp1, tp2, tp3 = romeopt_tp_sl(entry, side, atr_val, latest_ob, df)
    
    sig["sl"] = sl
    sig["tp1"] = tp1
    sig["tp2"] = tp2
    sig["tp3"] = tp3
    sig["latest_ob"] = latest_ob
    
    return sig

# ---------------- REGIME MONITORING ----------------
regime_history = defaultdict(lambda: deque(maxlen=50))
last_regime_alert = {}

async def log_regime_transition(symbol: str, timeframe: str, regime_info: dict):
    """Log regime transitions for analysis and alerts"""
    current_regime = regime_info["regime"]
    current_adx = regime_info["adx"]
    key = f"{symbol}:{timeframe}"
    
    history = regime_history[key]
    
    # Check if we should alert
    should_alert = False
    alert_reason = ""
    
    if history:
        last_entry = history[-1]
        last_regime = last_entry["regime"]
        last_adx = last_entry["adx"]
        
        # Major regime change
        if last_regime != current_regime:
            should_alert = True
            alert_reason = f"Regime change: {last_regime} → {current_regime}"
            
        # ADX crossing threshold
        elif last_adx <= ADX_TREND_THRESHOLD and current_adx > ADX_TREND_THRESHOLD:
            should_alert = True
            alert_reason = f"Trend strength increasing (ADX: {last_adx:.1f} → {current_adx:.1f})"
            
        elif last_adx > ADX_TREND_THRESHOLD and current_adx <= ADX_TREND_THRESHOLD:
            should_alert = True
            alert_reason = f"Trend strength decreasing (ADX: {last_adx:.1f} → {current_adx:.1f})"
    
    # Rate limit alerts (once per 30 minutes per symbol:timeframe)
    now = time.time()
    last_alert = last_regime_alert.get(key, 0)
    
    if should_alert and (now - last_alert) > 1800:  # 30 minutes
        await tg(f"⚠️ {symbol} ({timeframe}) {alert_reason}")
        last_regime_alert[key] = now
    
    # Store in history
    history.append({
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "regime": current_regime,
        "adx": current_adx,
        "confidence": regime_info["confidence"]
    })

# ---------------- SL CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time()
    dq = recent_sl[symbol]
    dq.append(now)
    cutoff = now - lookback_minutes * 60
    while dq and dq[0] < cutoff:
        dq.popleft()

def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]
    now = time.time()
    cutoff = now - lookback * 60
    while dq and dq[0] < cutoff:
        dq.popleft()
    return len(dq) >= threshold

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    """Log signal to database with regime information"""
    async with db_lock:
        try:
            regime = sig.get("regime_info", {}).get("regime", "UNKNOWN")
            adx_val = sig.get("regime_info", {}).get("adx", 0)
            conf = sig.get("regime_info", {}).get("confidence", 0.5)
            pos_mult = sig.get("regime_filter", {}).get("position_size_multiplier", 1.0)
            stop_mult = sig.get("regime_filter", {}).get("stop_multiplier", 1.0)
            regime_reason = sig.get("regime_info", {}).get("reason", "")
            
            # Convert latest_ob to string
            latest_ob = sig.get("latest_ob", {})
            latest_ob_str = json.dumps(latest_ob) if latest_ob else ""
            
            await db_conn.execute("""
                INSERT INTO signals (symbol, side, entry, sl, tp1, tp2, tp3, timestamp, status, reason, score, 
                                    latest_ob, regime, regime_adx, regime_conf, position_size_multiplier, 
                                    stop_multiplier, regime_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                sig["symbol"], sig["side"], sig["entry"], sig.get("sl"), sig.get("tp1"), 
                sig.get("tp2"), sig.get("tp3"), datetime.datetime.utcnow().isoformat(), 
                "OPEN", sig["reason"], sig["score"], latest_ob_str,
                regime, adx_val, conf, pos_mult, stop_mult, regime_reason
            ))
            await db_conn.commit()
            log.info(f"Logged signal: {sig['symbol']} {sig['side']} (Regime: {regime}, Score: {sig['score']})")
        except Exception as e:
            log.error(f"Error logging signal: {e}")

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """Monitor open signals for TP/SL hits"""
    while True:
        try:
            async with db_lock:
                async with db_conn.execute(
                    "SELECT id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status FROM signals WHERE status='OPEN'"
                ) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            
                            if last_price is None:
                                continue
                                
                            # Update TP/SL based on current market structure
                            ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 50)
                            if ohlcv:
                                df_live = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                                for c in ["open", "high", "low", "close", "volume"]:
                                    df_live[c] = pd.to_numeric(df_live[c], errors="coerce")
                                
                                sig_data = {
                                    "symbol": symbol, "side": side, "entry": entry, 
                                    "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3
                                }
                                sig_data = update_tp_sl_live(sig_data, df_live)
                                if sig_data:
                                    sl, tp1, tp2, tp3 = sig_data["sl"], sig_data["tp1"], sig_data["tp2"], sig_data["tp3"]
                            
                            # Check for hits
                            hits = []
                            sl_hit = False
                            
                            if side == "BUY":
                                if not tp1_hit and last_price >= tp1:
                                    hits.append("TP1")
                                    tp1_hit = 1
                                if not tp2_hit and last_price >= tp2:
                                    hits.append("TP2")
                                    tp2_hit = 1
                                if not tp3_hit and last_price >= tp3:
                                    hits.append("TP3")
                                    tp3_hit = 1
                                if last_price <= sl:
                                    hits.append("SL")
                                    status = "CLOSED"
                                    sl_hit = True
                            else:  # SELL
                                if not tp1_hit and last_price <= tp1:
                                    hits.append("TP1")
                                    tp1_hit = 1
                                if not tp2_hit and last_price <= tp2:
                                    hits.append("TP2")
                                    tp2_hit = 1
                                if not tp3_hit and last_price <= tp3:
                                    hits.append("TP3")
                                    tp3_hit = 1
                                if last_price >= sl:
                                    hits.append("SL")
                                    status = "CLOSED"
                                    sl_hit = True
                            
                            if hits:
                                regime_info = ""
                                async with db_conn.execute(
                                    "SELECT regime, regime_adx FROM signals WHERE id=?", (sig_id,)
                                ) as reg_cursor:
                                    reg_row = await reg_cursor.fetchone()
                                    if reg_row:
                                        regime_info = f" (Regime: {reg_row[0]}, ADX: {reg_row[1]})"
                                
                                await tg(
                                    f"🎯 {symbol}{regime_info} {side} update\n"
                                    f"Entry: {entry}\nLast: {last_price}\n"
                                    f"Hits: {','.join(hits)}\n"
                                    f"SL: {sl}\nTP1: {tp1} TP2: {tp2} TP3: {tp3}"
                                )
                            
                            if sl_hit:
                                record_sl_hit(symbol)
                            
                            # Update database
                            await db_conn.execute(
                                "UPDATE signals SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=?, sl=?, tp1=?, tp2=?, tp3=? WHERE id=?",
                                (tp1_hit, tp2_hit, tp3_hit, status, sl, tp1, tp2, tp3, sig_id)
                            )
                            
                        except Exception as e:
                            log.error(f"Error monitoring signal {symbol}: {e}")
                            continue
                            
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
            
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    """Main scanning loop"""
    while True:
        t0 = time.time()
        try:
            # Fetch top volume pairs
            tickers = await exchange.fetch_tickers()
            top = sorted(
                [(s, v.get("quoteVolume", 0)) for s, v in tickers.items() if s.endswith("/USDT")],
                key=lambda x: x[1], reverse=True
            )[:TOP_N]
            
            signals_found = 0
            
            for symbol, _ in top:
                if deprioritized(symbol):
                    log.debug(f"Skipping {symbol}: deprioritized due to recent SL hits")
                    continue
                    
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    
                    # Rate limiting
                    if key in last_signal_time and time.time() - last_signal_time[key] < 60:
                        continue
                    
                    # Fetch OHLCV data
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv or len(ohlcv) < 50:
                        continue
                        
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    # Rename volume column to 'vol' for compatibility
                    df = df.rename(columns={"volume": "vol"})
                    for c in ["open", "high", "low", "close", "vol"]:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    
                    # Generate signal
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    
                    if sig:
                        # Log regime transition
                        await log_regime_transition(symbol, tf, sig["regime_info"])
                        
                        # Prepare Telegram message
                        regime_msg = ""
                        if "regime_info" in sig:
                            regime = sig["regime_info"]["regime"]
                            conf = sig["regime_info"]["confidence"]
                            adx_val = sig["regime_info"]["adx"]
                            regime_msg = f"\n📊 Regime: {regime} (Conf: {conf}, ADX: {adx_val})"
                        
                        htf_flag = sig.get("htf_alignment", "N/A")
                        sweep_flag = sig.get("liquidity_sweep", "N/A")
                        
                        await tg(
                            f"🏆 {sig['symbol']} ({tf}) {sig['side']}{regime_msg}\n"
                            f"Entry: {sig['entry']}\n"
                            f"SL: {sig.get('sl')}\n"
                            f"TP1: {sig.get('tp1')} TP2: {sig.get('tp2')} TP3: {sig.get('tp3')}\n"
                            f"Score: {sig['score']}\n"
                            f"HTF: {htf_flag} Sweep: {sweep_flag}\n"
                            f"Breakdown: {', '.join(sig['reason_list'])}"
                        )
                        
                        # Log to database
                        await log_signal(sig)
                        
                        # Update rate limiting
                        last_signal_time[key] = time.time()
                        signals_found += 1
            
            if signals_found > 0:
                log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found (with regime filtering)")
            
        except Exception as e:
            log.exception(f"Scan error: {e}")
            
        elapsed = time.time() - t0
        sleep_time = max(1, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "RomeOPT Scanner with Dynamic Regime Detection", "version": "2.0"}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    log.info(f"Webhook received: {data}")
    
    # You could add webhook-based signal triggering here
    return {"ok": True}

@app.get("/signals")
async def get_signals(limit: int = 50, regime: str = None):
    """API endpoint to query recent signals"""
    async with db_lock:
        try:
            query = "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?"
            params = [limit]
            
            if regime:
                query = "SELECT * FROM signals WHERE regime = ? ORDER BY timestamp DESC LIMIT ?"
                params = [regime, limit]
                
            async with db_conn.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                
            signals = []
            for row in rows:
                signal = dict(zip(columns, row))
                # Convert to JSON serializable format
                for key, value in signal.items():
                    if isinstance(value, float) and (pd.isna(value) or np.isnan(value)):
                        signal[key] = None
                signals.append(signal)
                
            return {"signals": signals, "count": len(signals)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/regime_stats")
async def get_regime_stats():
    """API endpoint to get regime statistics"""
    async with db_lock:
        try:
            # Get regime distribution
            async with db_conn.execute(
                "SELECT regime, COUNT(*) as count, "
                "AVG(CASE WHEN status='CLOSED' AND tp1_hit=1 THEN 1.0 ELSE 0.0 END) as win_rate "
                "FROM signals WHERE regime != 'UNKNOWN' GROUP BY regime"
            ) as cursor:
                regime_stats = await cursor.fetchall()
                
            # Get recent regime transitions
            recent_transitions = []
            for key, history in list(regime_history.items())[:20]:  # Limit to 20 symbols
                if history:
                    symbol_tf = key.split(":")
                    recent_transitions.append({
                        "symbol": symbol_tf[0],
                        "timeframe": symbol_tf[1] if len(symbol_tf) > 1 else "N/A",
                        "current_regime": history[-1]["regime"],
                        "current_adx": history[-1]["adx"],
                        "timestamp": history[-1]["timestamp"]
                    })
                    
            return {
                "regime_stats": [
                    {"regime": row[0], "count": row[1], "win_rate": float(row[2]) if row[2] else 0} 
                    for row in regime_stats
                ],
                "recent_transitions": recent_transitions[:10]  # Last 10
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ---------------- MAIN ----------------
async def main():
    """Main application entry point"""
    global exchange
    
    # Initialize
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    
    # Startup message
    await tg("🚀 ROMEOPT 6-Step Scanner Started - Enhanced with Dynamic Regime Detection")
    await tg(f"📊 Regime Detection Active | ADX Threshold: {ADX_TREND_THRESHOLD} | DI Spread: {DI_SPREAD_THRESHOLD}")
    
    log.info("Starting RomeOPT Scanner with Dynamic Regime Detection")
    
    # Run main tasks
    try:
        await asyncio.gather(
            scan_loop(exchange),
            monitor_signals()
        )
    except KeyboardInterrupt:
        log.info("Shutdown requested")
    except Exception as e:
        log.exception(f"Fatal error: {e}")
    finally:
        # Clean shutdown
        try:
            if db_conn:
                await db_conn.close()
        except:
            pass
            
        try:
            if exchange:
                await exchange.close()
        except:
            pass
            
        await tg("🔴 ROMEOPT Scanner Stopped")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RomeOPT 6-Step Scanner")
    parser.add_argument("--http", action="store_true", help="Start HTTP server")
    parser.add_argument("--port", type=int, default=9000, help="HTTP server port")
    args = parser.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=args.port)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\nShutting down gracefully...")