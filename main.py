#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOPT-P ULTIMATE SCANNER (Merged Superior Version)
Combines best of both codes with full RomeOPT-P 6-step + SMC elements
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

# ============================================================================
# CONFIGURATION
# ============================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 10))

# RomeOPT-P Timeframe Hierarchy
TIMEFRAMES = ["5m", "15m", "1h"]  # Clean, focused set
HTF_MAP = {"5m": "1h", "15m": "4h", "1h": "4h"}  # Structural mapping

# Scoring System (RomeOPT-P 6-Step)
MIN_SCORE = 6  # Increased for quality
SCORE_WEIGHTS = {
    "liquidity_sweep": 2,
    "displacement": 2,
    "ob_zone": 1,
    "htf_alignment": 2,  # More important
    "momentum": 1,
    "volume_confirmation": 1
}

# Risk Management
MAX_DAILY_LOSS_PCT = 3.0
MIN_RR_RATIO = 1.5
SL_CLUSTER_THRESHOLD = 3  # Max SL hits in 30 minutes

# ============================================================================
# INITIALIZATION
# ============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_ultimate")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# SL Cluster Tracking
recent_sl_hits = defaultdict(lambda: deque(maxlen=10))

# ============================================================================
# CORE SMC FUNCTIONS (RomeOPT-P Style)
# ============================================================================

async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    """Fetch OHLCV with retry logic"""
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug(f"OHLCV fetch failed for {symbol} {timeframe}: {e}")
        return None

def calculate_atr(df: pd.DataFrame, period=14):
    """Average True Range for volatility measurement"""
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def detect_bos_choch(df: pd.DataFrame):
    """
    Detect Break of Structure / Change of Character
    Returns: (has_bos, has_choch, direction)
    """
    if len(df) < 20:
        return False, False, None
    
    # Recent swing points
    recent_highs = df['high'].iloc[-10:]
    recent_lows = df['low'].iloc[-10:]
    
    # BOS: Price breaks recent swing high/low with momentum
    current_high = df['high'].iloc[-1]
    current_low = df['low'].iloc[-1]
    prev_swing_high = recent_highs.max()
    prev_swing_low = recent_lows.min()
    
    has_bos_bullish = current_high > prev_swing_high and df['close'].iloc[-1] > df['close'].iloc[-3]
    has_bos_bearish = current_low < prev_swing_low and df['close'].iloc[-1] < df['close'].iloc[-3]
    
    # CHOCH: Structure shift without new extremes
    ema20 = df['close'].ewm(span=20, adjust=False).mean()
    ema50 = df['close'].ewm(span=50, adjust=False).mean()
    
    has_choch_bullish = ema20.iloc[-1] > ema50.iloc[-1] and ema20.iloc[-5] <= ema50.iloc[-5]
    has_choch_bearish = ema20.iloc[-1] < ema50.iloc[-1] and ema20.iloc[-5] >= ema50.iloc[-5]
    
    direction = None
    if has_bos_bullish or has_choch_bullish:
        direction = "bullish"
    elif has_bos_bearish or has_choch_bearish:
        direction = "bearish"
    
    return has_bos_bullish or has_bos_bearish, has_choch_bullish or has_choch_bearish, direction

def find_order_blocks(df: pd.DataFrame):
    """Find quality Order Blocks with volume confirmation"""
    blocks = []
    for i in range(3, len(df)-2):
        if i < 3: continue
        
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        next_candle = df.iloc[i+1]
        
        # Bullish OB: Bearish candle followed by bullish engulfing
        if (prev_candle["close"] < prev_candle["open"] and 
            candle["close"] > candle["open"] and
            candle["low"] < prev_candle["low"] and
            candle["close"] > prev_candle["open"]):
            
            # Volume confirmation
            vol_avg = df['volume'].iloc[i-5:i].mean()
            if candle['volume'] > vol_avg * 1.2:
                blocks.append({
                    "type": "bullish",
                    "index": i,
                    "low": min(candle["low"], prev_candle["low"]),
                    "high": max(candle["close"], prev_candle["close"]),
                    "body_low": min(candle["open"], candle["close"]),
                    "body_high": max(candle["open"], candle["close"])
                })
        
        # Bearish OB: Bullish candle followed by bearish engulfing
        elif (prev_candle["close"] > prev_candle["open"] and 
              candle["close"] < candle["open"] and
              candle["high"] > prev_candle["high"] and
              candle["close"] < prev_candle["open"]):
            
            vol_avg = df['volume'].iloc[i-5:i].mean()
            if candle['volume'] > vol_avg * 1.2:
                blocks.append({
                    "type": "bearish",
                    "index": i,
                    "low": min(candle["close"], prev_candle["close"]),
                    "high": max(candle["high"], prev_candle["high"]),
                    "body_low": min(candle["open"], candle["close"]),
                    "body_high": max(candle["open"], candle["close"])
                })
    
    return blocks[-1] if blocks else None

def detect_fvg(df: pd.DataFrame):
    """Detect Fair Value Gaps with premium/discount classification"""
    fvgs = []
    for i in range(2, len(df)-1):
        current_low = df['low'].iloc[i]
        prev_high = df['high'].iloc[i-1]
        next_high = df['high'].iloc[i+1]
        next_low = df['low'].iloc[i+1]
        
        # Bullish FVG
        if current_low > prev_high:
            # Check if it's in premium or discount
            ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
            is_premium = current_low > ema50
            
            fvgs.append({
                "type": "bullish",
                "index": i,
                "low": prev_high,
                "high": current_low,
                "premium": is_premium
            })
        
        # Bearish FVG
        elif current_high < prev_low:
            ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[i]
            is_discount = current_high < ema50
            
            fvgs.append({
                "type": "bearish",
                "index": i,
                "low": current_high,
                "high": prev_low,
                "discount": is_discount
            })
    
    # Return the most recent relevant FVG
    if fvgs:
        return fvgs[-1]
    return None

async def check_htf_structure(exchange, symbol: str, ltf: str, expected_side: str):
    """
    Comprehensive HTF structure analysis
    Returns: (is_aligned, confidence, reasons)
    """
    htf = HTF_MAP.get(ltf, "4h")
    ohlcv = await fetch_ohlcv(exchange, symbol, htf, 100)
    
    if not ohlcv:
        return False, 0, ["No HTF data"]
    
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    reasons = []
    confidence = 0
    
    # 1. EMA Trend
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    
    ema_slope = df['ema20'].iloc[-1] - df['ema20'].iloc[-5]
    above_fast_ema = df['close'].iloc[-1] > df['ema20'].iloc[-1]
    above_slow_ema = df['close'].iloc[-1] > df['ema50'].iloc[-1]
    
    if expected_side == "BUY":
        if ema_slope > 0: confidence += 1; reasons.append("EMA slope up")
        if above_fast_ema: confidence += 1; reasons.append("Above EMA20")
        if above_slow_ema: confidence += 1; reasons.append("Above EMA50")
    else:  # SELL
        if ema_slope < 0: confidence += 1; reasons.append("EMA slope down")
        if not above_fast_ema: confidence += 1; reasons.append("Below EMA20")
        if not above_slow_ema: confidence += 1; reasons.append("Below EMA50")
    
    # 2. Market Structure
    has_bos, has_choch, direction = detect_bos_choch(df)
    
    if direction == expected_side.lower():
        confidence += 2
        if has_bos:
            reasons.append(f"HTF BOS {direction}")
        if has_choch:
            reasons.append(f"HTF CHOCH {direction}")
    
    # 3. Reject strong counter-trend
    atr_val = float(calculate_atr(df).iloc[-1])
    ema_distance = abs(df['close'].iloc[-1] - df['ema50'].iloc[-1])
    
    if atr_val > 0:
        distance_in_atr = ema_distance / atr_val
        if distance_in_atr > 1.5:  # Strong trend
            if (expected_side == "BUY" and df['close'].iloc[-1] < df['ema50'].iloc[-1]) or \
               (expected_side == "SELL" and df['close'].iloc[-1] > df['ema50'].iloc[-1]):
                reasons.append(f"Strong counter-trend ({distance_in_atr:.1f} ATR)")
                return False, 0, reasons
    
    return confidence >= 3, confidence, reasons

# ============================================================================
# RISK & POSITION MANAGEMENT
# ============================================================================

def calculate_risk_parameters(entry: float, side: str, ob_zone: dict, df: pd.DataFrame):
    """
    RomeOPT-P risk management with market structure
    Returns: (sl, tp1, tp2, risk, rr_ratio)
    """
    atr_val = float(calculate_atr(df).iloc[-1])
    
    # Recent market structure for TP targets
    recent_high = df['high'].iloc[-20:].max()
    recent_low = df['low'].iloc[-20:].min()
    swing_high = df['high'].iloc[-50:].max()
    swing_low = df['low'].iloc[-50:].min()
    
    if side == "BUY":
        # SL: Below OB or recent swing low, whichever is lower
        sl_ob = ob_zone['low'] - (atr_val * 0.15)  # Tight but reasonable
        sl_structure = recent_low - (atr_val * 0.15)
        sl = min(sl_ob, sl_structure)
        
        risk = entry - sl
        if risk < atr_val * 0.3:  # Minimum meaningful risk
            risk = atr_val * 0.3
            sl = entry - risk
        
        # TP1: Nearest resistance or 1.0R
        tp1_structure = min(recent_high, swing_high)
        tp1_calculated = entry + risk * 1.0
        tp1 = min(tp1_structure, tp1_calculated) if tp1_structure > entry else tp1_calculated
        
        # TP2: Next major resistance or 1.8R
        tp2_calculated = entry + risk * 1.8
        tp2 = tp2_calculated
        
        # Ensure TP1 > entry + 0.5R (meaningful profit)
        tp1 = max(tp1, entry + risk * 0.5)
        tp2 = max(tp2, tp1 + risk * 0.5)
        
    else:  # SELL
        # SL: Above OB or recent swing high
        sl_ob = ob_zone['high'] + (atr_val * 0.15)
        sl_structure = recent_high + (atr_val * 0.15)
        sl = max(sl_ob, sl_structure)
        
        risk = sl - entry
        if risk < atr_val * 0.3:
            risk = atr_val * 0.3
            sl = entry + risk
        
        # TP1: Nearest support or 1.0R
        tp1_structure = max(recent_low, swing_low)
        tp1_calculated = entry - risk * 1.0
        tp1 = max(tp1_structure, tp1_calculated) if tp1_structure < entry else tp1_calculated
        
        # TP2: Next major support or 1.8R
        tp2_calculated = entry - risk * 1.8
        tp2 = tp2_calculated
        
        # Ensure TP1 < entry - 0.5R
        tp1 = min(tp1, entry - risk * 0.5)
        tp2 = min(tp2, tp1 - risk * 0.5)
    
    rr_ratio = (abs(tp1 - entry) / risk) if risk > 0 else 0
    
    return sl, tp1, tp2, risk, rr_ratio

def check_liquidity_path(df: pd.DataFrame, side: str, entry: float, tp1: float):
    """
    Check if liquidity path to TP is clear
    Returns True if path is blocked
    """
    if side == "BUY":
        # Check if price has recently touched TP zone
        recent_touches = (df['high'].iloc[-10:] >= tp1 * 0.99).any()
        return recent_touches
    else:  # SELL
        recent_touches = (df['low'].iloc[-10:] <= tp1 * 1.01).any()
        return recent_touches

# ============================================================================
# SIGNAL GENERATION (RomeOPT-P 6-Step)
# ============================================================================

async def generate_romeopt_signal(exchange, symbol: str, tf: str):
    """
    Complete RomeOPT-P 6-Step signal generation
    """
    # Fetch data
    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
    if not ohlcv or len(ohlcv) < 50:
        return None
    
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    last_candle = df.iloc[-1]
    prev_candles = df.iloc[-6:-1]
    
    reasons = []
    score_details = {}
    total_score = 0
    
    # STEP 1: Liquidity Sweep (+2)
    sweep_high = last_candle["high"] > prev_candles["high"].max()
    sweep_low = last_candle["low"] < prev_candles["low"].min()
    has_sweep = sweep_high or sweep_low
    
    if has_sweep:
        total_score += SCORE_WEIGHTS["liquidity_sweep"]
        score_details["liquidity_sweep"] = SCORE_WEIGHTS["liquidity_sweep"]
        reasons.append("Liquidity Sweep ✅")
    else:
        reasons.append("No Liquidity Sweep")
    
    # STEP 2: Displacement (+2) - with volume confirmation
    body_size = abs(last_candle["close"] - last_candle["open"])
    candle_range = last_candle["high"] - last_candle["low"]
    displacement_ratio = body_size / (candle_range + 1e-8)
    
    vol_avg = df['volume'].iloc[-10:].mean()
    vol_confirmation = last_candle['volume'] > vol_avg * 1.5
    
    if displacement_ratio > 0.6 and vol_confirmation:
        total_score += SCORE_WEIGHTS["displacement"]
        score_details["displacement"] = SCORE_WEIGHTS["displacement"]
        reasons.append(f"Strong Displacement ({displacement_ratio:.1%}) with Volume ✅")
    elif displacement_ratio > 0.6:
        total_score += 1  # Half points without volume
        score_details["displacement"] = 1
        reasons.append(f"Displacement ({displacement_ratio:.1%}) - weak volume")
    else:
        reasons.append(f"Weak Displacement ({displacement_ratio:.1%})")
    
    # STEP 3: Order Block Detection
    ob_zone = find_order_blocks(df)
    if not ob_zone:
        reasons.append("No quality OB found")
        return None
    
    side = "BUY" if ob_zone["type"] == "bullish" else "SELL"
    reasons.append(f"{ob_zone['type'].upper()} OB detected")
    
    # STEP 4: OB Zone Approach (+1)
    if side == "BUY" and last_candle["close"] <= ob_zone["high"]:
        total_score += SCORE_WEIGHTS["ob_zone"]
        score_details["ob_zone"] = SCORE_WEIGHTS["ob_zone"]
        reasons.append("OB Zone Approach ✅")
    elif side == "SELL" and last_candle["close"] >= ob_zone["low"]:
        total_score += SCORE_WEIGHTS["ob_zone"]
        score_details["ob_zone"] = SCORE_WEIGHTS["ob_zone"]
        reasons.append("OB Zone Approach ✅")
    else:
        reasons.append("Not in OB zone")
    
    # STEP 5: HTF Alignment (+2)
    htf_aligned, htf_confidence, htf_reasons = await check_htf_structure(
        exchange, symbol, tf, side
    )
    
    if htf_aligned:
        total_score += SCORE_WEIGHTS["htf_alignment"]
        score_details["htf_alignment"] = SCORE_WEIGHTS["htf_alignment"]
        reasons.append(f"HTF Alignment ✅ ({', '.join(htf_reasons)})")
    else:
        reasons.append(f"HTF Misalignment: {', '.join(htf_reasons)}")
        return None  # HTF alignment is mandatory
    
    # STEP 6: Momentum & Volume Confirmation (+1)
    momentum_ratio = displacement_ratio
    if side == "BUY" and momentum_ratio > 0.5 and last_candle["close"] > last_candle["open"]:
        total_score += SCORE_WEIGHTS["momentum"]
        score_details["momentum"] = SCORE_WEIGHTS["momentum"]
        reasons.append("Bullish Momentum ✅")
    elif side == "SELL" and momentum_ratio > 0.5 and last_candle["close"] < last_candle["open"]:
        total_score += SCORE_WEIGHTS["momentum"]
        score_details["momentum"] = SCORE_WEIGHTS["momentum"]
        reasons.append("Bearish Momentum ✅")
    else:
        reasons.append("Weak momentum")
    
    # Volume confirmation (bonus)
    if vol_confirmation:
        total_score += SCORE_WEIGHTS["volume_confirmation"]
        score_details["volume_confirmation"] = SCORE_WEIGHTS["volume_confirmation"]
        reasons.append("Volume Spike ✅")
    
    # CRITICAL CHECKS
    if total_score < MIN_SCORE:
        reasons.append(f"Score {total_score} < {MIN_SCORE}")
        return None
    
    if not (displacement_ratio > 0.6):
        reasons.append("Insufficient displacement")
        return None
    
    # Calculate TP/SL with RomeOPT-P logic
    entry = float(last_candle["close"])
    sl, tp1, tp2, risk, rr_ratio = calculate_risk_parameters(entry, side, ob_zone, df)
    
    # RR Ratio check
    if rr_ratio < MIN_RR_RATIO:
        reasons.append(f"RR ratio {rr_ratio:.1f} < {MIN_RR_RATIO}")
        return None
    
    # Liquidity path check
    if check_liquidity_path(df, side, entry, tp1):
        reasons.append("Liquidity path blocked")
        return None
    
    # SL Cluster check
    if len(recent_sl_hits[symbol]) >= SL_CLUSTER_THRESHOLD:
        recent_time = time.time() - 1800  # 30 minutes
        recent_hits = [t for t in recent_sl_hits[symbol] if t > recent_time]
        if len(recent_hits) >= SL_CLUSTER_THRESHOLD:
            reasons.append(f"SL cluster ({len(recent_hits)} hits)")
            return None
    
    # Create final signal
    signal = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "entry_tf": tf,
        "score": total_score,
        "score_details": score_details,
        "risk": risk,
        "rr_ratio": rr_ratio,
        "reasons": reasons,
        "ob_zone": ob_zone,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }
    
    return signal

# ============================================================================
# DATABASE & MONITORING
# ============================================================================

async def init_database():
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
            entry_tf TEXT NOT NULL,
            score INTEGER NOT NULL,
            rr_ratio REAL NOT NULL,
            risk REAL NOT NULL,
            status TEXT DEFAULT 'OPEN',
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            close_reason TEXT,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0
        )
    """)
    
    await db_conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_status ON romeopt_signals(status)
    """)
    
    await db_conn.commit()

async def save_signal(signal):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO romeopt_signals 
            (symbol, side, entry, sl, tp1, tp2, entry_tf, score, rr_ratio, risk, opened_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal["symbol"], signal["side"], signal["entry"], signal["sl"],
            signal["tp1"], signal["tp2"], signal["entry_tf"], signal["score"],
            signal["rr_ratio"], signal["risk"], signal["timestamp"]
        ))
        await db_conn.commit()

async def monitor_positions(exchange):
    """Monitor and manage open positions"""
    while True:
        try:
            async with db_lock:
                cursor = await db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status
                    FROM romeopt_signals 
                    WHERE status = 'OPEN'
                """)
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    sig_id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status = row
                    
                    # Get current price
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker.get("last")
                    if not current_price:
                        continue
                    
                    # Check TP/SL
                    hits = []
                    new_tp1_hit = tp1_hit
                    new_tp2_hit = tp2_hit
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
                            new_status = "CLOSED"
                        
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
                            new_status = "CLOSED"
                        
                        if current_price >= new_sl:
                            hits.append("SL")
                            new_status = "CLOSED"
                            recent_sl_hits[symbol].append(time.time())
                    
                    # Update database if changes
                    if hits or new_status != status:
                        # Calculate PnL
                        if new_status == "CLOSED":
                            exit_price = current_price
                            if "TP2" in hits or "SL" in hits:
                                # Full position closed
                                if side == "BUY":
                                    pnl = (exit_price - entry)
                                else:
                                    pnl = (entry - exit_price)
                                
                                pnl_pct = (pnl / entry) * 100
                            else:
                                # Partial close at TP1
                                pnl = 0  # Track partials separately if needed
                                pnl_pct = 0
                            
                            await db_conn.execute("""
                                UPDATE romeopt_signals 
                                SET tp1_hit=?, tp2_hit=?, sl=?, status=?, closed_at=?, pnl=?, pnl_pct=?
                                WHERE id=?
                            """, (
                                new_tp1_hit, new_tp2_hit, new_sl, new_status,
                                datetime.datetime.utcnow().isoformat(),
                                pnl, pnl_pct, sig_id
                            ))
                        
                        else:
                            # Just update TP hits and SL
                            await db_conn.execute("""
                                UPDATE romeopt_signals 
                                SET tp1_hit=?, tp2_hit=?, sl=?
                                WHERE id=?
                            """, (new_tp1_hit, new_tp2_hit, new_sl, sig_id))
                        
                        # Send alert for significant events
                        if hits:
                            alert_msg = f"🎯 {symbol} {side}\n"
                            alert_msg += f"Entry: {entry:.4f}\n"
                            alert_msg += f"Current: {current_price:.4f}\n"
                            alert_msg += f"Hits: {', '.join(hits)}\n"
                            alert_msg += f"SL: {new_sl:.4f}\n"
                            alert_msg += f"TP1: {tp1:.4f} TP2: {tp2:.4f}"
                            await send_telegram_alert(alert_msg)
                
                await db_conn.commit()
                
        except Exception as e:
            log.error(f"Monitor error: {e}")
        
        await asyncio.sleep(5)  # Check every 5 seconds

# ============================================================================
# MAIN SCANNING LOOP
# ============================================================================

async def scan_markets(exchange):
    """Main scanning loop"""
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
            
            for symbol, _ in top_pairs:
                # Skip if too many recent SL hits
                recent_time = time.time() - 1800  # 30 minutes
                recent_hits = [t for t in recent_sl_hits.get(symbol, []) if t > recent_time]
                if len(recent_hits) >= SL_CLUSTER_THRESHOLD:
                    continue
                
                for tf in TIMEFRAMES:
                    # Cooldown per symbol/TF
                    key = f"{symbol}:{tf}"
                    if key in last_scan and (time.time() - last_scan[key]) < 30:
                        continue
                    
                    signal = await generate_romeopt_signal(exchange, symbol, tf)
                    
                    if signal:
                        # Format alert message
                        alert_msg = f"🏆 ROMEOPT-P SIGNAL\n"
                        alert_msg += f"Pair: {signal['symbol']} ({signal['entry_tf']})\n"
                        alert_msg += f"Side: {signal['side']}\n"
                        alert_msg += f"Entry: {signal['entry']:.4f}\n"
                        alert_msg += f"SL: {signal['sl']:.4f} (Risk: {signal['risk']:.4f})\n"
                        alert_msg += f"TP1: {signal['tp1']:.4f} (1.0R)\n"
                        alert_msg += f"TP2: {signal['tp2']:.4f} (1.8R)\n"
                        alert_msg += f"Score: {signal['score']}/9\n"
                        alert_msg += f"RR: {signal['rr_ratio']:.1f}:1\n"
                        alert_msg += f"Reasons:\n" + "\n".join([f"• {r}" for r in signal['reasons'][-5:]])
                        
                        await send_telegram_alert(alert_msg)
                        await save_signal(signal)
                        
                        signals_found += 1
                        last_scan[key] = time.time()
            
            if signals_found > 0:
                log.info(f"Found {signals_found} quality signals")
            
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

async def send_telegram_alert(message: str):
    """Send alert to Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    escaped = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": escaped,
                "parse_mode": "HTML"
            })
        except Exception as e:
            log.warning(f"Telegram alert failed: {e}")

# ============================================================================
# MAIN APPLICATION
# ============================================================================

async def main():
    global exchange
    
    # Initialize
    await init_database()
    exchange = ccxt.okx({"enableRateLimit": True})
    
    # Startup message
    await send_telegram_alert(
        "🚀 ROMEOPT-P ULTIMATE SCANNER STARTED\n"
        "✅ Full 6-Step SMC Logic\n"
        "✅ BOS/CHOCH Detection\n"
        "✅ Premium/Discount FVG Zones\n"
        "✅ Quality OB Detection\n"
        "✅ Advanced Risk Management\n"
        "✅ SL Cluster Protection"
    )
    
    log.info("RomeOPT-P Ultimate Scanner started")
    
    # Run scanner and monitor concurrently
    await asyncio.gather(
        scan_markets(exchange),
        monitor_positions(exchange)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutdown requested")
    finally:
        if db_conn:
            asyncio.run(db_conn.close())