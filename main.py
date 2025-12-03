#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic (PRACTICAL VERSION with debugging)
- TP/SL tracking with ATR or OB
- Dynamic TP/SL updates (market-structure-based)
- Telegram alerts
- Async SQLite logging (with detailed JSON diagnostics)
- Filters: Score >=4, realistic conditions
"""

import os
import time
import asyncio
import logging
import datetime
import json
import math
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
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["15m", "30m", "1h"]  # Start with longer timeframes for better signals
MIN_SCORE = 4  # Reduced from 5 to 4
CRITICAL_FACTORS_MIN = 1  # Only require 1 critical factor

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None  # Global exchange instance

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
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
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
            details TEXT
        );
    """)
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if ohlcv and len(ohlcv) > 0:
            return ohlcv
    except Exception as e:
        log.debug(f"fetch_ohlcv failed for {symbol} {timeframe}: {e}")
    return None

# ---------------- INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

# ---------------- MULTI-TIMEFRAME ALIGNMENT (PRACTICAL) ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    """
    More practical: Require only 2/3 higher timeframes to align
    """
    tfs = ["15m", "1h", "4h"]
    alignments = 0
    total_checked = 0
    
    for tf in tfs:
        try:
            ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
            if not ohlcv or len(ohlcv) < 10:
                continue
                
            df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
            for col in ["open","high","low","close","vol"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Simple trend detection
            if len(df) >= 10:
                current_price = df["close"].iloc[-1]
                ma20 = df["close"].rolling(20).mean().iloc[-1]
                
                if side == "BUY":
                    if current_price > ma20:
                        alignments += 1
                else:  # SELL
                    if current_price < ma20:
                        alignments += 1
                
                total_checked += 1
                
        except Exception as e:
            log.debug(f"elite_tf_alignment error for {symbol} {tf}: {e}")
            continue
    
    # Require at least 2/3 alignments if we checked all, otherwise be lenient
    if total_checked >= 2:
        return alignments >= max(2, total_checked * 0.6)
    return True  # Be lenient if we can't check enough timeframes

# ---------------- BOS/CHOCH DETECTION (PRACTICAL) ----------------
def detect_bos_choch(df: pd.DataFrame, swing_lookback=15):
    """
    More practical BOS/CHOCH detection
    """
    res = {"has_bos": False, "bos_side": None, "has_choch": False, "choch_info": None}
    
    if len(df) < swing_lookback:
        return res
    
    # Calculate recent swing highs/lows
    tail = df.tail(swing_lookback)
    recent_high = tail['high'].max()
    recent_low = tail['low'].min()
    last_close = df['close'].iloc[-1]
    prev_close = df['close'].iloc[-2] if len(df) >= 2 else last_close
    
    # Calculate dynamic threshold (1% of range)
    price_range = recent_high - recent_low
    if price_range > 0:
        threshold = price_range * 0.01
    else:
        threshold = recent_high * 0.001  # Fallback
    
    # BOS Detection (more lenient)
    if last_close > recent_high - threshold and last_close > prev_close:
        res["has_bos"] = True
        res["bos_side"] = "BUY"
    elif last_close < recent_low + threshold and last_close < prev_close:
        res["has_bos"] = True
        res["bos_side"] = "SELL"
    
    # CHOCH Detection (EMA crossover)
    try:
        ema20 = df['close'].ewm(span=20, min_periods=1).mean()
        ema50 = df['close'].ewm(span=50, min_periods=1).mean()
        
        if len(df) >= 10:
            ema20_now = ema20.iloc[-1]
            ema50_now = ema50.iloc[-1]
            ema20_prev = ema20.iloc[-3]
            ema50_prev = ema50.iloc[-3]
            
            # Check for crossover
            if (ema20_now > ema50_now and ema20_prev <= ema50_prev) or \
               (ema20_now < ema50_now and ema20_prev >= ema50_prev):
                res["has_choch"] = True
                res["choch_info"] = {
                    "ema20": float(ema20_now),
                    "ema50": float(ema50_now),
                    "trend": "BULL" if ema20_now > ema50_now else "BEAR"
                }
    except Exception:
        pass
    
    # If no clear side from BOS, use CHOCH
    if res["bos_side"] is None and res["has_choch"] and res["choch_info"]:
        res["bos_side"] = "BUY" if res["choch_info"]["trend"] == "BULL" else "SELL"
    
    return res

# ---------------- VOLUME SPIKE (PRACTICAL) ----------------
def vol_spike(df: pd.DataFrame, idx=-1, factor=1.3, lookback=20):
    """
    More lenient volume spike detection
    """
    if len(df) < lookback:
        return True  # Not enough data, don't reject
        
    try:
        vol_series = df['vol'].tail(lookback)
        if vol_series.isnull().all():
            return True
            
        vol_avg = vol_series.mean()
        current_vol = float(df['vol'].iloc[idx])
        
        # Allow if volume is above average or if it's significantly higher than recent low
        if vol_avg > 0:
            return current_vol > vol_avg * factor
        else:
            # Check if volume is increasing
            if len(df) >= 3:
                prev_vol = float(df['vol'].iloc[idx-1])
                return current_vol > prev_vol * 1.2
    except Exception:
        pass
    
    return True  # Don't reject on error

# ---------------- FVG DETECTION (PRACTICAL) ----------------
def find_fvgs(df: pd.DataFrame, lookback=100):
    """
    Find Fair Value Gaps with more practical approach
    """
    fvgs = []
    n = len(df)
    if n < 10:
        return fvgs
    
    # Check last 50 candles for FVGs
    start_idx = max(0, n - min(lookback, n))
    
    for i in range(start_idx, n-2):
        if i+2 >= n:
            break
            
        c0 = df.iloc[i]    # First candle
        c1 = df.iloc[i+1]  # Gap candle
        c2 = df.iloc[i+2]  # Confirmation candle
        
        # Bullish FVG: c2 high < c0 low (gap up)
        if c2['high'] < c0['low']:
            # Verify it's not just a small gap
            gap_size = abs(c0['low'] - c2['high'])
            if gap_size > (c0['high'] - c0['low']) * 0.1:  # At least 10% of candle size
                fvgs.append({
                    "type": "bullish",
                    "low": float(c2['high']),
                    "high": float(c0['low']),
                    "idx": i,
                    "size": float(gap_size)
                })
        
        # Bearish FVG: c2 low > c0 high (gap down)
        elif c2['low'] > c0['high']:
            gap_size = abs(c2['low'] - c0['high'])
            if gap_size > (c0['high'] - c0['low']) * 0.1:
                fvgs.append({
                    "type": "bearish",
                    "low": float(c0['high']),
                    "high": float(c2['low']),
                    "idx": i,
                    "size": float(gap_size)
                })
    
    # Return only recent FVGs (last 20)
    return sorted(fvgs, key=lambda x: x['idx'], reverse=True)[:20]

# ---------------- ORDER BLOCK DETECTION (PRACTICAL) ----------------
def find_quality_order_block(df: pd.DataFrame, lookback=50):
    """
    Find quality order blocks with more practical approach
    """
    n = len(df)
    if n < 10:
        return None
    
    # Start from recent candles
    for i in range(n-3, max(1, n - lookback), -1):
        if i < 1 or i+1 >= n:
            continue
            
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Calculate candle properties
        candle_body = abs(candle['close'] - candle['open'])
        candle_range = candle['high'] - candle['low']
        
        if candle_range <= 0:
            continue
            
        body_ratio = candle_body / candle_range
        
        # Bullish Order Block: Strong bear candle followed by bullish reaction
        if (prev_candle['close'] < prev_candle['open'] and  # Prev was bearish
            candle['close'] > candle['open'] and             # Current is bullish
            body_ratio > 0.3 and                             # Has decent body
            candle['low'] <= prev_candle['low']):            # Takes out previous low
            
            # Check if next candle confirms (price moves up)
            if i+1 < n:
                next_candle = df.iloc[i+1]
                if next_candle['close'] > candle['close']:
                    return {
                        "type": "bullish",
                        "low": float(min(candle['low'], prev_candle['low'])),
                        "high": float(max(candle['close'], prev_candle['close'])),
                        "idx": i,
                        "strength": float(body_ratio)
                    }
        
        # Bearish Order Block: Strong bull candle followed by bearish reaction
        elif (prev_candle['close'] > prev_candle['open'] and  # Prev was bullish
              candle['close'] < candle['open'] and             # Current is bearish
              body_ratio > 0.3 and                             # Has decent body
              candle['high'] >= prev_candle['high']):          # Takes out previous high
            
            # Check if next candle confirms (price moves down)
            if i+1 < n:
                next_candle = df.iloc[i+1]
                if next_candle['close'] < candle['close']:
                    return {
                        "type": "bearish",
                        "low": float(min(candle['close'], prev_candle['close'])),
                        "high": float(max(candle['high'], prev_candle['high'])),
                        "idx": i,
                        "strength": float(body_ratio)
                    }
    
    return None

# ---------------- MARKET STRUCTURE SHIFT (PRACTICAL) ----------------
def confirm_market_structure_shift(df: pd.DataFrame, side: str):
    """
    More practical market structure shift detection
    """
    if len(df) < 15:
        return True  # Not enough data, don't reject
        
    # Simple structure detection using swing points
    highs = df['high'].values
    lows = df['low'].values
    
    # Find local highs and lows (simplified)
    local_highs = []
    local_lows = []
    
    for i in range(2, len(df)-2):
        if highs[i] >= highs[i-2] and highs[i] >= highs[i-1] and highs[i] >= highs[i+1] and highs[i] >= highs[i+2]:
            local_highs.append((i, highs[i]))
        if lows[i] <= lows[i-2] and lows[i] <= lows[i-1] and lows[i] <= lows[i+1] and lows[i] <= lows[i+2]:
            local_lows.append((i, lows[i]))
    
    # Check last 3 swing points
    if len(local_highs) >= 3 and len(local_lows) >= 3:
        recent_highs = sorted(local_highs[-3:], key=lambda x: x[0])
        recent_lows = sorted(local_lows[-3:], key=lambda x: x[0])
        
        if side == "BUY":
            # Check for higher lows
            if recent_lows[-1][1] > recent_lows[-2][1]:
                return True
        else:  # SELL
            # Check for lower highs
            if recent_highs[-1][1] < recent_highs[-2][1]:
                return True
    
    return True  # Default to True if can't determine

# ---------------- TP/SL CALCULATION ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    Calculate TP/SL levels
    """
    if atr_val <= 0:
        atr_val = entry * 0.01  # Default 1% if ATR fails
    
    # Get recent price extremes
    recent_high = df['high'].tail(20).max()
    recent_low = df['low'].tail(20).min()
    
    if side == "BUY":
        # Stop Loss
        if ob_zone and 'low' in ob_zone:
            sl = ob_zone['low'] - (atr_val * 0.5)
        else:
            sl = recent_low - (atr_val * 1.0)
        
        # Ensure minimum risk
        risk = entry - sl
        min_risk = atr_val * 0.5
        if risk < min_risk:
            sl = entry - min_risk
            risk = min_risk
        
        # Take Profit levels
        tp1 = entry + (risk * 1.0)
        tp2 = entry + (risk * 2.0)
        tp3 = entry + (risk * 3.0)
        
        # Adjust for nearby resistance
        resistance = df['high'].tail(50).max()
        if resistance > entry:
            tp1 = min(tp1, resistance * 0.98)
    
    else:  # SELL
        # Stop Loss
        if ob_zone and 'high' in ob_zone:
            sl = ob_zone['high'] + (atr_val * 0.5)
        else:
            sl = recent_high + (atr_val * 1.0)
        
        # Ensure minimum risk
        risk = sl - entry
        min_risk = atr_val * 0.5
        if risk < min_risk:
            sl = entry + min_risk
            risk = min_risk
        
        # Take Profit levels
        tp1 = entry - (risk * 1.0)
        tp2 = entry - (risk * 2.0)
        tp3 = entry - (risk * 3.0)
        
        # Adjust for nearby support
        support = df['low'].tail(50).min()
        if support < entry:
            tp1 = max(tp1, support * 1.02)
    
    return sl, tp1, tp2, tp3

# ---------------- SIGNAL GENERATION (PRACTICAL) ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    """
    Main function to generate a RomeOPT signal with practical validation
    """
    if df is None or len(df) < 50:
        log.debug(f"{symbol} {tf}: Insufficient data ({len(df) if df else 0} candles)")
        return None
    
    # Log data quality
    log.debug(f"{symbol} {tf}: Checking with {len(df)} candles, latest price: {df['close'].iloc[-1]}")
    
    # Step 1: Detect BOS/CHOCH (relaxed)
    ms_shift = detect_bos_choch(df)
    if not ms_shift["has_bos"] and not ms_shift["has_choch"]:
        log.debug(f"{symbol} {tf}: No BOS/CHOCH detected")
        return None
    
    side = ms_shift["bos_side"]
    if side is None:
        log.debug(f"{symbol} {tf}: Could not determine side from BOS/CHOCH")
        return None
    
    log.debug(f"{symbol} {tf}: Side determined as {side} from BOS/CHOCH")
    
    # Step 2: Volume spike (lenient)
    vol_ok = vol_spike(df)
    if not vol_ok:
        log.debug(f"{symbol} {tf}: Volume spike check failed")
        # Don't return None, just note it
    
    # Step 3: Find FVGs
    fvgs = find_fvgs(df)
    if not fvgs:
        log.debug(f"{symbol} {tf}: No FVGs found")
        # Don't return None, FVG is nice-to-have
    
    # Step 4: Quality OB (important but not mandatory)
    ob_zone = find_quality_order_block(df)
    if not ob_zone:
        log.debug(f"{symbol} {tf}: No quality OB found")
        # Don't return None, continue
    
    # Step 5: Market structure shift (lenient)
    mss_ok = confirm_market_structure_shift(df, side)
    if not mss_ok:
        log.debug(f"{symbol} {tf}: Market structure shift not confirmed")
        # Don't return None, continue
    
    # Step 6: Higher timeframe alignment (practical)
    elite_ok = await elite_tf_alignment(exchange, symbol, side)
    if not elite_ok:
        log.debug(f"{symbol} {tf}: Higher timeframe alignment failed")
        # Don't return None, continue
    
    # Entry price
    entry = df["close"].iloc[-1]
    
    # Calculate ATR for TP/SL
    atr_val = float(atr(df, 14).iloc[-1])
    if atr_val <= 0:
        atr_val = entry * 0.01
    
    # Get TP/SL levels
    sl, tp1, tp2, tp3 = romeopt_tp_sl(entry, side, atr_val, ob_zone, df)
    
    # Score calculation (more balanced)
    score = 0
    if ms_shift["has_bos"]: score += 2
    if ms_shift["has_choch"]: score += 1
    if vol_ok: score += 1
    if elite_ok: score += 2
    if fvgs: score += 1
    if mss_ok: score += 1
    if ob_zone: score += 2
    
    # Minimum score check
    if score < MIN_SCORE:
        log.debug(f"{symbol} {tf}: Score {score} below minimum {MIN_SCORE}")
        return None
    
    # Create signal
    signal = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason": f"RomeOPT signal on {tf}",
        "detailed": {
            "timeframe": tf,
            "bos": ms_shift.get("has_bos", False),
            "choch": ms_shift.get("has_choch", False),
            "fvgs": len(fvgs) if fvgs else 0,
            "ob_zone": bool(ob_zone),
            "elite_ok": elite_ok,
            "vol_ok": vol_ok,
            "mss_ok": mss_ok,
            "atr": atr_val,
            "risk_reward": round((tp1 - entry) / (entry - sl), 2) if side == "BUY" else round((entry - tp1) / (sl - entry), 2)
        },
        "reason_list": []
    }
    
    # Build reason list
    if ms_shift["has_bos"]: signal["reason_list"].append("BOS")
    if ms_shift["has_choch"]: signal["reason_list"].append("CHOCH")
    if vol_ok: signal["reason_list"].append("Volume")
    if fvgs: signal["reason_list"].append("FVG")
    if ob_zone: signal["reason_list"].append("OB")
    if mss_ok: signal["reason_list"].append("MSS")
    if elite_ok: signal["reason_list"].append("HTF_Align")
    
    log.info(f"✓ {symbol} {tf}: Generated {side} signal with score {score}")
    return signal

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        details_json = json.dumps(sig.get("detailed", {}), default=str)
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,details)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"],
            sig["side"],
            sig["entry"],
            sig.get("sl"),
            sig.get("tp1"),
            sig.get("tp2"),
            sig.get("tp3"),
            datetime.datetime.utcnow().isoformat(),
            "PENDING",
            sig.get("reason", ""),
            sig.get("score", 0),
            str(sig.get("ob_zone", "")),
            details_json
        ))
        await db_conn.commit()
        log.info(f"Logged signal for {sig['symbol']} to database")

# ---------------- SL CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time(); dq = recent_sl[symbol]; dq.append(now)
    cutoff = now - lookback_minutes*60
    while dq and dq[0]<cutoff: dq.popleft()

def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]; now=time.time(); cutoff=now-lookback*60
    while dq and dq[0]<cutoff: dq.popleft()
    return len(dq)>=threshold

# ---------------- UPDATE TP/SL LIVE ----------------
def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    """
    Update TP/SL based on latest market data
    """
    if df is None or len(df) < 20:
        return sig
    
    try:
        # Find latest order block
        latest_ob = find_quality_order_block(df)
        if not latest_ob:
            return sig
        
        # Calculate ATR
        atr_val = float(atr(df, 14).iloc[-1])
        if atr_val <= 0:
            atr_val = sig.get("entry", 0) * 0.01
        
        # Recalculate TP/SL
        entry = sig.get("entry_limit", sig.get("entry"))
        side = sig["side"]
        sl, tp1, tp2, tp3 = romeopt_tp_sl(entry, side, atr_val, latest_ob, df)
        
        # Update signal
        sig["sl"] = sl
        sig["tp1"] = tp1
        sig["tp2"] = tp2
        sig["tp3"] = tp3
        sig["latest_ob"] = latest_ob
        
    except Exception as e:
        log.debug(f"update_tp_sl_live error: {e}")
    
    return sig

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    """
    Monitor open signals for TP/SL hits
    """
    global exchange
    if exchange is None:
        log.error("Exchange not initialized in monitor_signals")
        return
        
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,details 
                    FROM signals 
                    WHERE status IN ('OPEN','PENDING')
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, details = row
                        tp1_hit = tp1_hit or 0
                        tp2_hit = tp2_hit or 0
                        tp3_hit = tp3_hit or 0

                        # Fetch current price
                        try:
                            ticker = await exchange.fetch_ticker(symbol)
                            last_price = ticker.get("last")
                            if last_price is None:
                                continue
                        except Exception as e:
                            log.debug(f"fetch_ticker failed for {symbol}: {e}")
                            continue

                        # Update TP/SL based on latest data
                        ohlcv = await fetch_ohlcv(exchange, symbol, "5m", 50)
                        if ohlcv:
                            df_live = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                            for col in ["open","high","low","close","vol"]:
                                df_live[col] = pd.to_numeric(df_live[col], errors="coerce")
                            
                            sig = {
                                "symbol": symbol,
                                "side": side,
                                "entry": entry,
                                "sl": sl,
                                "tp1": tp1,
                                "tp2": tp2,
                                "tp3": tp3
                            }
                            sig = update_tp_sl_live(sig, df_live)
                            sl, tp1, tp2, tp3 = sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"]

                        # Check for TP/SL hits
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

                        # Send alert if hits
                        if hits:
                            try:
                                diag = json.loads(details) if details else {}
                            except:
                                diag = {}
                            
                            alert_msg = (f"🎯 {symbol} {side} Update\n"
                                       f"Entry: {entry:.8f}\n"
                                       f"Last: {last_price:.8f}\n"
                                       f"Hits: {', '.join(hits)}\n"
                                       f"SL: {sl:.8f}\n"
                                       f"TP1: {tp1:.8f} TP2: {tp2:.8f} TP3: {tp3:.8f}\n"
                                       f"Status: {status}\n"
                                       f"Score: {diag.get('score', 'N/A')}")
                            await tg(alert_msg)

                        # Record SL hit
                        if sl_hit:
                            record_sl_hit(symbol)

                        # Update database
                        await db_conn.execute("""
                            UPDATE signals 
                            SET tp1_hit=?, tp2_hit=?, tp3_hit=?, status=? 
                            WHERE id=?
                        """, (tp1_hit, tp2_hit, tp3_hit, status, sig_id))
                
                await db_conn.commit()
                
        except Exception as e:
            log.exception(f"Monitor error: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    """
    Main scanning loop
    """
    while True:
        t0 = time.time()
        signals_found = 0
        
        try:
            # Fetch top volume symbols
            tickers = await exchange.fetch_tickers()
            usdt_pairs = [(s, v.get("quoteVolume", 0)) 
                         for s, v in tickers.items() 
                         if s.endswith("/USDT") or s.endswith("USDT")]
            
            if not usdt_pairs:
                log.warning("No USDT pairs found")
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            
            top = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
            log.info(f"Scanning top {len(top)} symbols by volume")
            
            for symbol, volume in top:
                # Skip if deprioritized
                if deprioritized(symbol):
                    log.debug(f"Skipping deprioritized {symbol}")
                    continue
                
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    
                    # Check cooldown
                    if key in last_signal_time and time.time() - last_signal_time[key] < 300:  # 5 min cooldown
                        continue
                    
                    log.debug(f"Checking {symbol} on {tf}")
                    
                    # Fetch OHLCV data
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv or len(ohlcv) < 100:
                        log.debug(f"Insufficient data for {symbol} {tf}")
                        continue
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    for col in ["open","high","low","close","vol"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Generate signal
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    
                    if sig:
                        # Prepare alert message
                        breakdown = ', '.join(sig.get('reason_list', []))
                        alert_msg = (f"🏆 {sig['symbol']} ({tf}) {sig['side']}\n"
                                   f"Entry: {sig['entry']:.8f}\n"
                                   f"SL: {sig.get('sl', 0):.8f}\n"
                                   f"TP1: {sig.get('tp1', 0):.8f} "
                                   f"TP2: {sig.get('tp2', 0):.8f} "
                                   f"TP3: {sig.get('tp3', 0):.8f}\n"
                                   f"Score: {sig['score']}\n"
                                   f"Breakdown: {breakdown}")
                        
                        # Send alert and log
                        await tg(alert_msg)
                        await log_signal(sig)
                        
                        # Update last signal time
                        last_signal_time[key] = time.time()
                        signals_found += 1
                        
                        log.info(f"Found signal for {sig['symbol']} on {tf}")
            
            log.info(f"📊 Scan complete: {signals_found} signals found")
            
        except Exception as e:
            log.exception(f"Scan error: {e}")
        
        # Sleep for remaining interval
        elapsed = time.time() - t0
        sleep_time = max(1, SCAN_INTERVAL - elapsed)
        await asyncio.sleep(sleep_time)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/")
async def root():
    return {"status": "RomeOPT Scanner is running"}

@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    data = await request.json()
    log.info(f"Webhook received: {data}")
    
    # Process webhook data if needed
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()}

# ---------------- CLEANUP FUNCTION ----------------
async def cleanup():
    """Cleanup resources properly"""
    global exchange, db_conn
    
    log.info("Cleaning up resources...")
    
    if exchange:
        try:
            await exchange.close()
            log.info("Exchange connection closed")
        except Exception as e:
            log.error(f"Error closing exchange: {e}")
    
    if db_conn:
        try:
            await db_conn.close()
            log.info("Database connection closed")
        except Exception as e:
            log.error(f"Error closing database: {e}")

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    
    # Initialize
    log.info("Starting RomeOPT Scanner...")
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "timeout": 30000,
        "rateLimit": 1000,
    })
    
    # Test connection
    try:
        await exchange.load_markets()
        log.info(f"Connected to {exchange.name}")
    except Exception as e:
        log.error(f"Failed to connect to exchange: {e}")
        await cleanup()
        return
    
    # Send startup message
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Practical Version")
    
    # Run scanner and monitor
    try:
        await asyncio.gather(
            scan_loop(exchange),
            monitor_signals()
        )
    except asyncio.CancelledError:
        log.info("Scanner tasks cancelled")
    except Exception as e:
        log.exception(f"Unexpected error in main: {e}")
    finally:
        await cleanup()

# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    import argparse
    import signal
    
    parser = argparse.ArgumentParser(description="RomeOPT Scanner")
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.debug("Debug logging enabled")
    
    if args.http:
        log.info("Starting HTTP server on port 9000")
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        # Setup signal handlers for graceful shutdown
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create task
        main_task = loop.create_task(main())
        
        # Signal handling
        def signal_handler(signame):
            log.info(f"Received signal {signame}, shutting down...")
            main_task.cancel()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s.name))
        
        try:
            loop.run_until_complete(main_task)
        except asyncio.CancelledError:
            log.info("Main task cancelled")
        except Exception as e:
            log.exception(f"Fatal error: {e}")
        finally:
            # Run cleanup one more time
            try:
                loop.run_until_complete(cleanup())
            except:
                pass
            
            # Close loop
            loop.close()
            log.info("Scanner shutdown complete")