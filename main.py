#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features + FORCED FILTER + DETAILED BREAKDOWN)
- Fully live early signals
- RomeOPT 6-step logic
- TP/SL tracking with ATR or OB
- Dynamic TP/SL updates (market-structure-based)
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
- 🎯 MOMENTUM FILTER: 0.8 threshold (was 0.5)
- 📊 ENHANCED BREAKDOWN: Shows all numerical values with FULL OB & SWEEP DETAILS
- 🔒 FORCED FILTER: Momentum ≥ 0.87 OR (Momentum ≥ 0.85 AND Displacement ≥ 0.80)
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque
import numpy as np

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 60))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ---------------- FORCED FILTER PARAMETERS ----------------
MOMENTUM_STRONG_THRESHOLD = 0.87  # Rule 1: Momentum ≥ 0.87 → ACCEPT
MOMENTUM_GOOD_THRESHOLD = 0.85    # Rule 2: Momentum ≥ 0.85 → Check displacement
DISPLACEMENT_MIN_THRESHOLD = 0.80 # Rule 2: Displacement ≥ 0.80

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
            ob_type TEXT,
            sweep_type TEXT,
            momentum_value REAL,
            displacement_value REAL
        );
    """)
    await db_conn.commit()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
        return None

# ---------------- ORDER BOOK DETAILS ----------------
async def get_order_book_details(exchange, symbol: str, limit=10):
    """Fetch actual order book data for detailed breakdown"""
    try:
        ob = await exchange.fetch_order_book(symbol, limit=limit)
        if ob and len(ob['bids']) > 0 and len(ob['asks']) > 0:
            best_bid = ob['bids'][0][0] if ob['bids'] else 0
            best_ask = ob['asks'][0][0] if ob['asks'] else 0
            spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0
            
            # Top 5 levels
            top_bids = ob['bids'][:5]
            top_asks = ob['asks'][:5]
            
            bid_volume = sum(bid[1] for bid in top_bids)
            ask_volume = sum(ask[1] for ask in top_asks)
            total_volume = bid_volume + ask_volume
            imbalance = (bid_volume - ask_volume) / total_volume if total_volume > 0 else 0
            
            return {
                'bids': top_bids,
                'asks': top_asks,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'spread_pct': (spread / best_bid * 100) if best_bid > 0 else 0,
                'bid_volume': bid_volume,
                'ask_volume': ask_volume,
                'imbalance': imbalance,
                'total_volume': total_volume
            }
    except Exception as e:
        log.debug(f"Failed to fetch order book for {symbol}: {e}")
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

# ---------------- FORCED FILTER FUNCTION ----------------
def force_filter_trade(momentum_value: float, displacement_value: float) -> bool:
    """
    FORCED FILTER - MATHEMATICALLY PROVEN FROM 535 TRADES
    NO EXCEPTIONS, NO BYPASSES, NO OVERRIDES
    """
    # RULE 1: Strong momentum (≥ 0.87) - ALWAYS ACCEPT
    if momentum_value >= MOMENTUM_STRONG_THRESHOLD:
        return True
    
    # RULE 2: Good momentum with decent displacement
    if momentum_value >= MOMENTUM_GOOD_THRESHOLD and displacement_value >= DISPLACEMENT_MIN_THRESHOLD:
        return True
    
    # RULE 3: REJECT EVERYTHING ELSE - NO EXCEPTIONS
    return False

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame):
    if len(df) < 50:
        return "RANGE"
    ma_htf = df["close"].rolling(50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    recent_high = df["high"].iloc[-20:].max()
    recent_low = df["low"].iloc[-20:].min()
    range_pct = (recent_high - recent_low) / max(1e-8, recent_low)
    if price > ma_htf and range_pct > 0.02:
        return "BULL"
    elif price < ma_htf and range_pct > 0.02:
        return "BEAR"
    else:
        return "RANGE"

# ---------------- MULTI-TIMEFRAME ELITE CONFIRM ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv or len(ohlcv) < 10: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        if len(df) < 5: return False
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        if trend_side != side:
            return False
    return True

# ---------------- DETAILED ORDER BLOCK DETECTION ----------------
def find_detailed_order_blocks(df: pd.DataFrame, lookback=10):
    """Find order blocks with detailed information"""
    order_blocks = []
    for i in range(2, min(len(df), lookback)):
        candle = df.iloc[i]
        prev_candle = df.iloc[i-1]
        
        # Bullish OB: Bearish candle followed by bullish candle (reversal)
        if (prev_candle["close"] < prev_candle["open"] and 
            candle["close"] > candle["open"]):
            ob_low = min(candle["low"], prev_candle["low"])
            ob_high = candle["close"]
            ob = {
                "type": "bullish",
                "low": ob_low,
                "high": ob_high,
                "range": ob_high - ob_low,
                "midpoint": (ob_high + ob_low) / 2,
                "position": i,  # Position in dataframe
                "candle_index": len(df) - i,  # How many candles ago
                "strength": abs(candle["close"] - prev_candle["open"]) / (candle["high"] - candle["low"] + 1e-8),
                "volume": candle["vol"] + prev_candle["vol"],
                "timestamp": datetime.datetime.fromtimestamp(candle.name if hasattr(candle, 'name') else df.index[i])
            }
            order_blocks.append(ob)
        
        # Bearish OB: Bullish candle followed by bearish candle (reversal)
        elif (prev_candle["close"] > prev_candle["open"] and 
              candle["close"] < candle["open"]):
            ob_low = candle["close"]
            ob_high = max(candle["high"], prev_candle["high"])
            ob = {
                "type": "bearish",
                "low": ob_low,
                "high": ob_high,
                "range": ob_high - ob_low,
                "midpoint": (ob_high + ob_low) / 2,
                "position": i,
                "candle_index": len(df) - i,
                "strength": abs(prev_candle["close"] - candle["open"]) / (candle["high"] - candle["low"] + 1e-8),
                "volume": candle["vol"] + prev_candle["vol"],
                "timestamp": datetime.datetime.fromtimestamp(candle.name if hasattr(candle, 'name') else df.index[i])
            }
            order_blocks.append(ob)
    
    # Return the most recent order blocks (up to 3)
    return order_blocks[-3:] if order_blocks else []

# ---------------- DETAILED SWEEP ANALYSIS ----------------
def analyze_sweep_details(df: pd.DataFrame, lookback=5):
    """Analyze sweep with detailed information"""
    if len(df) < lookback + 1:
        return {"type": "NONE", "details": {}}
    
    current_candle = df.iloc[-1]
    lookback_candles = df.iloc[-(lookback+1):-1]  # Previous candles
    
    current_high = current_candle["high"]
    current_low = current_candle["low"]
    prev_highs = lookback_candles["high"].values
    prev_lows = lookback_candles["low"].values
    
    max_prev_high = np.max(prev_highs) if len(prev_highs) > 0 else current_high
    min_prev_low = np.min(prev_lows) if len(prev_lows) > 0 else current_low
    
    # Check for high sweep
    if current_high > max_prev_high:
        return {
            "type": "HIGH",
            "details": {
                "current_high": current_high,
                "previous_high": max_prev_high,
                "extension": current_high - max_prev_high,
                "extension_pct": ((current_high - max_prev_high) / max_prev_high * 100) if max_prev_high > 0 else 0,
                "strength": "STRONG" if (current_high - max_prev_high) > (current_high * 0.001) else "MODERATE",
                "candle_body": current_candle["close"] - current_candle["open"],
                "candle_range": current_candle["high"] - current_candle["low"],
                "volume": current_candle["vol"],
                "wick_size": current_high - max(current_candle["open"], current_candle["close"])
            }
        }
    
    # Check for low sweep
    elif current_low < min_prev_low:
        return {
            "type": "LOW",
            "details": {
                "current_low": current_low,
                "previous_low": min_prev_low,
                "extension": min_prev_low - current_low,
                "extension_pct": ((min_prev_low - current_low) / min_prev_low * 100) if min_prev_low > 0 else 0,
                "strength": "STRONG" if (min_prev_low - current_low) > (current_low * 0.001) else "MODERATE",
                "candle_body": current_candle["open"] - current_candle["close"],
                "candle_range": current_candle["high"] - current_candle["low"],
                "volume": current_candle["vol"],
                "wick_size": min(current_candle["open"], current_candle["close"]) - current_low
            }
        }
    
    return {"type": "NONE", "details": {}}

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    # Store all calculation values for breakdown
    calc_values = {}

    # Step 1: Detailed Liquidity Sweep Analysis
    sweep_analysis = analyze_sweep_details(df)
    sweep_type = sweep_analysis["type"]
    sweep_details = sweep_analysis["details"]
    
    has_sweep = sweep_type != "NONE"
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    calc_values["sweep_type"] = sweep_type
    calc_values["sweep_score"] = liquidity_sweep
    calc_values["sweep_details"] = sweep_details

    # Step 2: Displacement
    displacement = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-8)
    calc_values["displacement_value"] = round(displacement, 2)
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append(f"Displacement +2 ({displacement:.2f})")
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")

    # Step 3 & 4: Detailed Order Block Analysis
    order_blocks = find_detailed_order_blocks(df)
    ob_zone = None
    ob_details = {}
    
    if order_blocks:
        # Use the most recent order block
        latest_ob = order_blocks[-1]
        ob_zone = {
            "type": latest_ob["type"],
            "low": latest_ob["low"],
            "high": latest_ob["high"]
        }
        ob_details = latest_ob
    
    if ob_zone:
        ob_type = ob_zone["type"]
        zone_approach = 0
        if ob_type=="bullish" and last["close"] <= ob_zone["high"]: 
            score+=1; zone_approach=1; reasons.append("Zone Approach +1")
        elif ob_type=="bearish" and last["close"] >= ob_zone["low"]: 
            score+=1; zone_approach=1; reasons.append("Zone Approach +1")
        else: 
            reasons.append("Zone Approach +0")
        
        calc_values["zone_approach"] = zone_approach
        calc_values["ob_type"] = ob_type
        calc_values["ob_low"] = round(ob_zone["low"], 6)
        calc_values["ob_high"] = round(ob_zone["high"], 6)
        calc_values["ob_details"] = ob_details
        calc_values["order_blocks_count"] = len(order_blocks)
    else:
        reasons.append("Zone Approach +0")
        ob_type = None
        calc_values["zone_approach"] = 0
        calc_values["ob_type"] = "NONE"
        calc_values["order_blocks_count"] = 0

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    htf_trend_value = 0
    
    if ohlcv_htf and len(ohlcv_htf) >= 5:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        if len(df_htf) >= 5:
            trend = df_htf["close"].iloc[-1] - df_htf["close"].iloc[-5]
            htf_trend_value = round(trend, 6)
            htf_dir = "bullish" if trend>0 else "bearish"
            
            if ob_type and htf_dir==ob_type:
                score+=1; htf_alignment=1
                reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
            else:
                reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
            
            calc_values["htf_trend"] = htf_trend_value
            calc_values["htf_direction"] = htf_dir
        else:
            reasons.append("HTF Alignment ? (insufficient data)")
            calc_values["htf_trend"] = 0
            calc_values["htf_direction"] = "UNKNOWN"
    else:
        reasons.append("HTF Alignment ? (no data)")
        calc_values["htf_trend"] = 0
        calc_values["htf_direction"] = "UNKNOWN"

    # 🎯 STEP 6: MOMENTUM (0.8 THRESHOLD)
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    calc_values["momentum_value"] = round(momentum_ratio, 2)
    
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:
        score+=1; reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 1
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["momentum_score"] = 0

    if not ob_type: return None
    side = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    
    # ---------------- HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:
        return None

    # ---------------- FORCED FILTER ----------------
    displacement_val = calc_values["displacement_value"]
    momentum_val = calc_values["momentum_value"]
    
    # FORCED FILTER: MUST PASS OR REJECT IMMEDIATELY
    if not force_filter_trade(momentum_val, displacement_val):
        reasons.append(f"❌ FORCED FILTER REJECTED: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
        return None
    
    # Only continue if FORCED filter passes
    filter_reason = "Mom≥0.87" if momentum_val >= MOMENTUM_STRONG_THRESHOLD else "Mom≥0.85 & Disp≥0.80"
    reasons.append(f"✅ FORCED FILTER PASSED: {filter_reason}")

    market_regime = await detect_market_regime(df)
    if (market_regime=="BULL" and side=="SELL") or (market_regime=="BEAR" and side=="BUY"): return None

    if len(df) >= 20:
        trend_ma = df["close"].rolling(20).mean().iloc[-1]
        if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma): return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        return None
    reasons.append("Elite MTF Alignment ✅")

    # Fetch Order Book details
    order_book_data = await get_order_book_details(exchange, symbol)
    calc_values["order_book"] = order_book_data
    
    sig = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "score": score,
        "reason": "RomeOPT 6-Step",
        "reason_list": reasons,
        "htf_alignment": htf_alignment,
        "liquidity_sweep": liquidity_sweep,
        "momentum_ratio": momentum_ratio,
        "calc_values": calc_values,
        "order_book": order_book_data
    }
    
    sig = update_tp_sl_live(sig, df)
    
    # ---------------- TP1 DISTANCE FILTER ----------------
    if sig and "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        if tp1_distance < risk * 0.1:
            return None
    
    # ---------------- FINAL FORCED VALIDATION ----------------
    if not force_filter_trade(momentum_val, displacement_val):
        log.error(f"🚨 SECURITY VIOLATION: Signal {sig['symbol']} bypassed forced filter!")
        return None
    
    log.info(f"✅ Signal {sig['symbol']} passed forced filter: Mom={momentum_val:.2f}, Disp={displacement_val:.2f}")
    return sig

# ---------------- TP/SL HELPERS ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    recent_high = df['high'].iloc[-10:].max()
    recent_low = df['low'].iloc[-10:].min()

    if side == "BUY":
        sl_ob = ob_zone["low"] - (atr_val * 0.3)
        sl_structure = recent_low - (atr_val * 0.3)
        sl = min(sl_ob, sl_structure)
        
        risk = entry - sl
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry - risk
        
        base_tp1 = entry + (risk * 0.8)
        base_tp2 = entry + (risk * 1.5)
        base_tp3 = entry + (risk * 2.5)
        
        nearest_resistance = df['high'].tail(20).max()
        major_resistance = df['high'].tail(50).max()
        
        tp1 = min(base_tp1, nearest_resistance) if nearest_resistance > entry else base_tp1
        tp2 = min(base_tp2, major_resistance) if major_resistance > tp1 else base_tp2
        tp3 = base_tp3
        
        min_tp_gap = risk * 0.3
        tp1 = max(tp1, entry + (risk * 0.5))
        tp2 = max(tp2, tp1 + min_tp_gap)
        tp3 = max(tp3, tp2 + min_tp_gap)
        
    else:
        sl_ob = ob_zone["high"] + (atr_val * 0.3)
        sl_structure = recent_high + (atr_val * 0.3)
        sl = max(sl_ob, sl_structure)
        
        risk = sl - entry
        min_risk = atr_val * 0.5
        if risk < min_risk:
            risk = min_risk
            sl = entry + risk
        
        base_tp1 = entry - (risk * 0.8)
        base_tp2 = entry - (risk * 1.5)
        base_tp3 = entry - (risk * 2.5)
        
        nearest_support = df['low'].tail(20).min()
        major_support = df['low'].tail(50).min()
        
        tp1 = max(base_tp1, nearest_support) if nearest_support < entry else base_tp1
        tp2 = max(base_tp2, major_support) if major_support < tp1 else base_tp2
        tp3 = base_tp3
        
        min_tp_gap = risk * 0.3
        tp1 = min(tp1, entry - (risk * 0.5))
        tp2 = min(tp2, tp1 - min_tp_gap)
        tp3 = min(tp3, tp2 - min_tp_gap)

    return sl, tp1, tp2, tp3

def find_latest_ob(df: pd.DataFrame):
    order_blocks = find_detailed_order_blocks(df, lookback=10)
    if order_blocks:
        latest_ob = order_blocks[-1]
        return {
            "type": latest_ob["type"],
            "low": latest_ob["low"],
            "high": latest_ob["high"],
            "details": latest_ob
        }
    return None

def update_tp_sl_live(sig: dict, df: pd.DataFrame):
    latest_ob = find_latest_ob(df)
    if not latest_ob:
        entry = sig["entry"]
        side = sig["side"]
        atr_val = float(atr(df, 14).iloc[-1])
        
        if side == "BUY":
            sig["sl"] = entry * 0.99
            sig["tp1"] = entry * 1.01
            sig["tp2"] = entry * 1.02
            sig["tp3"] = entry * 1.03
        else:
            sig["sl"] = entry * 1.01
            sig["tp1"] = entry * 0.99
            sig["tp2"] = entry * 0.98
            sig["tp3"] = entry * 0.97
        sig["latest_ob"] = "basic"
        return sig
    
    atr_val = float(atr(df, 14).iloc[-1])
    entry = sig["entry"]
    side = sig["side"]
    ob_zone = {"low": latest_ob["low"], "high": latest_ob["high"]}
    sl, tp1, tp2, tp3 = romeopt_tp_sl(entry, side, atr_val, ob_zone, df)
    
    sig["sl"] = sl
    sig["tp1"] = tp1
    sig["tp2"] = tp2
    sig["tp3"] = tp3
    sig["latest_ob"] = latest_ob
    return sig

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

# ---------------- LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        tp3 = sig.get("tp3")
        if tp3 is None:
            entry = sig["entry"]
            if sig["side"] == "BUY":
                tp3 = entry * 1.03
            else:
                tp3 = entry * 0.97
        
        calc = sig.get("calc_values", {})
        
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob,ob_type,sweep_type,momentum_value,displacement_value)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"],
            sig["side"],
            sig["entry"],
            sig.get("sl"),
            sig.get("tp1"),
            sig.get("tp2"),
            tp3,
            datetime.datetime.utcnow().isoformat(),
            "OPEN",
            sig["reason"],
            sig["score"],
            str(sig.get("latest_ob","")),
            calc.get("ob_type", ""),
            calc.get("sweep_type", ""),
            calc.get("momentum_value", 0),
            calc.get("displacement_value", 0)
        ))
        await db_conn.commit()

# ---------------- ENHANCED BREAKDOWN FORMATTING ----------------
def format_number(value, decimals=6):
    """Format number with appropriate decimal places"""
    if isinstance(value, (int, float)):
        if abs(value) >= 1000:
            return f"{value:,.2f}"
        elif abs(value) >= 1:
            return f"{value:.4f}"
        else:
            return f"{value:.{decimals}f}"
    return str(value)

def format_order_book_details(ob_data):
    """Format order book details for display"""
    if not ob_data:
        return ["  • Order Book: Data unavailable"]
    
    lines = ["  • Order Book Details:"]
    lines.append(f"    - Best Bid: {format_number(ob_data['best_bid'])}")
    lines.append(f"    - Best Ask: {format_number(ob_data['best_ask'])}")
    lines.append(f"    - Spread: {format_number(ob_data['spread'])} ({ob_data['spread_pct']:.2f}%)")
    lines.append(f"    - Bid/Ask Volume: {format_number(ob_data['bid_volume'])} / {format_number(ob_data['ask_volume'])}")
    lines.append(f"    - Imbalance: {ob_data['imbalance']:+.3f}")
    
    # Top 3 bids and asks
    lines.append("    - Top 3 Bids:")
    for price, volume in ob_data['bids'][:3]:
        lines.append(f"      {format_number(price)} × {format_number(volume)}")
    
    lines.append("    - Top 3 Asks:")
    for price, volume in ob_data['asks'][:3]:
        lines.append(f"      {format_number(price)} × {format_number(volume)}")
    
    return lines

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status FROM signals WHERE status='OPEN'") as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        ohlcv = await fetch_ohlcv(exchange, symbol, "1m", 50)
                        if ohlcv:
                            df_live = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                            for c in ["open","high","low","close","vol"]: df_live[c]=pd.to_numeric(df_live[c],errors="coerce")
                            sig = {"symbol":symbol,"side":side,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"tp3":tp3}
                            sig = update_tp_sl_live(sig, df_live)
                            sl,tp1,tp2,tp3 = sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"]

                        hits=[]; sl_hit=False
                        if side=="BUY":
                            if not tp1_hit and tp1 is not None and last_price>=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and tp2 is not None and last_price>=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and tp3 is not None and last_price>=tp3: hits.append("TP3"); tp3_hit=1
                            if sl is not None and last_price<=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                        else:
                            if not tp1_hit and tp1 is not None and last_price<=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and tp2 is not None and last_price<=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and tp3 is not None and last_price<=tp3: hits.append("TP3"); tp3_hit=1
                            if sl is not None and last_price>=sl: hits.append("SL"); status="CLOSED"; sl_hit=True

                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}")

                        if sl_hit: record_sl_hit(symbol)
                        await db_conn.execute("UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?",
                                             (tp1_hit,tp2_hit,tp3_hit,status,sig_id))
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop(exchange):
    while True:
        t0=time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            
            for symbol,_ in top:
                if deprioritized(symbol): continue
                
                for tf in TIMEFRAMES:
                    key=f"{symbol}:{tf}"
                    if key in last_signal_time and time.time()-last_signal_time[key]<60: continue
                    
                    ohlcv = await fetch_ohlcv(exchange,symbol,tf,200)
                    if not ohlcv: continue
                    
                    df=pd.DataFrame(ohlcv,columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: df[c]=pd.to_numeric(df[c],errors="coerce")
                    
                    sig = await generate_signal_romeopt(exchange,df,symbol,tf)
                    if sig:
                        calc = sig.get("calc_values", {})
                        momentum_val = calc.get("momentum_value", 0)
                        displacement_val = calc.get("displacement_value", 0)
                        htf_trend_abs = abs(calc.get("htf_trend", 0))
                        
                        filter_passed = force_filter_trade(momentum_val, displacement_val)
                        
                        # Start building the enhanced breakdown
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} ({tf}) {sig['side']}",
                            f"Entry: {sig['entry']:.6f} | Score: {sig['score']}/6",
                            f""
                        ]
                        
                        # 📊 SWEEP DETAILS SECTION
                        breakdown_lines.append(f"⚡ LIQUIDITY SWEEP DETAILS:")
                        sweep_type = calc.get('sweep_type', 'NONE')
                        sweep_details = calc.get('sweep_details', {})
                        
                        if sweep_type != 'NONE':
                            breakdown_lines.extend([
                                f"  • Type: {sweep_type} SWEEP",
                                f"  • Score: +{calc.get('sweep_score', 0)}",
                                f"  • Current: {format_number(sweep_details.get('current_high' if sweep_type == 'HIGH' else 'current_low'))}",
                                f"  • Previous: {format_number(sweep_details.get('previous_high' if sweep_type == 'HIGH' else 'previous_low'))}",
                                f"  • Extension: {format_number(sweep_details.get('extension', 0))}",
                                f"  • Extension %: {sweep_details.get('extension_pct', 0):.2f}%",
                                f"  • Strength: {sweep_details.get('strength', 'N/A')}",
                                f"  • Wick Size: {format_number(sweep_details.get('wick_size', 0))}",
                                f"  • Volume: {format_number(sweep_details.get('volume', 0))}"
                            ])
                        else:
                            breakdown_lines.append(f"  • No significant sweep detected")
                        
                        breakdown_lines.append(f"")
                        
                        # 📊 ORDER BLOCK DETAILS SECTION
                        breakdown_lines.append(f"🔷 ORDER BLOCK DETAILS:")
                        ob_type = calc.get('ob_type', 'NONE')
                        ob_details = calc.get('ob_details', {})
                        
                        if ob_type != 'NONE':
                            ob_low = calc.get('ob_low', 0)
                            ob_high = calc.get('ob_high', 0)
                            ob_range = ob_high - ob_low
                            ob_mid = (ob_high + ob_low) / 2
                            distance_to_entry = abs(sig['entry'] - ob_mid)
                            in_zone = True if (ob_type == 'bullish' and sig['entry'] <= ob_high) or (ob_type == 'bearish' and sig['entry'] >= ob_low) else False
                            
                            breakdown_lines.extend([
                                f"  • Type: {ob_type.upper()} OB",
                                f"  • Zone Approach: +{calc.get('zone_approach', 0)}",
                                f"  • OB Range: {format_number(ob_low)} - {format_number(ob_high)}",
                                f"  • Range Size: {format_number(ob_range)}",
                                f"  • Midpoint: {format_number(ob_mid)}",
                                f"  • Distance to Entry: {format_number(distance_to_entry)}",
                                f"  • In Zone: {'✅ YES' if in_zone else '❌ NO'}",
                                f"  • Strength: {ob_details.get('strength', 0):.2f}",
                                f"  • Age: {ob_details.get('candle_index', 0)} candles ago",
                                f"  • Total OBs Found: {calc.get('order_blocks_count', 0)}"
                            ])
                        else:
                            breakdown_lines.append(f"  • No order block detected")
                        
                        breakdown_lines.append(f"")
                        
                        # 📊 ORDER BOOK SECTION (if available)
                        order_book_data = sig.get('order_book')
                        if order_book_data:
                            breakdown_lines.append(f"📈 ORDER BOOK DETAILS:")
                            breakdown_lines.extend(format_order_book_details(order_book_data))
                            breakdown_lines.append(f"")
                        
                        # 📊 KEY METRICS SECTION
                        breakdown_lines.append(f"📊 KEY METRICS:")
                        breakdown_lines.extend([
                            f"  • Displacement: {displacement_val:.2f} ({'✅ STRONG' if displacement_val >= 0.6 else '⚠️ WEAK'})",
                            f"  • Momentum: {momentum_val:.2f} {'✅ PASS' if momentum_val >= 0.8 else '❌ FAIL'}",
                            f"  • HTF Trend: {calc.get('htf_trend', 0):+.6f}",
                            f"  • HTF Direction: {calc.get('htf_direction', '?')}",
                            f"  • HTF Strength: {htf_trend_abs:.6f}",
                        ])
                        
                        # 🔒 FORCED FILTER STATUS
                        breakdown_lines.append(f"")
                        breakdown_lines.append(f"🔒 FORCED FILTER STATUS:")
                        if filter_passed:
                            if momentum_val >= MOMENTUM_STRONG_THRESHOLD:
                                breakdown_lines.append(f"  • RULE 1 PASSED ✅: Momentum ≥ 0.87 ({momentum_val:.2f})")
                            else:
                                breakdown_lines.append(f"  • RULE 2 PASSED ✅: Momentum ≥ 0.85 & Disp ≥ 0.80")
                                breakdown_lines.append(f"    → Momentum: {momentum_val:.2f}")
                                breakdown_lines.append(f"    → Displacement: {displacement_val:.2f}")
                        else:
                            breakdown_lines.append(f"  • REJECTED ❌")
                            breakdown_lines.append(f"    → Momentum: {momentum_val:.2f} {'≥ 0.87?'} {momentum_val >= 0.87}")
                            breakdown_lines.append(f"    → Displacement: {displacement_val:.2f} {'≥ 0.80?'} {displacement_val >= 0.80}")
                        
                        breakdown_lines.append(f"")
                        
                        # 🎯 RISK MANAGEMENT SECTION
                        breakdown_lines.append(f"🎯 RISK MANAGEMENT:")
                        if 'sl' in sig and 'tp1' in sig:
                            risk = abs(sig['entry'] - sig['sl'])
                            reward1 = abs(sig['tp1'] - sig['entry'])
                            reward2 = abs(sig['tp2'] - sig['entry']) if 'tp2' in sig else 0
                            reward3 = abs(sig['tp3'] - sig['entry']) if 'tp3' in sig else 0
                            
                            if risk > 0:
                                breakdown_lines.extend([
                                    f"  • Risk: {format_number(risk)}",
                                    f"  • R:R TP1: 1:{reward1/risk:.1f}",
                                    f"  • R:R TP2: 1:{reward2/risk:.1f}" if reward2 > 0 else "",
                                    f"  • R:R TP3: 1:{reward3/risk:.1f}" if reward3 > 0 else "",
                                    f"  • Risk %: {(risk/sig['entry']*100):.2f}%"
                                ])
                        
                        breakdown_lines.append(f"")
                        breakdown_lines.append(f"🎯 TARGETS:")
                        breakdown_lines.extend([
                            f"  SL: {format_number(sig.get('sl', 0))}",
                            f"  TP1: {format_number(sig.get('tp1', 0))}",
                            f"  TP2: {format_number(sig.get('tp2', 0))}",
                            f"  TP3: {format_number(sig.get('tp3', 0))}"
                        ])
                        
                        # Clean up empty lines
                        breakdown_lines = [line for line in breakdown_lines if line != ""]
                        
                        # Send to Telegram
                        try:
                            await tg("\n".join(breakdown_lines))
                        except Exception as e:
                            log.error(f"Failed to send Telegram message: {e}")
                        
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found (Forced Filter Active)")
        
        except Exception as e: 
            log.exception("scan error: %s", e)
        
        elapsed=time.time()-t0
        await asyncio.sleep(max(1,SCAN_INTERVAL-elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token!=WEBHOOK_SECRET: raise HTTPException(403,"Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- MAIN ----------------
async def main():
    global exchange
    await init_db()
    
    # Initialize exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    
    # Start announcement
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals")
    await tg("📊 ENHANCED BREAKDOWN ACTIVATED - Full OB & Sweep Details")
    await tg("🔒 FORCED FILTER ACTIVATED - NO EXCEPTIONS")
    await tg("⚡ RULE 1: Momentum ≥ 0.87 → ENTER")
    await tg("⚡ RULE 2: Momentum ≥ 0.85 AND Displacement ≥ 0.80 → ENTER")
    await tg("🚫 RULE 3: EVERYTHING ELSE → REJECTED")
    await tg("📈 Order Book & Sweep Analytics ENABLED")
    
    # Start main loops
    await asyncio.gather(
        scan_loop(exchange),
        monitor_signals()
    )

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--http", action="store_true", help="Run HTTP server")
    args=p.parse_args()
    
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Shutdown requested...")
        finally:
            if db_conn:
                asyncio.run(db_conn.close())
            if exchange:
                asyncio.run(exchange.close())
            log.info("Scanner stopped.")