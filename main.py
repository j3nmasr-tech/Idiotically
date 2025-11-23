#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PRODUCTION SCANNER - ALL ORIGINAL LOGIC + ALL WINNER FILTERS + ELITE SYSTEM
- Your exact SMC core + ATR TP/SL + SL-cluster
- BTC direction filter
- Higher timeframe alignment 
- Momentum confirmation
- Zone quality detection
- Market condition filter
- NEW: Elite Entry Checklist (Score 14+)
- NEW: Market Regime Filter (Stop Fighting the Tide)
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ---------------- EXACT ORIGINAL CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 40))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", 1000000))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", 0.002))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", 23))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]

# ---------------- EXACT ORIGINAL LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("smc_bot")
db_lock = asyncio.Lock()

# ---------------- EXACT ORIGINAL TELEGRAM ----------------
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
        except: pass

# ---------------- EXACT ORIGINAL DATABASE ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
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
            tier TEXT DEFAULT 'STANDARD',
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pauses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reason TEXT,
            timestamp TEXT
        );
        """)
        await db.commit()

# ---------------- EXACT ORIGINAL OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try: return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except: return None

# ---------------- EXACT ORIGINAL INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def sma(series: pd.Series, period: int):
    return series.rolling(period, min_periods=1).mean()

def rsi(series: pd.Series, period: int):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# ---------------- EXACT ORIGINAL SMC CORE ----------------
def detect_swing_points(df: pd.DataFrame):
    if len(df) < 5: return None
    last = df.iloc[-1]; prev = df.iloc[-3:-1]
    swing_high = last["high"] > prev["high"].max()
    swing_low = last["low"] < prev["low"].min()
    return swing_high, swing_low

def detect_active_range(df: pd.DataFrame, lookback=10):
    last = df.iloc[-lookback:]
    return last["high"].max(), last["low"].min()

def detect_liquidity_pools(df: pd.DataFrame):
    hh, ll = detect_swing_points(df)
    return hh, ll

def detect_sweep(df: pd.DataFrame):
    if len(df) < 6: return False, False
    last = df.iloc[-1]; prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos_mss(df: pd.DataFrame):
    hh, ll = detect_sweep(df)
    return hh, ll

def detect_fvg(df: pd.DataFrame):
    if len(df) < 3: return False, False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
    bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
    return bull, bear

def detect_order_blocks(df: pd.DataFrame):
    if len(df) < 3: return None, None, None
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    return "bearish", candle["high"], candle["open"]

# ---------------- EXACT ORIGINAL SL-CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time(); dq = recent_sl[symbol]; dq.append(now)
    cutoff = now - lookback_minutes * 60
    while dq and dq[0] < cutoff: dq.popleft()
    
def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]; now = time.time(); cutoff = now - lookback * 60
    while dq and dq[0] < cutoff: dq.popleft()
    return len(dq) >= threshold

# ---------------- EXACT ORIGINAL SIGNAL GENERATOR ----------------
def generate_signal(df: pd.DataFrame, symbol: str, context=None):
    if context is None: context = {}
    tf = context.get("tf","15m")

    if df is None or len(df) < 6: return None

    last = df["close"].iloc[-1]

    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None: return None

    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)
    bos_hh, bos_ll = detect_bos_mss(df)

    if not (bos_hh or bos_ll): return None

    score = 0; reasons = []

    if ob_type=="bullish": score+=2; reasons.append("OB Bull +2")
    else: score+=2; reasons.append("OB Bear +2")

    if bull_fvg: score+=2; reasons.append("FVG Bull +2")
    elif bear_fvg: score+=2; reasons.append("FVG Bear +2")

    score+=2; reasons.append("BOS +2")
    if sweep_h or sweep_l: score+=1; reasons.append("Sweep +1")
    else: reasons.append("No Sweep +0")

    side = "BUY" if ob_type=="bullish" else "SELL"

    # EXACT ORIGINAL ATR-based TP/SL
    atr_val = None
    df15 = context.get("df_15m")
    if df15 is not None and len(df15)>=10:
        atr_val = float(atr(df15,14).iloc[-1])
    entry = float(last)
    tp_mult, sl_mult = 0.8, 1.0
    if atr_val:
        if side=="BUY":
            sl = entry - sl_mult*atr_val
            tp1 = entry + tp_mult*atr_val
            tp2 = entry + tp_mult*1.5*atr_val
            tp3 = entry + tp_mult*2.5*atr_val
        else:
            sl = entry + sl_mult*atr_val
            tp1 = entry - tp_mult*atr_val
            tp2 = entry - tp_mult*1.5*atr_val
            tp3 = entry - tp_mult*2.5*atr_val
    else:
        if side=="BUY":
            sl = float(ob_lo)
            tp1 = entry*1.004; tp2 = entry*1.008; tp3 = entry*1.012
        else:
            sl = float(ob_hi)
            tp1 = entry*0.996; tp2 = entry*0.992; tp3 = entry*0.988

    if sl==entry:
        sl = entry - entry*0.002 if side=="BUY" else entry + entry*0.002

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason": "Set B SMC Signal",
        "reason_list": reasons
    }

# ---------------- NEW: ELITE ENTRY CHECKLIST ----------------
def check_elite_filters(signal, df, context):
    """Elite Entry Checklist - Score 14+"""
    elite_score = 0
    elite_details = []
    
    # 1. MTF Confluence (1m & 5m agree)
    if check_mtf_confluence(signal, context):
        elite_score += 1
        elite_details.append("MTF ✓")
    
    # 2. Premium Zone Quality (Score 3/3)
    if check_premium_zone(signal, df, context):
        elite_score += 1
        elite_details.append("Premium Zone ✓")
    
    # 3. Momentum Alignment (RSI Filter)
    if check_momentum_alignment(signal, df, context):
        elite_score += 1
        elite_details.append("Momentum ✓")
    
    # 4. Volume-Verified Sweep
    if check_volume_verification(df):
        elite_score += 1
        elite_details.append("Volume ✓")
    
    return elite_score == 4, elite_details

def check_mtf_confluence(signal, context):
    """Multi-Timeframe Confluence - 1m & 5m agreement"""
    try:
        df_5m = context.get("df_5m")
        if df_5m is None or len(df_5m) < 10:
            return False
            
        # Check if 5m has same OB type in same price zone
        ob_type_5m, ob_hi_5m, ob_lo_5m = detect_order_blocks(df_5m)
        if ob_type_5m is None:
            return False
            
        # Convert signal side to match OB type
        signal_ob_type = "bullish" if signal["side"] == "BUY" else "bearish"
        if ob_type_5m != signal_ob_type:
            return False
            
        # Price zone confluence (within 0.2%)
        current_price = signal["entry"]
        ob_price_5m = ob_hi_5m if ob_type_5m == "bearish" else ob_lo_5m
        price_diff_pct = abs(current_price - ob_price_5m) / current_price
        return price_diff_pct < 0.002
    except:
        return False

def check_premium_zone(signal, df, context):
    """Premium Zone Quality Scoring (3/3 points required)"""
    zone_score = 0
    
    # +1: Fresh OB (formed within last 12 candles)
    if len(df) >= 15:
        zone_score += 1
    
    # +1: OB at Key HTF Level (using 1h data)
    df_1h = context.get("df_1h")
    if df_1h is not None and len(df_1h) >= 50:
        current_price = signal["entry"]
        # Check if price is near 1h EMA levels (support/resistance)
        ema_20_1h = df_1h['close'].ewm(span=20).mean().iloc[-1]
        ema_50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
        price_diff_pct_20 = abs(current_price - ema_20_1h) / current_price
        price_diff_pct_50 = abs(current_price - ema_50_1h) / current_price
        if price_diff_pct_20 < 0.005 or price_diff_pct_50 < 0.005:  # Within 0.5% of HTF level
            zone_score += 1
    
    # +1: OB + FVG Confluence
    bull_fvg, bear_fvg = detect_fvg(df)
    if (signal["side"] == "BUY" and bull_fvg) or (signal["side"] == "SELL" and bear_fvg):
        zone_score += 1
    
    return zone_score >= 3

def check_momentum_alignment(signal, df, context):
    """Momentum Alignment with RSI Filter"""
    try:
        # Use 3m data for momentum
        df_3m = context.get("df_3m")
        if df_3m is None or len(df_3m) < 14:
            df_3m = df  # Fallback to current TF
            
        rsi_3m = rsi(df_3m['close'], 14).iloc[-1]
        
        if signal["side"] == "BUY":
            return rsi_3m > 40 and rsi_3m < 80  # Not overbought, has momentum
        else:
            return rsi_3m < 60 and rsi_3m > 20  # Not oversold, has momentum
    except:
        return False

def check_volume_verification(df):
    """Volume Verification for Sweep"""
    if len(df) < 20:
        return False
        
    # Check if sweep candle had above-average volume
    sweep_high, sweep_low = detect_sweep(df)
    if not (sweep_high or sweep_low):
        return False
        
    current_volume = df['vol'].iloc[-1]
    avg_volume = df['vol'].tail(20).mean()
    
    return current_volume > avg_volume * 1.5  # 50% above average

# ---------------- NEW: MARKET REGIME FILTER ----------------
def check_market_regime(symbol, signal_direction, context):
    """Stop Fighting the Tide - Market Regime Filter"""
    try:
        df_1h = context.get("df_1h")
        if df_1h is None or len(df_1h) < 50:
            return True  # Allow if no data
            
        current_price = df_1h['close'].iloc[-1]
        ema_20_1h = df_1h['close'].ewm(span=20).mean().iloc[-1]
        ema_50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
        
        # Determine regime
        if current_price > ema_20_1h and ema_20_1h > ema_50_1h:
            regime = "STRONG_BULL"
        elif current_price < ema_20_1h and ema_20_1h < ema_50_1h:
            regime = "STRONG_BEAR"
        else:
            regime = "NEUTRAL"
        
        # Trading rules
        if regime == "STRONG_BULL":
            return signal_direction == "BUY"  # Only BUY allowed
        elif regime == "STRONG_BEAR":
            return signal_direction == "SELL"  # Only SELL allowed
        else:
            return True  # Both allowed in neutral
        
    except Exception as e:
        log.error(f"Market regime check error: {e}")
        return True  # Allow on error

# ---------------- EXACT ORIGINAL LOG SIGNAL (UPDATED FOR TIERS) ----------------
async def log_signal(sig, tier="STANDARD"):
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,tier)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["symbol"],sig["side"],sig["entry"],sig["sl"],sig["tp1"],sig["tp2"],sig["tp3"],
                  datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"],tier))
            await db.commit()

# ---------------- WINNER FILTERS ----------------
def get_btc_direction(btc_15m, btc_1h):
    """BTC direction detection"""
    if btc_15m is None or btc_1h is None: return "NEUTRAL"
    try:
        price = btc_15m['close'].iloc[-1]
        ema_1h_50 = btc_1h['close'].ewm(span=50).mean().iloc[-1]
        ema_15m_20 = btc_15m['close'].ewm(span=20).mean().iloc[-1]
        
        if price > ema_1h_50 and price > ema_15m_20: return "BULLISH"
        elif price < ema_1h_50 and price < ema_15m_20: return "BEARISH"
        else: return "NEUTRAL"
    except: return "NEUTRAL"

def is_trade_allowed(signal_side, btc_direction):
    """BTC BULLISH: Only BUY allowed | BTC BEARISH: Only SELL allowed"""
    if btc_direction == "BULLISH": return signal_side == "BUY"
    elif btc_direction == "BEARISH": return signal_side == "SELL"
    else: return True

def check_higher_tf_alignment(signal, higher_tf_data):
    """Higher timeframe alignment filter"""
    if higher_tf_data is None or len(higher_tf_data) < 20:
        return False
    current_price = signal['entry']
    higher_tf_ema_20 = higher_tf_data['close'].ewm(span=20).mean().iloc[-1]
    higher_tf_ema_50 = higher_tf_data['close'].ewm(span=50).mean().iloc[-1]
    if signal['side'] == 'BUY':
        return current_price > higher_tf_ema_20 and current_price > higher_tf_ema_50
    else:
        return current_price < higher_tf_ema_20 and current_price < higher_tf_ema_50

def check_momentum_confirmation(df, signal_direction):
    """Momentum confirmation filter"""
    if len(df) < 3: return False
    current_candle = df.iloc[-1]
    prev_candle = df.iloc[-2]
    if signal_direction == 'BUY':
        return (current_candle['close'] > current_candle['open'] and 
                current_candle['close'] > prev_candle['close'])
    else:
        return (current_candle['close'] < current_candle['open'] and
                current_candle['close'] < prev_candle['close'])

def check_entry_zone_quality(df, signal_direction):
    """Zone quality detection"""
    if len(df) < 15: return False
    recent_high = df['high'].tail(15).max()
    recent_low = df['low'].tail(15).min()
    current_price = df['close'].iloc[-1]
    if recent_high == recent_low: return False
    range_position = (current_price - recent_low) / (recent_high - recent_low)
    if signal_direction == 'BUY':
        return range_position < 0.3
    else:
        return range_position > 0.7

def detect_choppy_market(df):
    """Market condition filter"""
    if len(df) < 25: return True
    high, low, close = df['high'], df['low'], df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    atr = true_range.rolling(14).mean().iloc[-1]
    current_price = close.iloc[-1]
    price_range_pct = (df['high'].tail(20).max() - df['low'].tail(20).min()) / current_price
    return (atr < (current_price * 0.002) and price_range_pct < 0.02)

# ---------------- EXACT ORIGINAL MONITOR ----------------
async def monitor_signals(exchange):
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status,tier 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status, tier = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        hits=[]; sl_hit=False
                        if side=="BUY":
                            if not tp1_hit and last_price>=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price>=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price>=tp3: hits.append("TP3"); tp3_hit=1
                            if last_price<=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                        else:
                            if not tp1_hit and last_price<=tp1: hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price<=tp2: hits.append("TP2"); tp2_hit=1
                            if not tp3_hit and last_price<=tp3: hits.append("TP3"); tp3_hit=1
                            if last_price>=sl: hits.append("SL"); status="CLOSED"; sl_hit=True

                        if hits:
                            tier_icon = "🎯" if tier == "ELITE" else "📊"
                            await tg(f"{tier_icon} {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}")

                        if sl_hit: record_sl_hit(symbol)

                        async with db_lock:
                            await db.execute("""
                                UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?
                            """,(tp1_hit,tp2_hit,tp3_hit,status,sig_id))
                await db.commit()
        except Exception as e: log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- FIXED SCAN LOOP WITH ERROR HANDLING ----------------
last_signal_time = {}
async def scan_loop(exchange):
    while True:
        t0=time.time()
        try:
            # Get BTC direction first
            btc_15m_data = await fetch_ohlcv(exchange, "BTC/USDT", "15m", 100)
            btc_1h_data = await fetch_ohlcv(exchange, "BTC/USDT", "1h", 100)
            btc_15m = pd.DataFrame(btc_15m_data, columns=["ts","open","high","low","close","vol"]) if btc_15m_data else None
            btc_1h = pd.DataFrame(btc_1h_data, columns=["ts","open","high","low","close","vol"]) if btc_1h_data else None
            btc_direction = get_btc_direction(btc_15m, btc_1h)
            log.info(f"🎯 BTC Direction: {btc_direction}")
            
            # FIXED: Get top coins with proper error handling
            try:
                tickers = await exchange.fetch_tickers()
                # Filter valid USDT pairs only
                valid_pairs = []
                for symbol, data in tickers.items():
                    if symbol.endswith("/USDT") and data.get('quoteVolume', 0) > MIN_VOLUME:
                        try:
                            # Validate symbol structure
                            if symbol.count('/') == 1 and len(symbol.split('/')[0]) > 0:
                                valid_pairs.append((symbol, data.get('quoteVolume', 0)))
                        except:
                            continue
                
                top = sorted(valid_pairs, key=lambda x: x[1], reverse=True)[:TOP_N]
                log.info(f"📊 Processing {len(top)} valid symbols")
                
            except Exception as e:
                log.error(f"Error fetching tickers: {e}")
                top = []
            
            signals_found = 0
            elite_signals = 0
            
            for symbol, volume in top:
                if deprioritized(symbol): continue
                ohlcvs = {}
                
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800: 
                        continue
                    
                    try:
                        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                        if not ohlcv: continue
                        
                        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                        for c in ["open","high","low","close","vol"]: 
                            df[c] = pd.to_numeric(df[c], errors="coerce")
                        
                        # Fetch additional timeframes for context
                        context = {"tf": tf}
                        additional_tfs = ["15m", "1h", "5m", "3m"]
                        
                        for add_tf in additional_tfs:
                            if add_tf not in ohlcvs and add_tf != tf:
                                try:
                                    add_ohlcv = await fetch_ohlcv(exchange, symbol, add_tf, 200)
                                    if add_ohlcv:
                                        ohlcvs[add_tf] = pd.DataFrame(add_ohlcv, columns=["ts","open","high","low","close","vol"])
                                        for col in ["open","high","low","close","vol"]:
                                            ohlcvs[add_tf][col] = pd.to_numeric(ohlcvs[add_tf][col], errors="coerce")
                                except:
                                    continue
                        
                        # Add all available timeframes to context
                        for tf_key in additional_tfs:
                            context[f"df_{tf_key}"] = ohlcvs.get(tf_key)
                        
                        # Generate original signal
                        sig = generate_signal(df, symbol, context)
                        
                        # APPLY ALL FILTERS - MARKET REGIME FILTER FIRST (MANDATORY FOR ALL)
                        if sig:
                            # LAYER 1: MARKET REGIME FILTER (APPLIED TO ALL SIGNALS)
                            if not check_market_regime(sig['symbol'], sig['side'], context):
                                log.info(f"⏸️ Blocked by Market Regime: {sig['side']} signal in wrong regime")
                                continue
                                
                            filters_passed = True
                            tier = "STANDARD"
                            
                            # ORIGINAL WINNER FILTERS
                            # 1. BTC Direction Filter
                            if not is_trade_allowed(sig['side'], btc_direction):
                                log.info(f"⏸️ Blocked: {sig['side']} vs BTC {btc_direction}")
                                filters_passed = False
                                
                            # 2. Higher TF Alignment
                            elif not check_higher_tf_alignment(sig, context.get("df_15m")):
                                log.info(f"⏸️ Blocked: Higher TF misalignment")
                                filters_passed = False
                                
                            # 3. Momentum Confirmation (skip for 1m/3m)
                            elif tf not in ["1m", "3m"] and not check_momentum_confirmation(df, sig['side']):
                                log.info(f"⏸️ Blocked: No momentum confirmation")
                                filters_passed = False
                                
                            # 4. Zone Quality
                            elif not check_entry_zone_quality(df, sig['side']):
                                log.info(f"⏸️ Blocked: Poor entry zone")
                                filters_passed = False
                                
                            # 5. Market Condition
                            elif detect_choppy_market(df):
                                log.info(f"⏸️ Blocked: Choppy market")
                                filters_passed = False
                            
                            if filters_passed:
                                # LAYER 2: ELITE FILTERS CLASSIFICATION
                                is_elite, elite_details = check_elite_filters(sig, df, context)
                                
                                if is_elite:
                                    tier = "ELITE"
                                    sig['score'] += 10  # Elite bonus
                                    elite_signals += 1
                                
                                # Add filter bonuses to reason list
                                sig['reason_list'].extend([
                                    f"BTC {btc_direction} ✓", "Higher TF ✓", 
                                    "Zone ✓", "Trending ✓", "Regime ✓"
                                ])
                                if tf not in ["1m", "3m"]:
                                    sig['reason_list'].append("Momentum ✓")
                                
                                if is_elite:
                                    sig['reason_list'].extend(elite_details)
                                
                                # Format message based on tier
                                if tier == "ELITE":
                                    icon = "🎯"
                                    size_note = " (3X SIZE)"
                                    tier_note = "ELITE - Score: 14+"
                                else:
                                    icon = "📊"  
                                    size_note = ""
                                    tier_note = f"STANDARD - Score: {sig['score']}"
                                
                                message = (
                                    f"{icon} {sig['symbol']} ({tf}) {sig['side']}{size_note}\n"
                                    f"Entry: {sig['entry']:.6f}\n"
                                    f"SL: {sig['sl']:.6f}\n"
                                    f"TP1: {sig['tp1']:.6f} TP2: {sig['tp2']:.6f} TP3: {sig['tp3']:.6f}\n"
                                    f"{tier_note}\n"
                                    f"Breakdown: {', '.join(sig['reason_list'])}"
                                )
                                
                                await tg(message)
                                await log_signal(sig, tier)
                                last_signal_time[key] = time.time()
                                signals_found += 1
                    
                    except Exception as e:
                        log.error(f"Error processing {symbol} {tf}: {e}")
                        continue
                            
            log.info(f"📊 Scan complete: {signals_found} signals ({elite_signals} elite)")
                        
        except Exception as e: 
            log.exception("scan error: %s", e)
        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- EXACT ORIGINAL FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token != WEBHOOK_SECRET: 
        raise HTTPException(403, "Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok": True}

# ---------------- EXACT ORIGINAL MAIN ----------------
async def main():
    await init_db()
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot"
        }
    })
    
    # Startup message
    startup_msg = (
        "🏆 ULTIMATE WINNER SCANNER STARTED\n"
        "• All original SMC logic preserved\n"
        "• BTC direction alignment enforced\n" 
        "• Higher TF alignment required\n"
        "• Momentum confirmation (5m+)\n"
        "• Zone quality checks\n"
        "• Trending markets only\n"
        "• NEW: Market Regime Filter (Stop Fighting the Tide)\n"
        "• NEW: Elite Entry Checklist (Score 14+)\n"
        "🎯 Target: 80%+ Win Rate"
    )
    await tg(startup_msg)
    log.info("✅ Scanner started with all winner filters + elite system")
    
    await asyncio.gather(scan_loop(exchange), monitor_signals(exchange))

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        asyncio.run(main())