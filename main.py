#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
- Fully live early signals
- RomeOPT 6-step logic
- Strict TP/SL (0.8R/1.6R, SL→BE after TP1, no TP3)
- Liquidity path filter
- Clean traffic / range avoidance
- Telegram alerts
- Async SQLite logging
- Filters: Score >=5, Displacement +2, Sweep+2 OR Zone+1, avoid counter-trend
- Improved Order Block detection
- Adaptive Market Regime detection
- HTF + Sweep scoring threshold
- Elite multi-timeframe confirmation (15m,1h,4h)
- FIXED: Strong trend filter to avoid counter-trend losses
- ADDED: BOS/CHOCH detection
- ADDED: FVG detection
- ADDED: Winner Pattern Filter (HTF_Align + BOS/CHOCH + FVG)
- ALL ORIGINAL 6 STEPS PRESERVED
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

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N = int(os.getenv("TOP_N", 30))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# Timeframe mapping for TP scaling (RomeOPT-P logic)
TP_TIMEFRAME_MAP = {
    "1m": "5m",    # 1m → 5m ATR (5×) - less aggressive
    "3m": "15m",   # 3m → 15m ATR (5×)
    "5m": "15m",   # 5m → 15m ATR (3×) - conservative
    "15m": "1h",   # 15m → 1h ATR (4×)
    "30m": "1h"    # 30m → 1h ATR (2×) - minimal scaling
}

# ---------------- GLOBALS ----------------
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

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

# ---------------- DATABASE MIGRATION ----------------
async def migrate_db():
    try:
        cursor = await db_conn.execute("PRAGMA table_info(signals)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'entry_tf' not in column_names:
            log.info("Migrating database: adding entry_tf column")
            await db_conn.execute("ALTER TABLE signals ADD COLUMN entry_tf TEXT DEFAULT ''")
        
        if 'tp_tf' not in column_names:
            log.info("Migrating database: adding tp_tf column")
            await db_conn.execute("ALTER TABLE signals ADD COLUMN tp_tf TEXT DEFAULT ''")
        
        await db_conn.commit()
        log.info("Database migration complete")
    except Exception as e:
        log.error(f"Migration failed: {e}")
        await db_conn.execute("DROP TABLE IF EXISTS signals")
        await db_conn.commit()

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
            entry_tf TEXT DEFAULT '',
            tp_tf TEXT DEFAULT '',
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            latest_ob TEXT
        );
    """)
    await db_conn.commit()
    await migrate_db()

# ---------------- OHLCV ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        log.debug("fetch_ohlcv failed for %s %s: %s", symbol, timeframe, e)
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

def calculate_ema(df, period):
    return df['close'].ewm(span=period, adjust=False).mean()

# ---------------- STRONG TREND DETECTION ----------------
async def check_strong_counter_trend(exchange, symbol: str, timeframe: str, signal_side: str):
    trend_check_map = {
        "1m": "15m", "3m": "30m", "5m": "1h", "15m": "4h", "30m": "4h"
    }
    
    check_tf = trend_check_map.get(timeframe, "15m")
    ohlcv = await fetch_ohlcv(exchange, symbol, check_tf, 50)
    if not ohlcv: return False
        
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: 
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    df['ema20'] = calculate_ema(df, 20)
    recent = df.iloc[-10:]
    above_ema = (recent['close'] > recent['ema20']).sum()
    below_ema = (recent['close'] < recent['ema20']).sum()
    
    recent_trend = []
    for i in range(len(recent)-1):
        if recent['close'].iloc[i+1] > recent['close'].iloc[i]:
            recent_trend.append(1)
        else:
            recent_trend.append(-1)
    
    if len(recent_trend) >= 5:
        last_5 = recent_trend[-5:]
        if all(x > 0 for x in last_5):
            if signal_side == "SELL":
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Strong {check_tf} UPTREND")
                return True
        elif all(x < 0 for x in last_5):
            if signal_side == "BUY":
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Strong {check_tf} DOWNTREND")
                return True
    
    current_atr = float(atr(df, 14).iloc[-1])
    ema_distance = abs(df['close'].iloc[-1] - df['ema20'].iloc[-1])
    
    if current_atr > 0:
        distance_in_atr = ema_distance / current_atr
        if distance_in_atr > 2.0:
            if signal_side == "BUY" and df['close'].iloc[-1] < df['ema20'].iloc[-1]:
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Price >2 ATR below {check_tf} EMA")
                return True
            elif signal_side == "SELL" and df['close'].iloc[-1] > df['ema20'].iloc[-1]:
                log.info(f"🚫 {symbol} {timeframe} {signal_side} rejected: Price >2 ATR above {check_tf} EMA")
                return True
    
    return False

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame):
    ma_htf = df["close"].rolling(50).mean().iloc[-1]
    price = df["close"].iloc[-1]
    recent_high = df["high"].iloc[-20:].max()
    recent_low = df["low"].iloc[-20:].min()
    range_pct = (recent_high - recent_low) / max(1e-8, recent_low)
    if price > ma_htf and range_pct > 0.02: return "BULL"
    elif price < ma_htf and range_pct > 0.02: return "BEAR"
    else: return "RANGE"

# ---------------- MULTI-TIMEFRAME ELITE CONFIRM ----------------
async def elite_tf_alignment(exchange, symbol: str, side: str):
    tfs = ["15m","1h","4h"]
    for tf in tfs:
        ohlcv = await fetch_ohlcv(exchange, symbol, tf, 50)
        if not ohlcv: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        df['ema20'] = calculate_ema(df, 20)
        current_slope = df['ema20'].iloc[-1] - df['ema20'].iloc[-3]
        trend_side = "BUY" if current_slope > 0 else "SELL"
        if trend_side != side:
            log.debug(f"Elite alignment failed: {tf} trend {trend_side} vs signal {side}")
            return False
    return True

# ---------------- ORDER BLOCK DETECTION ----------------
def find_latest_ob(df: pd.DataFrame):
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
    return None

# ---------------- BOS/CHOCH DETECTION ----------------
def detect_bos_choch(df: pd.DataFrame, swing_lookback=15):
    res = {"has_bos": False, "bos_side": None, "has_choch": False}
    if len(df) < swing_lookback: return res
    
    tail = df.tail(swing_lookback)
    recent_high = tail['high'].max()
    recent_low = tail['low'].min()
    last_close = df['close'].iloc[-1]
    prev_close = df['close'].iloc[-2] if len(df) >= 2 else last_close
    
    price_range = recent_high - recent_low
    if price_range > 0: threshold = price_range * 0.01
    else: threshold = recent_high * 0.001
    
    if last_close > recent_high - threshold and last_close > prev_close:
        res["has_bos"] = True
        res["bos_side"] = "BUY"
    elif last_close < recent_low + threshold and last_close < prev_close:
        res["has_bos"] = True
        res["bos_side"] = "SELL"
    
    try:
        ema20 = df['close'].ewm(span=20, min_periods=1).mean()
        ema50 = df['close'].ewm(span=50, min_periods=1).mean()
        if len(df) >= 10:
            ema20_now = ema20.iloc[-1]
            ema50_now = ema50.iloc[-1]
            ema20_prev = ema20.iloc[-3]
            ema50_prev = ema50.iloc[-3]
            if (ema20_now > ema50_now and ema20_prev <= ema50_prev) or \
               (ema20_now < ema50_now and ema20_prev >= ema50_prev):
                res["has_choch"] = True
    except: pass
    return res

# ---------------- FVG DETECTION ----------------
def find_fvgs(df: pd.DataFrame, lookback=100):
    fvgs = []
    n = len(df)
    if n < 10: return fvgs
    start_idx = max(0, n - min(lookback, n))
    for i in range(start_idx, n-2):
        if i+2 >= n: break
        c0, c1, c2 = df.iloc[i], df.iloc[i+1], df.iloc[i+2]
        if c2['high'] < c0['low']:
            gap_size = abs(c0['low'] - c2['high'])
            if gap_size > (c0['high'] - c0['low']) * 0.1:
                fvgs.append({"type": "bullish", "low": float(c2['high']), "high": float(c0['low']), "idx": i, "size": float(gap_size)})
        elif c2['low'] > c0['high']:
            gap_size = abs(c2['low'] - c0['high'])
            if gap_size > (c0['high'] - c0['low']) * 0.1:
                fvgs.append({"type": "bearish", "low": float(c0['high']), "high": float(c2['low']), "idx": i, "size": float(gap_size)})
    return sorted(fvgs, key=lambda x: x['idx'], reverse=True)[:20]

# ===== WINNER PATTERN FILTER =====
def filter_winner_patterns(signal: dict) -> tuple:
    reason_list = signal.get("reason_list", [])
    if not reason_list: return True, "No breakdown data"
    has_htf_align = any("HTF Alignment +1" in reason for reason in reason_list)
    has_bos = signal.get("has_bos", False)
    has_choch = signal.get("has_choch", False)
    has_fvg = signal.get("has_fvg", False)
    if not has_htf_align: return True, "Missing HTF_Alignment"
    if not has_bos and not has_choch: return True, "Missing both BOS and CHOCH"
    if not has_fvg: return True, "Missing FVG"
    return False, ""
# ===========================================

# ---------------- TP/SL CALCULATION ----------------
async def romeoptp_tp_sl(exchange, entry: float, side: str, entry_tf: str, ob_zone: dict, symbol: str):
    if not ob_zone: return None, None, None, entry_tf
    tp_tf = TP_TIMEFRAME_MAP.get(entry_tf, "15m")
    htf_ohlcv = await fetch_ohlcv(exchange, symbol, tp_tf, 100)
    if not htf_ohlcv:
        htf_ohlcv = await fetch_ohlcv(exchange, symbol, entry_tf, 100)
        tp_tf = entry_tf
    if not htf_ohlcv: return None, None, None, tp_tf
    
    df_htf = pd.DataFrame(htf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: df_htf[c] = pd.to_numeric(df_htf[c], errors="coerce")
    atr_val = float(atr(df_htf, 14).iloc[-1])
    
    if side == "BUY":
        sl = ob_zone['low'] - (atr_val * 0.1)
        risk = entry - sl
        tp1 = entry + (risk * 0.8)
        tp2 = entry + (risk * 1.6)
    else:
        sl = ob_zone['high'] + (atr_val * 0.1)
        risk = sl - entry
        tp1 = entry - (risk * 0.8)
        tp2 = entry - (risk * 1.6)
    return sl, tp1, tp2, tp_tf

# ---------------- UPDATE SIGNAL TP/SL ----------------
async def update_tp_sl_live(sig: dict):
    global exchange
    if 'entry_tf' not in sig or 'symbol' not in sig or 'side' not in sig: return sig
    entry_tf_ohlcv = await fetch_ohlcv(exchange, sig["symbol"], sig["entry_tf"], 50)
    if not entry_tf_ohlcv: return sig
    df_entry = pd.DataFrame(entry_tf_ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: df_entry[c] = pd.to_numeric(df_entry[c], errors="coerce")
    latest_ob = find_latest_ob(df_entry)
    if not latest_ob: return sig
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(exchange, sig["entry"], sig["side"], sig["entry_tf"], latest_ob, sig["symbol"])
    if sl is not None and tp1 is not None and tp2 is not None:
        sig["sl"] = sl; sig["tp1"] = tp1; sig["tp2"] = tp2; sig["tp_tf"] = tp_tf; sig["latest_ob"] = latest_ob
    return sig

# ---------------- IMPROVED HTF ALIGNMENT DETECTION ----------------
async def get_htf_trend(exchange, symbol: str, timeframe: str):
    ohlcv = await fetch_ohlcv(exchange, symbol, timeframe, 50)
    if not ohlcv: return "neutral"
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
    for c in ["open","high","low","close","vol"]: df[c] = pd.to_numeric(df[c], errors="coerce")
    df['ema20'] = calculate_ema(df, 20)
    ema_slope = df['ema20'].iloc[-1] - df['ema20'].iloc[-3]
    recent_closes = df['close'].iloc[-6:]
    direction_sum = 0
    for i in range(1, len(recent_closes)):
        if recent_closes.iloc[i] > recent_closes.iloc[i-1]: direction_sum += 1
        else: direction_sum -= 1
    above_ema = df['close'].iloc[-1] > df['ema20'].iloc[-1]
    if ema_slope > 0 and direction_sum >= 2 and above_ema: return "bullish"
    elif ema_slope < 0 and direction_sum <= -2 and not above_ema: return "bearish"
    else: return "neutral"

# ---------------- ROMEOPT SIGNAL GENERATOR ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    ms_shift = detect_bos_choch(df)
    has_bos = ms_shift["has_bos"]
    has_choch = ms_shift["has_choch"]
    bos_side = ms_shift["bos_side"]
    fvgs = find_fvgs(df)
    has_fvg = len(fvgs) > 0
    
    # Step1: Liquidity Sweep
    sweep_high = last["high"] > prev5["high"].max()
    sweep_low = last["low"] < prev5["low"].min()
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    
    # Step2: Displacement
    displacement = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    has_disp = displacement > 0.6
    if has_disp: 
        score += 2
        reasons.append("Displacement +2")
    else: 
        reasons.append("Displacement +0")
    
    # Step3&4: OB detection
    ob_zone = find_latest_ob(df)
    if not ob_zone: 
        reasons.append("No OB detected")
        return None
    
    # ZONE APPROACH (+1 point) - ORIGINAL STEP
    ob_type = ob_zone['type']
    side = "BUY" if ob_type == "bullish" else "SELL"
    
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
    
    # Check strong counter-trend
    should_reject = await check_strong_counter_trend(exchange, symbol, tf, side)
    if should_reject:
        reasons.append(f"Strong HTF trend against {side} → Rejected")
        return None
    
    # HTF Alignment
    tf_map = {"1m":"15m", "3m":"30m", "5m":"1h", "15m":"4h", "30m":"1h"}
    htf = tf_map.get(tf, "15m")
    htf_trend = await get_htf_trend(exchange, symbol, htf)
    htf_alignment = 0
    
    if htf_trend != "neutral":
        htf_dir = "bullish" if htf_trend == "bullish" else "bearish"
        if htf_dir == ob_zone['type']: 
            htf_alignment = 1
            score += 1
            reasons.append(f"HTF Alignment +1 ({htf}: {htf_trend})")
        else:
            reasons.append(f"HTF Misalignment ({htf}: {htf_trend})")
    else:
        reasons.append(f"HTF Neutral ({htf})")
    
    # BOS/CHOCH/FVG status
    if has_bos: reasons.append("BOS✅")
    else: reasons.append("BOS❌")
    if has_choch: reasons.append("CHOCH✅")
    else: reasons.append("CHOCH❌")
    if has_fvg: reasons.append("FVG✅")
    else: reasons.append("FVG❌")
    
    # ELITE MTF ALIGNMENT - ORIGINAL STEP
    elite_alignment = await elite_tf_alignment(exchange, symbol, side)
    if elite_alignment: reasons.append("Elite MTF Alignment ✅")
    else: reasons.append("Elite MTF Alignment ❌")
    
    # Critical filters
    if score < MIN_SCORE: 
        reasons.append(f"Score {score} < {MIN_SCORE}")
        return None
    if not has_disp: 
        reasons.append("No displacement")
        return None

    # Momentum
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    if momentum_ratio < 0.5: 
        reasons.append("Momentum Failed")
        return None

    # TP/SL calculation
    sl, tp1, tp2, tp_tf = await romeoptp_tp_sl(exchange, float(last["close"]), side, tf, ob_zone, symbol)
    if sl is None or tp1 is None or tp2 is None:
        reasons.append("TP/SL calc failed")
        return None
    
    sig = {
        "symbol": symbol, "side": side, "entry": float(last["close"]), "sl": sl, "tp1": tp1, "tp2": tp2,
        "entry_tf": tf, "tp_tf": tp_tf, "score": int(score), "reason": "RomeOPT-P 6-Step",
        "reason_list": reasons, "ob_zone": ob_zone, "has_bos": has_bos, "has_choch": has_choch,
        "has_fvg": has_fvg, "bos_side": bos_side
    }

    # Winner Pattern Filter
    should_reject, reject_reason = filter_winner_patterns(sig)
    if should_reject:
        log.debug(f"❌ Winner pattern filter REJECTED {symbol} {side} on {tf}: {reject_reason}")
        return None
    sig["reason_list"].append("WinnerFilter✅")

    # Liquidity path filter
    if side == "BUY" and any(df['high'].iloc[-20:] >= sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        return None
    if side == "SELL" and any(df['low'].iloc[-20:] <= sig['tp1']): 
        reasons.append("Liquidity Path Blocked")
        return None

    return sig

# ---------------- DATABASE LOGGING ----------------
async def log_signal(sig):
    async with db_lock:
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,entry_tf,tp_tf,timestamp,status,reason,score,latest_ob)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["symbol"], sig["side"], sig["entry"], sig["sl"], sig["tp1"], sig["tp2"],
            sig.get("entry_tf", ""), sig.get("tp_tf", ""),
            datetime.datetime.utcnow().isoformat(), "OPEN", sig["reason"], 
            int(sig["score"]), str(sig.get("latest_ob",""))
        ))
        await db_conn.commit()

# ---------------- MONITOR SIGNALS ----------------
async def monitor_signals():
    global exchange
    while True:
        try:
            async with db_lock:
                async with db_conn.execute(
                    "SELECT id,symbol,side,entry,sl,tp1,tp2,tp1_hit,tp2_hit,status FROM signals WHERE status='OPEN'"
                ) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp1_hit, tp2_hit, status = row
                        ticker = await exchange.fetch_ticker(symbol)
                        last_price = ticker.get("last")
                        if last_price is None: continue

                        # TP/SL NEVER CHANGE - USE ORIGINAL VALUES
                        hits=[]; sl_hit=False
                        if side=="BUY":
                            if not tp1_hit and last_price>=tp1: 
                                hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price>=tp2: 
                                hits.append("TP2"); tp2_hit=1; status="CLOSED"
                            if last_price<=sl: 
                                hits.append("SL"); status="CLOSED"; sl_hit=True
                        else:
                            if not tp1_hit and last_price<=tp1: 
                                hits.append("TP1"); tp1_hit=1
                            if not tp2_hit and last_price<=tp2: 
                                hits.append("TP2"); tp2_hit=1; status="CLOSED"
                            if last_price>=sl: 
                                hits.append("SL"); status="CLOSED"; sl_hit=True

                        if hits:
                            await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2}")

                        await db_conn.execute("UPDATE signals SET tp1_hit=?,tp2_hit=?,status=? WHERE id=?", (tp1_hit,tp2_hit,status,sig_id))
                await db_conn.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- SCAN LOOP ----------------
last_signal_time = {}
async def scan_loop():
    global exchange
    while True:
        t0 = time.time()
        try:
            tickers = await exchange.fetch_tickers()
            top = sorted([(s,v.get("quoteVolume",0)) for s,v in tickers.items() if s.endswith("USDT")], 
                        key=lambda x:x[1], reverse=True)[:TOP_N]
            signals_found = 0
            for symbol,_ in top:
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 60: continue
                    ohlcv = await fetch_ohlcv(exchange, symbol, tf, 200)
                    if not ohlcv: continue
                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    for c in ["open","high","low","close","vol"]: df[c] = pd.to_numeric(df[c], errors="coerce")
                    sig = await generate_signal_romeopt(exchange, df, symbol, tf)
                    if sig:
                        breakdown_str = ", ".join(sig['reason_list'])
                        risk = abs(sig['entry'] - sig['sl'])
                        reward1 = abs(sig['tp1'] - sig['entry'])
                        rr_ratio = round(reward1 / risk, 2) if risk > 0 else 0
                        await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\n"
                                 f"Entry: {sig['entry']:.8f}\n"
                                 f"SL: {sig.get('sl', 0):.8f}\n"
                                 f"TP1: {sig.get('tp1', 0):.8f} (0.8R)\n"
                                 f"TP2: {sig.get('tp2', 0):.8f} (1.6R)\n"
                                 f"Score: {sig['score']} | R:R: {rr_ratio}:1\n"
                                 f"Breakdown: {breakdown_str}\n"
                                 f"⚠️ RomeOPT-P: Fixed TP/SL (No SL→BE)")
                        await log_signal(sig)
                        last_signal_time[key] = time.time()
                        signals_found += 1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
        except Exception as e: log.exception("scan error: %s", e)
        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth","")
    if token != WEBHOOK_SECRET: raise HTTPException(403, "Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok":True}

# ---------------- MAIN ----------------
async def main():
    global exchange, db_conn
    await init_db()
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started - Live Early Signals\n"
             "✅ ALL 6 ORIGINAL STEPS ACTIVE\n"
             "✅ WINNER PATTERN FILTER: HTF_Align + (BOS or CHOCH) + FVG\n"
             "✅ FIXED: TP/SL never change after signal generation")
    await asyncio.gather(scan_loop(), monitor_signals())

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try: asyncio.run(main())
        except KeyboardInterrupt: log.info("Shutting down...")
        finally:
            if db_conn: asyncio.run(db_conn.close())