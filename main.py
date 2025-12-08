#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LIVE ROMEOPT 6-STEP SCANNER (Enhanced + Elite Features)
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
- 📊 COMPACT BREAKDOWN: All numbers, minimal text
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
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
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
MIN_SCORE = 5
CRITICAL_FACTORS_MIN = 2  # HTF Alignment + Liquidity Sweep minimum

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("romeopt_bot")
db_lock = asyncio.Lock()
db_conn = None

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
            latest_ob TEXT
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

# ---------------- INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

# ---------------- MARKET REGIME ----------------
async def detect_market_regime(df: pd.DataFrame):
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
        if not ohlcv: return False
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
        trend = df["close"].iloc[-1] - df["close"].iloc[-5]
        trend_side = "BUY" if trend>0 else "SELL"
        if trend_side != side:
            return False
    return True

# ---------------- ROMEOPT 6-STEP SIGNAL ----------------
async def generate_signal_romeopt(exchange, df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 20: return None
    last = df.iloc[-1]
    prev5 = df.iloc[-6:-1]
    score = 0
    reasons = []
    
    # Store all calculation values for breakdown
    calc_values = {}

    # Step 1: Liquidity Sweep
    prev5_high_max = float(prev5["high"].max())
    prev5_low_min = float(prev5["low"].min())
    last_high = float(last["high"])
    last_low = float(last["low"])
    
    sweep_high = last_high > prev5_high_max
    sweep_low = last_low < prev5_low_min
    has_sweep = sweep_high or sweep_low
    liquidity_sweep = 2 if has_sweep else 0
    score += liquidity_sweep
    sweep_type = "HIGH" if sweep_high else ("LOW" if sweep_low else "NONE")
    reasons.append(f"Liquidity Sweep +{liquidity_sweep}")
    
    calc_values["step1"] = {
        "type": sweep_type,
        "score": liquidity_sweep,
        "prev_high": prev5_high_max,
        "prev_low": prev5_low_min,
        "last_high": last_high,
        "last_low": last_low
    }

    # Step 2: Displacement
    candle_size = float(last["high"] - last["low"])
    body_size = float(abs(last["close"] - last["open"]))
    displacement = body_size / (candle_size + 1e-8)
    calc_values["step2"] = {
        "displacement": round(displacement, 3),
        "candle_size": round(candle_size, 6),
        "body_size": round(body_size, 6),
        "threshold": 0.6
    }
    
    has_disp = displacement > 0.6
    if has_disp:
        score += 2; reasons.append(f"Displacement +2 ({displacement:.2f})")
        calc_values["step2"]["score"] = 2
    else:
        reasons.append(f"Displacement +0 ({displacement:.2f})")
        calc_values["step2"]["score"] = 0

    # Step 3 & 4: Order Block & Zone
    ob_zone = None
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            ob_zone={"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"], "idx": i}; break
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            ob_zone={"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"]), "idx": i}; break

    zone_approach = 0
    if ob_zone:
        ob_type = ob_zone["type"]
        last_close = float(last["close"])
        
        if ob_type=="bullish" and last_close <= ob_zone["high"]: 
            score+=1; zone_approach=1; reasons.append("Zone Approach +1")
        elif ob_type=="bearish" and last_close >= ob_zone["low"]: 
            score+=1; zone_approach=1; reasons.append("Zone Approach +1")
        else: 
            reasons.append("Zone Approach +0")
        
        calc_values["step3_4"] = {
            "type": ob_type,
            "low": round(ob_zone["low"], 6),
            "high": round(ob_zone["high"], 6),
            "price": round(last_close, 6),
            "zone_score": zone_approach,
            "in_zone": zone_approach == 1
        }
    else:
        reasons.append("Zone Approach +0")
        ob_type=None
        calc_values["step3_4"] = {
            "type": "NONE",
            "zone_score": 0,
            "in_zone": False
        }

    # Step 5: HTF Alignment
    tf_map={"1m":"15m","3m":"30m","5m":"1h","15m":"4h","30m":"1h"}
    htf=tf_map.get(tf,"15m")
    ohlcv_htf = await fetch_ohlcv(exchange, symbol, htf, 50)
    htf_alignment = 0
    htf_trend_value = 0
    htf_dir = "UNKNOWN"
    
    if ohlcv_htf:
        df_htf = pd.DataFrame(ohlcv_htf, columns=["ts","open","high","low","close","vol"])
        price_now = float(df_htf["close"].iloc[-1])
        price_5bars_ago = float(df_htf["close"].iloc[-5])
        trend = price_now - price_5bars_ago
        htf_trend_value = round(trend, 6)
        htf_dir = "bullish" if trend>0 else "bearish"
        if ob_type and htf_dir==ob_type:
            score+=1; htf_alignment=1; reasons.append(f"HTF Alignment +1 ({htf_dir} {trend:+.6f})")
        else:
            reasons.append(f"HTF Alignment +0 ({htf_dir} {trend:+.6f})")
        
        calc_values["step5"] = {
            "tf": htf,
            "dir": htf_dir,
            "trend": htf_trend_value,
            "price_now": round(price_now, 6),
            "price_old": round(price_5bars_ago, 6),
            "score": htf_alignment,
            "aligned": htf_alignment == 1
        }
    else:
        reasons.append("HTF Alignment ?")
        calc_values["step5"] = {
            "tf": htf,
            "dir": "NO_DATA",
            "score": 0,
            "aligned": False
        }

    # Step 6: Momentum (0.8 THRESHOLD)
    momentum_ratio = abs(last["close"]-last["open"])/(last["high"]-last["low"]+1e-8)
    
    calc_values["step6"] = {
        "ratio": round(momentum_ratio, 3),
        "threshold": 0.8,
        "body": round(float(abs(last["close"] - last["open"])), 6),
        "candle": round(float(last["high"] - last["low"]), 6)
    }
    
    momentum_score = 0
    if ob_type=="bullish" and momentum_ratio>=0.8 and last["close"]>last["open"]:
        score+=1; momentum_score=1
        reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["step6"]["score"] = 1
        calc_values["step6"]["passed"] = True
    elif ob_type=="bearish" and momentum_ratio>=0.8 and last["close"]<last["open"]:
        score+=1; momentum_score=1
        reasons.append(f"Momentum +1 ({momentum_ratio:.2f})")
        calc_values["step6"]["score"] = 1
        calc_values["step6"]["passed"] = True
    else:
        reasons.append(f"Momentum +0 ({momentum_ratio:.2f})")
        calc_values["step6"]["score"] = 0
        calc_values["step6"]["passed"] = False

    if not ob_type: return None
    side = "BUY" if ob_type=="bullish" else "SELL"
    entry = float(last["close"])

    # ---------------- CRITICAL FILTERS ----------------
    critical_score = htf_alignment + liquidity_sweep
    if critical_score < CRITICAL_FACTORS_MIN: return None
    if score < MIN_SCORE: return None
    if not has_disp: return None
    
    # ---------------- HTF ALIGNMENT MANDATORY FILTER ----------------
    if htf_alignment != 1:  # MUST HAVE HTF Alignment = 1
        return None

    market_regime = await detect_market_regime(df)
    if (market_regime=="BULL" and side=="SELL") or (market_regime=="BEAR" and side=="BUY"): return None

    trend_ma = df["close"].rolling(20).mean().iloc[-1]
    if (side=="BUY" and last["close"]<trend_ma) or (side=="SELL" and last["close"]>trend_ma): return None

    # ---------------- ELITE MTF CONFIRMATION ----------------
    if not await elite_tf_alignment(exchange, symbol, side):
        return None
    reasons.append("Elite MTF Alignment ✅")

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
        "calc_values": calc_values
    }
    
    sig = update_tp_sl_live(sig, df)
    
    # ---------------- TP1 DISTANCE FILTER ----------------
    if sig and "sl" in sig and "tp1" in sig:
        risk = abs(sig["entry"] - sig["sl"])
        tp1_distance = abs(sig["tp1"] - sig["entry"])
        
        # Reject if TP1 is less than 10% of risk
        if tp1_distance < risk * 0.1:
            return None
    
    return sig

# ---------------- TP/SL HELPERS ----------------
def romeopt_tp_sl(entry, side, atr_val, ob_zone, df):
    """
    OPTIMIZED TP/SL using market structure + ATR
    """
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
        
    else:  # SELL
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
    for i in range(len(df)-5, len(df)-1):
        candle, prev_candle = df.iloc[i], df.iloc[i-1]
        if candle["close"]>candle["open"] and prev_candle["close"]<prev_candle["open"]:
            return {"type":"bullish","low":min(candle["low"], prev_candle["low"]),"high":candle["close"]}
        elif candle["close"]<candle["open"] and prev_candle["close"]>prev_candle["open"]:
            return {"type":"bearish","low":candle["close"],"high":max(candle["high"], prev_candle["high"])}
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
    sl, tp1, tp2, tp3 = romeopt_tp_sl(entry, side, atr_val, latest_ob, df)
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
        
        await db_conn.execute("""
            INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score,latest_ob)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["symbol"],sig["side"],sig["entry"],sig.get("sl"),sig.get("tp1"),sig.get("tp2"),tp3,
              datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"],str(sig.get("latest_ob",""))))
        await db_conn.commit()

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
                        # COMPACT BUT COMPLETE BREAKDOWN
                        calc = sig.get("calc_values", {})
                        
                        step1 = calc.get("step1", {})
                        step2 = calc.get("step2", {})
                        step3_4 = calc.get("step3_4", {})
                        step5 = calc.get("step5", {})
                        step6 = calc.get("step6", {})
                        
                        breakdown_lines = [
                            f"🏆 {sig['symbol']} {sig['side']} ({tf})",
                            f"Entry: {sig['entry']:.6f} | Score: {sig['score']}/6",
                            f"",
                            f"1️⃣ Sweep: +{step1.get('score',0)} {step1.get('type','-')}",
                            f"   • High: {step1.get('last_high',0):.6f} > {step1.get('prev_high',0):.6f}",
                            f"   • Low: {step1.get('last_low',0):.6f} vs {step1.get('prev_low',0):.6f}",
                            f"",
                            f"2️⃣ Disp: +{step2.get('score',0)} {step2.get('displacement',0):.3f}",
                            f"   • Body: {step2.get('body_size',0):.3f} | Candle: {step2.get('candle_size',0):.3f}",
                            f"   • Thresh: >{step2.get('threshold',0.6)}",
                            f"",
                            f"3️⃣4️⃣ Zone: +{step3_4.get('zone_score',0)} {step3_4.get('type','-')}",
                            f"   • OB: {step3_4.get('low',0):.6f}-{step3_4.get('high',0):.6f}",
                            f"   • Price: {step3_4.get('price',0):.6f} | In zone: {'✓' if step3_4.get('in_zone') else '✗'}",
                            f"",
                            f"5️⃣ HTF: +{step5.get('score',0)} {step5.get('dir','-')} ({step5.get('tf','-')})",
                            f"   • Trend: {step5.get('trend',0):+.6f}",
                            f"   • Aligned: {'✓' if step5.get('aligned') else '✗'}",
                            f"",
                            f"6️⃣ Mom: +{step6.get('score',0)} {step6.get('ratio',0):.3f}",
                            f"   • Body/Candle: {step6.get('body',0):.3f}/{step6.get('candle',0):.3f}",
                            f"   • Thresh: ≥{step6.get('threshold',0.8)} | Passed: {'✓' if step6.get('passed') else '✗'}",
                            f"",
                            f"🎯 Setup:",
                            f"   • SL: {sig.get('sl',0):.6f}",
                            f"   • TP1: {sig.get('tp1',0):.6f}",
                            f"   • TP2: {sig.get('tp2',0):.6f}",
                            f"   • TP3: {sig.get('tp3',0):.6f}",
                            f"   • R:R: {(abs(sig.get('tp1',0) - sig['entry']) / max(abs(sig['entry'] - sig.get('sl',0)), 0.000001)):.2f}:1"
                        ]
                        
                        await tg("\n".join(breakdown_lines))
                        await log_signal(sig)
                        last_signal_time[key]=time.time()
                        signals_found+=1
            log.info(f"📊 Scan complete: {signals_found} RomeOPT signals found")
        except Exception as e: log.exception("scan error: %s", e)
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
    await init_db()
    global exchange
    exchange = ccxt.okx({"enableRateLimit": True})
    await tg("🏆 ROMEOPT 6-Step Scanner Started")
    await tg("📊 Compact breakdown enabled - all numbers shown")
    await asyncio.gather(scan_loop(exchange), monitor_signals())

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args=p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        try:
            asyncio.run(main())
        finally:
            if db_conn:
                asyncio.run(db_conn.close())