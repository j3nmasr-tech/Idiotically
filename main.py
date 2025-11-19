#!/usr/bin/env python3
# FastScalp v2 – Dynamic ATR / TP / SL | Bitget | USDT Perps
import os, time, csv, re, requests, traceback
import pandas as pd
import numpy as np
from datetime import datetime

# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

DEBUG = True   # <<< TURN DEBUG ON/OFF HERE

CAPITAL = 80.0
LEVERAGE = 30
CHECK_INTERVAL = 300
SYMBOL_DELAY = 0.3
TOP_SYMBOLS = 40

TIMEFRAMES = ["5m","15m","30m","1h"]
MIN_TF_SCORE = 55
CONF_MIN_TFS = 2
CONFIDENCE_MIN = 60.0

ENTRY_FILTER_SCORE = MIN_TF_SCORE
ENTRY_FILTER_CONF  = CONFIDENCE_MIN

MAX_OPEN_TRADES = 50
MAX_EXPOSURE_PCT = 0.25
MIN_SL_DISTANCE_PCT = 0.0015
SYMBOL_BLACKLIST = set([])
RECENT_SIGNAL_SIGNATURE_EXPIRE = 300

WEIGHT_BIAS   = 0.2
WEIGHT_TURTLE = 0.4
WEIGHT_CRT    = 0.25
WEIGHT_VOLUME = 0.15

LOG_CSV = "./fastscalp_v2_signals.csv"

# ===== STATE =====
open_trades = []
recent_signals = {}
signals_sent_total = 0
skipped_signals = 0
last_heartbeat = time.time()

# ===== HELPERS =====
def debug_print(*args):
    if DEBUG:
        print("[DEBUG]", *args)

def send_message(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured:", text)
        return False
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": text},
            timeout=10
        )
        return True
    except Exception as e:
        print("Telegram error:", e)
        return False

def sanitize_symbol(symbol):
    return re.sub(r"[^A-Z0-9_.-]","",symbol.upper())[:20]

# ===== BITGET API =====
BITGET_TICKERS = "https://api.bitget.com/api/spot/v1/market/tickers"
BITGET_KLINES  = "https://api.bitget.com/api/spot/v1/market/candles"

# Bitget SPOT granularity mapping
_interval_map = {
    "1m":  "1min",
    "3m":  "3min",
    "5m":  "5min",
    "15m": "15min",
    "30m": "30min",
    "1h":  "1hour",
    "4h":  "4hour"
}

def interval_to_bitget(tf: str) -> str:
    return _interval_map.get(tf, "5min")

def to_bitget_symbol(symbol: str) -> str:
    if symbol.endswith("_SPBL"):
        return symbol
    return f"{symbol}_SPBL"

# ===== SAFE JSON =====
def safe_get_json(url, params=None, timeout=5, retries=1):
    for attempt in range(retries+1):
        try:
            debug_print(f"Requesting {url} params={params}")
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            debug_print("Response OK")
            return j
        except Exception as e:
            debug_print(f"Request failed (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(0.4)
            else:
                debug_print("Returning None after retries.")
                return None

# ===== TOP SYMBOLS =====
def get_top_symbols(n=TOP_SYMBOLS):
    j = safe_get_json(BITGET_TICKERS)
    if not j or "data" not in j:
        debug_print("Ticker list failed, using BTC/ETH fallback")
        return ["BTCUSDT","ETHUSDT"]

    pairs = []
    for d in j["data"]:
        sym = sanitize_symbol(d.get("symbol",""))
        if not sym.endswith("USDT"): continue
        try:
            vol = float(d.get("baseVolume",0))
            last = float(d.get("last",0))
            pairs.append((sym, vol * last))
        except:
            continue

    pairs.sort(key=lambda x: x[1], reverse=True)
    final = [s for s,_ in pairs[:n]]
    debug_print("Top symbols:", final)
    return final or ["BTCUSDT","ETHUSDT"]

# ===== KLINES =====
def get_klines(symbol, interval="5m", limit=200):
    symbol_bitget = to_bitget_symbol(sanitize_symbol(symbol))
    gran = interval_to_bitget(interval)

    j = safe_get_json(
        BITGET_KLINES,
        {"symbol": symbol_bitget, "granularity": gran, "limit": limit},
        timeout=8,
        retries=2
    )

    if not j:
        debug_print(f"Klines None for {symbol_bitget} {interval}")
        return None

    data = j.get("data")
    if not data:
        debug_print(f"Klines empty for {symbol_bitget}")
        return None

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], (list, tuple)):
        try:
            df = pd.DataFrame(data, columns=["timestamp","open","high","low","close","volume"])
        except:
            df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(data)
        mapping = {}
        if "time" in df.columns and "timestamp" not in df.columns:
            mapping["time"] = "timestamp"
        if "close_price" in df.columns:
            mapping["close_price"] = "close"
        if "quoteVolume" in df.columns:
            mapping["quoteVolume"] = "volume"
        if mapping:
            df = df.rename(columns=mapping)

    if df.empty:
        debug_print(f"Klines DataFrame empty for {symbol_bitget}")
        return None

    for col in ["open","high","low","close","volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)

# ===== PRICE =====
def get_price(symbol):
    symbol_bitget = to_bitget_symbol(sanitize_symbol(symbol))
    j = safe_get_json(BITGET_TICKERS)
    if not j or "data" not in j:
        return None
    for d in j["data"]:
        if sanitize_symbol(d.get("symbol","")) == symbol_bitget.replace("_SPBL",""):
            try:
                return float(d.get("last",0))
            except:
                return None
    return None

# ===== INDICATORS =====
def detect_crt(df):
    if len(df)<12:
        debug_print("CRT skipped - insufficient candles")
        return False,False
    last = df.iloc[-1]
    o,h,l,c,v = last["open"],last["high"],last["low"],last["close"],last["volume"]
    body_series = (df["close"] - df["open"]).abs()
    avg_body = body_series.rolling(10,min_periods=6).mean().iloc[-1]
    avg_vol  = df["volume"].rolling(10,min_periods=6).mean().iloc[-1]
    if np.isnan(avg_body) or np.isnan(avg_vol):
        return False,False
    body = abs(c-o)
    wick_up = h - max(o,c)
    wick_down = min(o,c) - l
    bull = body < avg_body*0.7 and wick_down > avg_body*0.6 and v < avg_vol*1.2 and c > o
    bear = body < avg_body*0.7 and wick_up > avg_body*0.6 and v < avg_vol*1.2 and c < o
    return bull,bear

def detect_turtle(df, look=20):
    if len(df)<look+2:
        return False,False
    ph = df["high"].iloc[-look-1:-1].max()
    pl = df["low"].iloc[-look-1:-1].min()
    last = df.iloc[-1]
    bull = last["low"] < pl and last["close"] > pl*1.005
    bear = last["high"] > ph and last["close"] < ph*0.995
    return bull,bear

def smc_bias(df):
    e20 = df["close"].ewm(span=20).mean().iloc[-1]
    e50 = df["close"].ewm(span=50).mean().iloc[-1]
    return "bull" if (e20 - e50)/e50 > 0.002 else "bear"

def volume_ok(df):
    ma = df["volume"].rolling(20,min_periods=8).mean().iloc[-1]
    if np.isnan(ma): return True
    e20 = df["close"].ewm(span=20).mean().iloc[-1]
    e50 = df["close"].ewm(span=50).mean().iloc[-1]
    mult = 1.2 if e20 > e50 else 1.1
    return df["volume"].iloc[-1] > ma * mult

# ===== ATR & TRADE PARAMETERS =====
def get_atr(df, period=14):
    if len(df) < period+1: return None
    h,l,c = df["high"].values, df["low"].values, df["close"].values
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,len(df))]
    if not trs: return None
    return float(np.mean(trs))

def trade_params_dynamic(entry, side, atr, conf_pct):
    adj = 1 + (conf_pct/100)*0.5
    tp1 = entry + atr*1.5*adj if side=="BUY" else entry - atr*1.5*adj
    tp2 = entry + atr*2.5*adj if side=="BUY" else entry - atr*2.5*adj
    tp3 = entry + atr*3.5*adj if side=="BUY" else entry - atr*3.5*adj
    sl  = entry - atr*1.2       if side=="BUY" else entry + atr*1.2
    return round(sl,6), round(tp1,6), round(tp2,6), round(tp3,6)

# ===== SIGNAL ENGINE =====
def generate_signal(symbol):
    debug_print(f"Generating signal for {symbol}")
    global recent_signals

    dfs = {}
    directions = {}
    total_score = 0
    conf_count = 0

    for tf in TIMEFRAMES:
        df = get_klines(symbol, tf, 100)
        if df is None:
            debug_print(f"{symbol} {tf}: no data")
            directions[tf] = None
            continue

        dfs[tf] = df
        bull_t, bear_t = detect_turtle(df)
        bull_c, bear_c = detect_crt(df)
        bias = smc_bias(df)
        vol_ok_flag = volume_ok(df)

        bull_score = 0
        bear_score = 0

        if bull_t: bull_score += WEIGHT_TURTLE*100
        if bear_t: bear_score += WEIGHT_TURTLE*100
        if bull_c: bull_score += WEIGHT_CRT*100
        if bear_c: bear_score += WEIGHT_CRT*100
        if bias=="bull": bull_score += WEIGHT_BIAS*100
        else: bear_score += WEIGHT_BIAS*100
        if vol_ok_flag:
            bull_score += WEIGHT_VOLUME*50
            bear_score += WEIGHT_VOLUME*50

        total_score += max(bull_score,bear_score)

        if bull_score >= ENTRY_FILTER_SCORE:
            directions[tf] = "BUY"
        elif bear_score >= ENTRY_FILTER_SCORE:
            directions[tf] = "SELL"
        else:
            directions[tf] = None

        if directions[tf] is not None:
            conf_count += 1

        debug_print(symbol, tf, "bull:", bull_score, "bear:", bear_score, "dir:", directions[tf])

    if conf_count < CONF_MIN_TFS:
        debug_print(f"{symbol} rejected: insufficient TF confirmations")
        return None

    buy_count = sum(1 for d in directions.values() if d=="BUY")
    sell_count = sum(1 for d in directions.values() if d=="SELL")
    side = "BUY" if buy_count > sell_count else "SELL"
    confidence = total_score / max(1, len(dfs))

    if confidence < ENTRY_FILTER_CONF:
        debug_print(f"{symbol} rejected: low confidence {confidence:.2f}")
        return None

    sig_signature = f"{symbol}_{side}"
    if sig_signature in recent_signals and (time.time() - recent_signals[sig_signature]) < RECENT_SIGNAL_SIGNATURE_EXPIRE:
        debug_print(f"{symbol} duplicate signal blocked.")
        return None

    recent_signals[sig_signature] = time.time()

    df_main = dfs.get("5m") or list(dfs.values())[0]
    atr = get_atr(df_main)
    if atr is None:
        debug_print(f"{symbol} ATR failed")
        return None

    price = get_price(symbol)
    if price is None:
        debug_print(f"{symbol} price unavailable")
        return None

    sl,tp1,tp2,tp3 = trade_params_dynamic(price, side, atr, confidence)

    return {
        "symbol": symbol,
        "side": side,
        "entry": price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "confidence": round(confidence,2)
    }

# ===== LOGGING =====
def log_signal(signal):
    if signal is None: return
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        now, signal["symbol"], signal["side"], signal["entry"], signal["sl"],
        signal["tp1"], signal["tp2"], signal["tp3"], signal["confidence"]
    ]
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV,"w",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time","symbol","side","entry","sl","tp1","tp2","tp3","confidence"])
    with open(LOG_CSV,"a",newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ===== MAIN LOOP =====
def run_bot():
    global signals_sent_total, skipped_signals, last_heartbeat

    send_message("🚀 FastScalp v2 started on Bitget!")
    print(f"[{datetime.utcnow()}] FastScalp v2 started on Bitget!")

    symbols = get_top_symbols(TOP_SYMBOLS)
    print(f"[{datetime.utcnow()}] Scanning {len(symbols)} symbols: {symbols}")

    while True:
        for symbol in symbols:
            try:
                print(f"[{datetime.utcnow()}] Checking {symbol}...")
                signal = generate_signal(symbol)

                if signal:
                    log_signal(signal)
                    send_message(
                        f"🔥 {signal['symbol']} | {signal['side']} | Entry: {signal['entry']}\n"
                        f"SL: {signal['sl']} | TP1: {signal['tp1']} | TP2: {signal['tp2']} | TP3: {signal['tp3']}\n"
                        f"Conf: {signal['confidence']}%"
                    )
                    signals_sent_total += 1
                else:
                    skipped_signals += 1
            except Exception as e:
                traceback.print_exc()
                print(f"Error on {symbol}: {e}")
            time.sleep(SYMBOL_DELAY)

        if time.time() - last_heartbeat > 600:
            send_message(f"Heartbeat: {signals_sent_total} signals sent, {skipped_signals} skipped")
            last_heartbeat = time.time()

        print(f"Sleeping {CHECK_INTERVAL}s...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run_bot()