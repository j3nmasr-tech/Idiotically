#!/usr/bin/env python3
# FastScalp v2 – Dynamic ATR / TP / SL | OKX Spot | USDT Perps
import os, time, csv, requests, re, traceback
import pandas as pd
import numpy as np
from datetime import datetime

# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

DEBUG = True   # full debug

CAPITAL = 80.0
LEVERAGE = 30

CHECK_INTERVAL = 60    # seconds between full symbol cycles
SYMBOL_DELAY = 0.1     # short pause between symbols

TOP_SYMBOLS = 60  # top 60 coins
# ===== TIMEFRAMES =====
TIMEFRAMES = ["5m", "15m", "30m", "1h"]

# ===== SIGNAL FILTERS =====
MIN_TF_SCORE   = 45
CONF_MIN_TFS   = 1
CONFIDENCE_MIN = 60.0

ENTRY_FILTER_SCORE = MIN_TF_SCORE
ENTRY_FILTER_CONF  = CONFIDENCE_MIN

# ===== TRADE RISK MANAGEMENT =====
MAX_OPEN_TRADES       = 50
MAX_EXPOSURE_PCT      = 0.25
MIN_SL_DISTANCE_PCT   = 0.0015
SYMBOL_BLACKLIST      = set([])
RECENT_SIGNAL_SIGNATURE_EXPIRE = 300

# ===== WEIGHTS =====
WEIGHT_TURTLE = 0.5
WEIGHT_BIAS   = 0.3
WEIGHT_CRT    = 0.15
WEIGHT_VOLUME = 0.05

LOG_CSV = "./fastscalp_v2_signals.csv"

# ===== STATE =====
open_trades = []
recent_signals = {}
signals_sent_total = 0
skipped_signals = 0
_last_loop_start = 0.0

# ===== HELPERS =====
def debug_print(*args):
    if DEBUG:
        print("[DEBUG]", *args, flush=True)

def send_message(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured:", text, flush=True)
        return False
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": text},
            timeout=10
        )
        return True
    except Exception as e:
        print("Telegram error:", e, flush=True)
        return False

def sanitize_symbol(symbol):
    return re.sub(r"[^A-Z0-9]", "", symbol.upper())

# ===== OKX API =====
OKX_TICKERS = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
OKX_KLINES  = "https://www.okx.com/api/v5/market/candles"

def okx_symbol(sym):
    sym = sanitize_symbol(sym)
    if sym.endswith("USDT"):
        return sym.replace("USDT", "-USDT")
    return sym

def get_top_symbols(n=TOP_SYMBOLS):
    try:
        j = requests.get(OKX_TICKERS, timeout=5).json()
    except Exception as e:
        debug_print("Ticker request failed:", e)
        return ["BTCUSDT","ETHUSDT"]

    if "data" not in j:
        debug_print("Ticker response invalid:", j)
        return ["BTCUSDT","ETHUSDT"]

    pairs = []
    for d in j["data"]:
        instId = d.get("instId","")
        if not instId.endswith("-USDT"):
            continue
        try:
            vol = float(d.get("vol24h",0))
            last = float(d.get("last",0))
            usdt = instId.replace("-","")
            pairs.append((usdt, vol * last))
        except:
            continue

    pairs.sort(key=lambda x: x[1], reverse=True)
    final = [s for s,_ in pairs[:n]]
    debug_print("Top symbols:", final)
    return final or ["BTCUSDT","ETHUSDT"]

def get_klines(symbol, interval="1m", limit=200):
    instId = okx_symbol(symbol)
    tf_map = {
        "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H", "1d": "1D"
    }
    okx_tf = tf_map.get(interval, "1m")
    url = f"{OKX_KLINES}?instId={instId}&bar={okx_tf}&limit={limit}"
    try:
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        debug_print(f"{symbol} klines failed: {e}")
        return None

    if "data" not in j or len(j["data"])==0:
        return None

    rows = j["data"][::-1]
    df = pd.DataFrame(rows, columns=[
        "timestamp","open","high","low","close","volume",
        "volCcy","volCcyQuote","confirm"
    ])
    df = df[["timestamp","open","high","low","close","volume"]].astype(float)
    return df.reset_index(drop=True)

def get_price(symbol):
    try:
        j = requests.get(OKX_TICKERS, timeout=5).json()
    except Exception as e:
        debug_print(f"{symbol} get_price failed: {e}")
        return None
    if "data" not in j:
        return None
    instId = okx_symbol(symbol)
    for d in j["data"]:
        if d.get("instId") == instId:
            try:
                return float(d.get("last",0))
            except:
                return None
    return None

# ===== INDICATORS =====
def detect_crt(df):
    if len(df)<12: return False,False
    last = df.iloc[-1]
    o,h,l,c,v = last["open"], last["high"], last["low"], last["close"], last["volume"]
    body_series = (df["close"] - df["open"]).abs()
    avg_body = body_series.rolling(10,min_periods=6).mean().iloc[-1]
    avg_vol  = df["volume"].rolling(10,min_periods=6).mean().iloc[-1]
    if np.isnan(avg_body) or np.isnan(avg_vol): return False,False
    body = abs(c-o)
    wick_up = h - max(o,c)
    wick_down = min(o,c) - l
    bull = body<avg_body*0.7 and wick_down>avg_body*0.6 and v<avg_vol*1.2 and c>o
    bear = body<avg_body*0.7 and wick_up>avg_body*0.6 and v<avg_vol*1.2 and c<o
    return bull,bear

def detect_turtle(df, look=20):
    if len(df)<look+2: return False,False
    ph = df["high"].iloc[-look-1:-1].max()
    pl = df["low"].iloc[-look-1:-1].min()
    last = df.iloc[-1]
    bull = last["low"] < pl and last["close"] > pl*1.005
    bear = last["high"] > ph and last["close"] < ph*0.995
    return bull,bear

def smc_bias(df):
    e20 = df["close"].ewm(span=20).mean().iloc[-1]
    e50 = df["close"].ewm(span=50).mean().iloc[-1]
    return "bull" if (e20-e50)/e50>0.002 else "bear"

def volume_ok(df):
    ma = df["volume"].rolling(20,min_periods=8).mean().iloc[-1]
    if np.isnan(ma): return True
    e20 = df["close"].ewm(span=20).mean().iloc[-1]
    e50 = df["close"].ewm(span=50).mean().iloc[-1]
    mult = 1.2 if e20>e50 else 1.1
    return df["volume"].iloc[-1] > ma*mult

# ===== ATR & TP/SL =====
def get_atr(df, period=14):
    if len(df)<period+1: return None
    h,l,c = df["high"].values, df["low"].values, df["close"].values
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,len(df))]
    return float(np.mean(trs)) if trs else None

def trade_params_dynamic(entry, side, atr, conf_pct):
    adj = 1 + (conf_pct/100)*0.5
    tp1 = entry + atr*1.5*adj if side=="BUY" else entry - atr*1.5*adj
    tp2 = entry + atr*2.5*adj if side=="BUY" else entry - atr*2.5*adj
    tp3 = entry + atr*3.5*adj if side=="BUY" else entry - atr*3.5*adj
    sl  = entry - atr*1.2 if side=="BUY" else entry + atr*1.2
    return round(sl,6), round(tp1,6), round(tp2,6), round(tp3,6)

# ===== BTC TREND & VOLATILITY HELPERS =====
VOLATILITY_THRESHOLD_PCT = 0.8

def btc_volatility_spike():
    df = get_klines("BTCUSDT", "5m", 3)
    if df is None or len(df) < 3:
        return False
    pct = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0] * 100.0
    return abs(pct) >= VOLATILITY_THRESHOLD_PCT

def btc_trend_agree():
    df1 = get_klines("BTCUSDT", "15m", 50)
    df2 = get_klines("BTCUSDT", "1h", 50)
    if df1 is None or df2 is None:
        return None, None
    b1 = smc_bias(df1)
    b2 = smc_bias(df2)
    return (b1 == b2), b1 if b1==b2 else None

def entry_allowed(symbol, df):
    atr = get_atr(df)
    last_candle = df.iloc[-1]
    if atr is not None and abs(last_candle['close'] - last_candle['open']) > 2.5 * atr:
        return False

    recent_high = df['high'].iloc[-5:].max()
    recent_low  = df['low'].iloc[-5:].min()
    if (recent_high - recent_low)/recent_low < 0.0015:
        return False

    btc_agree, btc_dir = btc_trend_agree()
    # Relaxed: only check if we can detect BTC direction
    if btc_dir is None:
        return False  # only skip if trend is undetectable
    # otherwise allow trades even if BTC 15m disagrees

    if btc_volatility_spike():
        return False

    return True

# ===== SIGNAL ENGINE (with BTC filter) =====
def generate_signal(symbol):
    debug_print(f"Generating signal for {symbol}")
    global recent_signals
    dfs = {}
    directions = {}
    total_score = 0
    conf_count = 0

    # BTC & candle filter first
    df_check = get_klines(symbol, "5m", 50)
    if df_check is None or not entry_allowed(symbol, df_check):
        debug_print(f"{symbol} skipped due to BTC trend/volatility or candle filter")
        return None

    # ===== SCORING =====
    for tf in TIMEFRAMES:
        df = get_klines(symbol, tf, 100)
        if df is None:
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
    side = "BUY" if buy_count>sell_count else "SELL"

    confidence = total_score / max(1,len(dfs))
    if confidence < ENTRY_FILTER_CONF:
        debug_print(f"{symbol} rejected: low confidence {confidence:.2f}")
        return None

    sig_signature = f"{symbol}_{side}"
    if sig_signature in recent_signals and (time.time()-recent_signals[sig_signature]) < RECENT_SIGNAL_SIGNATURE_EXPIRE:
        debug_print(f"{symbol} duplicate signal blocked.")
        return None

    recent_signals[sig_signature] = time.time()

    df_main = dfs[TIMEFRAMES[0]]  # 5m TF for ATR
    atr = get_atr(df_main)
    if atr is None: return None
    price = get_price(symbol)
    if price is None: return None

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
    row = [now, signal["symbol"], signal["side"], signal["entry"],
           signal["sl"], signal["tp1"], signal["tp2"], signal["tp3"],
           signal["confidence"]]
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV,"w",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time","symbol","side","entry","sl","tp1","tp2","tp3","confidence"])
    with open(LOG_CSV,"a",newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ===== MAIN LOOP =====
def run_bot():
    global signals_sent_total, skipped_signals, _last_loop_start

    if not BOT_TOKEN or not CHAT_ID:
        print(f"[{datetime.utcnow()}] ⚠️ Telegram not configured!", flush=True)
    else:
        print(f"[{datetime.utcnow()}] Telegram credentials loaded.", flush=True)

    _last_loop_start = time.time()
    startup_text = "🚀 FastScalp v2 started on OKX! Scanning Top Symbols immediately."
    sent = send_message(startup_text)
    if sent:
        print(f"[{datetime.utcnow()}] ✅ Startup message sent to Telegram.", flush=True)
    else:
        print(f"[{datetime.utcnow()}] ⚠️ Failed to send startup message.", flush=True)

    try:
        symbols = get_top_symbols(TOP_SYMBOLS)
    except Exception as e:
        print(f"[{datetime.utcnow()}] ⚠️ Error fetching top symbols: {e}", flush=True)
        symbols = ["BTCUSDT","ETHUSDT"]

    print(f"[{datetime.utcnow()}] Scanning {len(symbols)} symbols: {symbols}", flush=True)

    while True:
        loop_start = time.time()
        print(f"[{datetime.utcnow()}] >>> Loop start...", flush=True)

        for idx, symbol in enumerate(symbols, start=1):
            try:
                print(f"[{datetime.utcnow()}] [{idx}/{len(symbols)}] Checking {symbol}...", flush=True)
                signal = generate_signal(symbol)
                if signal:
                    log_signal(signal)
                    send_message(
                        f"🔥 {signal['symbol']} | {signal['side']} | Entry: {signal['entry']}\n"
                        f"SL: {signal['sl']} | TP1: {signal['tp1']} | TP2: {signal['tp2']} | TP3: {signal['tp3']}\n"
                        f"Conf: {signal['confidence']}%"
                    )
                    signals_sent_total += 1
                    print(f"[{datetime.utcnow()}] >>> SIGNAL for {symbol}: {signal['side']} (conf {signal['confidence']})", flush=True)
                else:
                    skipped_signals += 1
                    print(f"[{datetime.utcnow()}] >>> No signal for {symbol}", flush=True)
            except Exception as e:
                traceback.print_exc()
                print(f"Error on {symbol}: {e}", flush=True)

            time.sleep(SYMBOL_DELAY)

        loop_duration = time.time() - loop_start
        print(f"[{datetime.utcnow()}] Loop finished in {loop_duration:.2f}s. Sleeping {CHECK_INTERVAL}s...", flush=True)
        time.sleep(CHECK_INTERVAL)

if __name__=="__main__":
    run_bot()