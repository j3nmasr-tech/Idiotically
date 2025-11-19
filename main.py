#!/usr/bin/env python3
# FastScalp v2 – Dynamic ATR / TP / SL | Bybit | USDT Perps
import os, time, csv, re, requests
import pandas as pd
import numpy as np
from datetime import datetime

# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

CAPITAL = 80.0
LEVERAGE = 30
CHECK_INTERVAL = 60  # 1 min
API_CALL_DELAY = 0.05
TOP_SYMBOLS = 40

TIMEFRAMES = ["5m","15m","30m","1h"]
MIN_TF_SCORE = 55
CONF_MIN_TFS = 2
CONFIDENCE_MIN = 60.0
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
signals_hit_total = 0
signals_fail_total = 0
total_checked_signals = 0
skipped_signals = 0
last_heartbeat = time.time()
last_summary   = time.time()

# ===== HELPERS =====
def send_message(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured:", text)
        return False
    try:
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                      data={"chat_id": CHAT_ID, "text": text}, timeout=10)
        return True
    except Exception as e:
        print("Telegram error:", e)
        return False

def sanitize_symbol(symbol):
    return re.sub(r"[^A-Z0-9_.-]","",symbol.upper())[:20]

def safe_get_json(url, params=None, timeout=5, retries=1):
    for attempt in range(retries+1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except:
            if attempt < retries: time.sleep(0.5*(attempt+1))
            else: return None

# ===== BYBIT ACCESS =====
BYBIT_BASE    = "https://api.bybit.com/v5/market"
BYBIT_TICKERS = f"{BYBIT_BASE}/tickers"
BYBIT_KLINES  = f"{BYBIT_BASE}/kline"
_interval_map = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30","1h":"60","4h":"240"}

def interval_to_bybit(interval):
    return _interval_map.get(interval, interval)

def get_top_symbols(n=TOP_SYMBOLS):
    j = safe_get_json(BYBIT_TICKERS, {"category":"linear"})
    if not j or "result" not in j or "list" not in j["result"]: return ["BTCUSDT","ETHUSDT"]
    usdt_pairs = []
    for d in j["result"]["list"]:
        sym = sanitize_symbol(d.get("symbol",""))
        if not sym.endswith("USDT"): continue
        try:
            vol = float(d.get("volume24h",0))
            last = float(d.get("lastPrice",0))
            usdt_pairs.append((sym,vol*last))
        except: continue
    usdt_pairs.sort(key=lambda x:x[1], reverse=True)
    return [s[0] for s in usdt_pairs[:n]] or ["BTCUSDT","ETHUSDT"]

def get_klines(symbol, interval="5m", limit=200):
    symbol = sanitize_symbol(symbol)
    iv = interval_to_bybit(interval)
    j = safe_get_json(BYBIT_KLINES, {"category":"linear","symbol":symbol,"interval":iv,"limit":limit})
    if not j or "result" not in j or "list" not in j["result"]: return None
    df = pd.DataFrame(j["result"]["list"])
    if df.empty: return None
    if set(["open","high","low","close","volume"]).issubset(df.columns):
        return df[["open","high","low","close","volume"]].astype(float)
    elif set(["o","h","l","c","v"]).issubset(df.columns):
        df = df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume"})
        return df[["open","high","low","close","volume"]].astype(float)
    elif isinstance(df.iloc[0,0], list):
        df = pd.DataFrame(df.iloc[:,1:6].values, columns=["open","high","low","close","volume"]).astype(float)
        return df
    return None

def get_price(symbol):
    j = safe_get_json(BYBIT_TICKERS, {"category":"linear","symbol":symbol})
    if not j or "result" not in j or "list" not in j["result"]: return None
    for d in j["result"]["list"]:
        if sanitize_symbol(d.get("symbol",""))==symbol:
            return float(d.get("lastPrice",0))
    return None

# ===== INDICATORS =====
def detect_crt(df):
    if len(df)<12: return False,False
    last = df.iloc[-1]
    o,h,l,c,v = last["open"],last["high"],last["low"],last["close"],last["volume"]
    body_series = (df["close"]-df["open"]).abs()
    avg_body = body_series.rolling(10,min_periods=6).mean().iloc[-1]
    avg_vol  = df["volume"].rolling(10,min_periods=6).mean().iloc[-1]
    if np.isnan(avg_body) or np.isnan(avg_vol): return False,False
    body = abs(c-o)
    wick_up = h-max(o,c)
    wick_down = min(o,c)-l
    bull = body<avg_body*0.7 and wick_down>avg_body*0.6 and v<avg_vol*1.2 and c>o
    bear = body<avg_body*0.7 and wick_up>avg_body*0.6 and v<avg_vol*1.2 and c<o
    return bull,bear

def detect_turtle(df, look=20):
    if len(df)<look+2: return False,False
    ph = df["high"].iloc[-look-1:-1].max()
    pl = df["low"].iloc[-look-1:-1].min()
    last = df.iloc[-1]
    bull = last["low"]<pl and last["close"]>pl*1.005
    bear = last["high"]>ph and last["close"]<ph*0.995
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
    return df["volume"].iloc[-1]>ma*mult

# ===== ATR & DYNAMIC TP/SL =====
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

# ===== SIGNAL ENGINE =====
def generate_signal(symbol):
    global recent_signals
    dfs = {}
    directions = {}
    scores = {}
    conf_count = 0
    total_score = 0
    for tf in TIMEFRAMES:
        df = get_klines(symbol, tf, 100)
        if df is None or df.empty: continue
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
        if vol_ok_flag: bull_score += WEIGHT_VOLUME*50; bear_score += WEIGHT_VOLUME*50
        total_score += max(bull_score,bear_score)
        scores[tf] = {"bull":bull_score, "bear":bear_score, "bias":bias, "vol_ok":vol_ok_flag}
        if bull_score>=MIN_TF_SCORE: directions[tf]="BUY"
        elif bear_score>=MIN_TF_SCORE: directions[tf]="SELL"
        else: directions[tf]=None
        if directions[tf] is not None: conf_count+=1

    if conf_count<CONF_MIN_TFS: return None

    buy_count = sum(1 for d in directions.values() if d=="BUY")
    sell_count = sum(1 for d in directions.values() if d=="SELL")
    side = "BUY" if buy_count>sell_count else "SELL"

    confidence = total_score / max(1,len(dfs))
    if confidence<CONFIDENCE_MIN: return None

    sig_signature = f"{symbol}_{side}"
    if sig_signature in recent_signals and (time.time()-recent_signals[sig_signature])<RECENT_SIGNAL_SIGNATURE_EXPIRE:
        return None
    recent_signals[sig_signature] = time.time()

    df_main = dfs.get("5m") or list(dfs.values())[0]
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
    global LOG_CSV
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = [now, signal["symbol"], signal["side"], signal["entry"], signal["sl"], signal["tp1"], signal["tp2"], signal["tp3"], signal["confidence"]]
    if not os.path.exists(LOG_CSV):
        with open(LOG_CSV,"w",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["time","symbol","side","entry","sl","tp1","tp2","tp3","confidence"])
    with open(LOG_CSV,"a",newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ===== MAIN LOOP =====
def run_bot():
    global open_trades, signals_sent_total, skipped_signals, last_heartbeat
    symbols = get_top_symbols(TOP_SYMBOLS)
    while True:
        for symbol in symbols:
            try:
                signal = generate_signal(symbol)
                if signal:
                    log_signal(signal)
                    send_message(f"🔥 {signal['symbol']} | {signal['side']} | Entry: {signal['entry']}\nSL: {signal['sl']} | TP1: {signal['tp1']} | TP2: {signal['tp2']} | TP3: {signal['tp3']}\nConf: {signal['confidence']}%")
                    signals_sent_total += 1
                else:
                    skipped_signals += 1
                time.sleep(API_CALL_DELAY)
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
        if time.time()-last_heartbeat>600:
            send_message(f"FastScalp v2 heartbeat: {signals_sent_total} signals sent, {skipped_signals} skipped")
            last_heartbeat = time.time()
        time.sleep(CHECK_INTERVAL)

if __name__=="__main__":
    send_message("🚀 FastScalp v2 started!")
    run_bot()