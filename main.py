#!/usr/bin/env python3
"""
BingX Low-Volume Altcoin Scalp Bot - Fixed Instant-Start Version
- Low-volume USDT coins 100k–5M USD 24h volume
- Instant startup: sends Telegram message immediately
- Rolling scan without threads
- EMA trend + ATR TP/SL + RSI/MACD/volume/candle filter
"""

import os, time, logging, json
from datetime import datetime, timedelta
import requests
import pandas as pd
import ccxt

# ---------- Config ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))  # seconds

EMA_SHORT = int(os.getenv("EMA_SHORT", "50"))
EMA_LONG  = int(os.getenv("EMA_LONG", "200"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
VOL_MULTIPLIER = float(os.getenv("VOL_MULTIPLIER", "1.6"))

MIN_24H_VOLUME_USD = 100_000
MAX_24H_VOLUME_USD = 5_000_000
MIN_CANDLES = 50
SIGNAL_COOLDOWN_MINUTES = 60

SIGNAL_LOG_FILE = os.getenv("SIGNAL_LOG_FILE", "signals_log.csv")
LAST_SIGNALS_FILE = os.getenv("LAST_SIGNALS_FILE", "last_signals.json")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- CCXT ----------
exchange = ccxt.bingx({'enableRateLimit': True})
exchange.load_markets()

# ---------- Telegram ----------
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode":"HTML"},
            timeout=10
        )
    except Exception as e:
        logging.exception("Telegram send failed: %s", e)

# ---------- OHLCV & Indicators ----------
def fetch_ohlcv(symbol, tf, limit=300):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        if not raw: return None
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except:
        return None

def ema(series, period): return series.ewm(span=period, adjust=False).mean()
def rsi(series, period=14):
    delta = series.diff()
    up, down = delta.clip(lower=0), -1*delta.clip(upper=0)
    ma_up, ma_down = up.ewm(alpha=1/period, adjust=False).mean(), down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))
def macd(series, fast=12, slow=26, signal=9):
    fast_ema, slow_ema = ema(series, fast), ema(series, slow)
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist
def atr(df, period=14):
    high, low, close = df['high'], df['low'], df['close']
    tr = pd.concat([high-low, (high-close.shift(1)).abs(), (low-close.shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean().iloc[-1]
def detect_trend(df):
    if df is None or len(df)<EMA_LONG: return None
    e_short, e_long = ema(df['close'], EMA_SHORT).iloc[-1], ema(df['close'], EMA_LONG).iloc[-1]
    if e_short>e_long: return "BUY"
    elif e_short<e_long: return "SELL"
    return None
def check_entry_5m(df5, direction):
    if df5 is None or len(df5)<30: return False, {}
    close, vol = df5['close'], df5['volume']
    last = df5.iloc[-1]
    mean_vol20 = vol[-21:-1].mean() if len(vol)>=21 else vol.mean()
    rsi_val = rsi(close, RSI_PERIOD).iloc[-1]
    macd_hist_last = macd(close)[2].iloc[-1]
    if direction=="BUY": cond_rsi, cond_macd = rsi_val>55, macd_hist_last>0
    else: cond_rsi, cond_macd = rsi_val<45, macd_hist_last<0
    cond_vol = last['volume']>(mean_vol20*VOL_MULTIPLIER) if mean_vol20>0 else False
    body_ratio = abs(last['close']-last['open'])/(last['high']-last['low']+1e-12)
    cond_body = body_ratio>0.3
    ok = cond_rsi and cond_macd and cond_vol and cond_body
    return ok, {"rsi":float(rsi_val),"macd_hist":float(macd_hist_last),"vol":float(last['volume']),"mean_vol20":float(mean_vol20),"body_ratio":float(body_ratio)}

# ---------- Persistent Signals ----------
def load_last_signals():
    if not os.path.exists(LAST_SIGNALS_FILE): return {}
    try: return json.load(open(LAST_SIGNALS_FILE))
    except: return {}
def save_last_signals(data):
    json.dump(data, open(LAST_SIGNALS_FILE,'w'))
def can_signal(symbol):
    last = load_last_signals()
    ts_str = last.get(symbol)
    if not ts_str: return True
    try: return datetime.utcnow()-datetime.fromisoformat(ts_str)>timedelta(minutes=SIGNAL_COOLDOWN_MINUTES)
    except: return True
def mark_signaled(symbol):
    last = load_last_signals()
    last[symbol] = datetime.utcnow().isoformat()
    save_last_signals(last)

# ---------- Signal Logging ----------
if not os.path.exists(SIGNAL_LOG_FILE):
    pd.DataFrame(columns=["Timestamp","Symbol","Direction","EntryPrice","TP1","TP2","TP3","SL","Status","ClosePrice","CloseTime"]).to_csv(SIGNAL_LOG_FILE,index=False)
def log_signal(data):
    df = pd.read_csv(SIGNAL_LOG_FILE)
    df = pd.concat([df,pd.DataFrame([data])],ignore_index=True)
    df.to_csv(SIGNAL_LOG_FILE,index=False)
def update_signal_status(symbol,direction,price):
    updated=False
    df=pd.read_csv(SIGNAL_LOG_FILE)
    for idx,row in df.iterrows():
        if row['Symbol']==symbol and row['Direction']==direction and row['Status']=="Open":
            sl=row['SL']; tp=row.get('TP3',row['TP3']); status=None
            if direction=="BUY":
                if price>=tp: status="TP Hit"
                elif price<=sl: status="SL Hit"
            else:
                if price<=tp: status="TP Hit"
                elif price>=sl: status="SL Hit"
            if status:
                df.at[idx,'Status']=status
                df.at[idx,'ClosePrice']=price
                df.at[idx,'CloseTime']=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                updated=True
    if updated: df.to_csv(SIGNAL_LOG_FILE,index=False)
    return updated

# ---------- Signal Formatting ----------
def format_signal(symbol,direction,entry,tp1,tp2,tp3,sl):
    now=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return f"⚡ <b>SCALP SIGNAL: {direction}</b>\n🔹 Symbol: <b>{symbol}</b>\n⏱ Time: {now}\n💵 Entry Price: {entry:.8f}\n🎯 TP: {tp1:.8f} | {tp2:.8f} | {tp3:.8f}\n🛑 SL: {sl:.8f}\nℹ️ Status: Open"

# ---------- Symbol Processing ----------
def process_symbol(sym):
    df30 = fetch_ohlcv(sym,"30m",50)
    if df30 is None or len(df30)<20: return
    avg_vol = (df30['close']*df30['volume']).tail(20).mean()
    if avg_vol<MIN_24H_VOLUME_USD or avg_vol>MAX_24H_VOLUME_USD: return

    df15 = fetch_ohlcv(sym,"15m",MIN_CANDLES)
    if df15 is None or len(df15)<MIN_CANDLES: return
    trend15, trend30 = detect_trend(df15), detect_trend(df30)
    if trend15!=trend30 or trend15 is None: return
    direction = trend15

    df5 = fetch_ohlcv(sym,"5m",200)
    ok,_ = check_entry_5m(df5,direction)
    if not ok: return

    entry = df5['close'].iloc[-1]
    atr_val = atr(df15)
    momentum_factor = min(abs(macd(df15['close'])[2].iloc[-1])/ (abs(macd(df15)['close'][2].iloc[-14:]).max()+1e-12),3)

    if direction=="BUY":
        sl = entry - atr_val; tp1 = entry+1.5*atr_val*momentum_factor; tp2=entry+2.0*atr_val*momentum_factor; tp3=entry+3.0*atr_val*momentum_factor
    else:
        sl = entry + atr_val; tp1 = entry-1.5*atr_val*momentum_factor; tp2=entry-2.0*atr_val*momentum_factor; tp3=entry-3.0*atr_val*momentum_factor

    if not can_signal(sym): return
    msg = format_signal(sym,direction,entry,tp1,tp2,tp3,sl)
    logging.info("Signal -> %s %s",sym,direction)
    send_telegram(msg)
    log_signal({"Timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),"Symbol":sym,"Direction":direction,"EntryPrice":float(entry),"TP1":float(tp1),"TP2":float(tp2),"TP3":float(tp3),"SL":float(sl),"Status":"Open","ClosePrice":"","CloseTime":""})
    mark_signaled(sym)
    time.sleep(0.2)

# ---------- Main Loop ----------
def run():
    logging.info("🤖 Bot starting. Loading markets...")
    exchange.load_markets()
    try: tickers = exchange.fetch_tickers()
    except Exception as e: logging.error("fetch_tickers failed: %s", e); return

    available = [s for s,t in tickers.items() if s.endswith("/USDT") and MIN_24H_VOLUME_USD<=t.get('quoteVolume',0)<=MAX_24H_VOLUME_USD]
    if not available:
        logging.error("No low-volume USDT symbols found")
        return

    # --- send startup message immediately ---
    logging.info(f"Found {len(available)} low-volume symbols. Starting rolling scan...")
    send_telegram(f"🤖 Bot started. Found {len(available)} low-volume USDT symbols. Starting rolling scan...")
    time.sleep(1)

    i = 0
    while True:
        sym = available[i % len(available)]
        process_symbol(sym)

        # Update open signals
        open_df = pd.read_csv(SIGNAL_LOG_FILE)
        for idx,row in open_df[open_df['Status']=="Open"].iterrows():
            tick = exchange.fetch_ticker(row['Symbol'])
            last_price = tick.get('last')
            if last_price: update_signal_status(row['Symbol'], row['Direction'], last_price)

        i += 1
        time.sleep(0.5)  # small pause to avoid rate limit

if __name__=="__main__":
    run()