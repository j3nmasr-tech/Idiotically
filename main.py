#!/usr/bin/env python3
"""
Advanced BingX Scalp Bot - Rate-Limited Full Version
Features:
- Scans all USDT spot pairs on BingX with 24h volume >= 5M USD
- Uses ATR + indicator + dynamic TP/SL based on momentum
- Safe, rate-limited worker queue (no freezes)
- Telegram notifications
- Logs signals and updates open trades
"""

import os, time, logging, json, threading
from datetime import datetime, timedelta
import requests
import pandas as pd
import ccxt
from queue import Queue
import numpy as np

# ---------- Config ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "120"))

EMA_SHORT = int(os.getenv("EMA_SHORT", "50"))
EMA_LONG  = int(os.getenv("EMA_LONG", "200"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
VOL_MULTIPLIER = float(os.getenv("VOL_MULTIPLIER", "1.6"))

MIN_AVG_VOL_USDT = float(os.getenv("MIN_AVG_VOL_USDT", "1000"))
MIN_CANDLES = int(os.getenv("MIN_CANDLES", "250"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))  # safe for 200+ symbols
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "60"))
MIN_24H_VOLUME_USD = 5_000_000

SIGNAL_LOG_FILE = os.getenv("SIGNAL_LOG_FILE", "signals_log.csv")
LAST_SIGNALS_FILE = os.getenv("LAST_SIGNALS_FILE", "last_signals.json")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- Locks ----------
file_lock = threading.Lock()
last_signals_lock = threading.Lock()

# ---------- CCXT ----------
exchange = ccxt.bingx({'enableRateLimit': True})

# ---------- Telegram ----------
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.debug("Telegram not configured")
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
    except Exception as e:
        logging.debug("fetch_ohlcv fail %s %s", symbol, e)
        return None

def ema(series, period): return series.ewm(span=period, adjust=False).mean()
def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1*delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))
def macd(series, fast=12, slow=26, signal=9):
    fast_ema = ema(series, fast)
    slow_ema = ema(series, slow)
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
    if direction=="BUY":
        cond_rsi, cond_macd = rsi_val>55, macd_hist_last>0
    else:
        cond_rsi, cond_macd = rsi_val<45, macd_hist_last<0
    cond_vol = last['volume']>(mean_vol20*VOL_MULTIPLIER) if mean_vol20>0 else False
    body_ratio = abs(last['close']-last['open'])/(last['high']-last['low']+1e-12)
    cond_body = body_ratio>0.3
    ok = cond_rsi and cond_macd and cond_vol and cond_body
    return ok, {"rsi":float(rsi_val),"macd_hist":float(macd_hist_last),"vol":float(last['volume']),"mean_vol20":float(mean_vol20),"body_ratio":float(body_ratio)}

# ---------- Persistent Signals ----------
def load_last_signals():
    with last_signals_lock:
        if not os.path.exists(LAST_SIGNALS_FILE): return {}
        try: return json.load(open(LAST_SIGNALS_FILE))
        except: return {}
def save_last_signals(data):
    with last_signals_lock: json.dump(data, open(LAST_SIGNALS_FILE,'w'))
def can_signal(symbol):
    last = load_last_signals()
    ts_str = last.get(symbol)
    if not ts_str: return True
    try:
        return datetime.utcnow()-datetime.fromisoformat(ts_str)>timedelta(minutes=SIGNAL_COOLDOWN_MINUTES)
    except: return True
def mark_signaled(symbol):
    last = load_last_signals()
    last[symbol] = datetime.utcnow().isoformat()
    save_last_signals(last)

# ---------- Signal Logging ----------
if not os.path.exists(SIGNAL_LOG_FILE):
    pd.DataFrame(columns=["Timestamp","Symbol","Direction","EntryPrice","TP1","TP2","TP3","SL","Status","ClosePrice","CloseTime"]).to_csv(SIGNAL_LOG_FILE,index=False)
def log_signal(data):
    with file_lock:
        df = pd.read_csv(SIGNAL_LOG_FILE)
        df = pd.concat([df,pd.DataFrame([data])],ignore_index=True)
        df.to_csv(SIGNAL_LOG_FILE,index=False)
def update_signal_status(symbol,direction,price):
    updated=False
    with file_lock:
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
                if status: df.at[idx,'Status']=status; df.at[idx,'ClosePrice']=price; df.at[idx,'CloseTime']=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"); updated=True
        if updated: df.to_csv(SIGNAL_LOG_FILE,index=False)
    return updated

# ---------- Signal Formatting ----------
def format_signal(symbol,direction,entry,tp1,tp2,tp3,sl):
    now=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return f"⚡ <b>SCALP SIGNAL: {direction}</b>\n🔹 Symbol: <b>{symbol}</b>\n⏱ Time: {now}\n💵 Entry Price: {entry:.8f}\n🎯 TP: {tp1:.8f} | {tp2:.8f} | {tp3:.8f}\n🛑 SL: {sl:.8f}\nℹ️ Status: Open"

# ---------- Worker ----------
def process_symbol(sym):
    try:
        df15=fetch_ohlcv(sym,"15m",MIN_CANDLES)
        df30=fetch_ohlcv(sym,"30m",50)
        if df15 is None or df30 is None or len(df15)<MIN_CANDLES: return
        avg_vol=(df30['close']*df30['volume']).tail(20).mean()
        if avg_vol<MIN_AVG_VOL_USDT: return
        trend15,trend30=detect_trend(df15),detect_trend(df30)
        if trend15!=trend30 or trend15 is None: return
        direction=trend15
        df5=fetch_ohlcv(sym,"5m",200)
        ok,_=check_entry_5m(df5,direction)
        if not ok: return
        entry=df5['close'].iloc[-1]
        atr_val=atr(df15)
        momentum_factor=min(abs(macd(df15['close'])[2].iloc[-1])/ (abs(macd(df15['close'])[2].iloc[-14:]).max()+1e-12),3)
        if direction=="BUY":
            sl=entry-atr_val; tp1=entry+1.5*atr_val*momentum_factor; tp2=entry+2.0*atr_val*momentum_factor; tp3=entry+3.0*atr_val*momentum_factor
        else:
            sl=entry+atr_val; tp1=entry-1.5*atr_val*momentum_factor; tp2=entry-2.0*atr_val*momentum_factor; tp3=entry-3.0*atr_val*momentum_factor
        if not can_signal(sym): return
        msg=format_signal(sym,direction,entry,tp1,tp2,tp3,sl)
        logging.info("Signal -> %s %s",sym,direction)
        send_telegram(msg)
        log_signal({"Timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),"Symbol":sym,"Direction":direction,"EntryPrice":float(entry),"TP1":float(tp1),"TP2":float(tp2),"TP3":float(tp3),"SL":float(sl),"Status":"Open","ClosePrice":"","CloseTime":""})
        mark_signaled(sym)
        time.sleep(0.3)  # rate-limit
    except Exception as e:
        logging.debug("process_symbol error %s %s", sym, e)

# ---------- Main Loop ----------
def run():
    logging.info("🤖 Bot starting. Loading markets...")
    exchange.load_markets()
    logging.info("Markets loaded. Fetching tickers...")

    # Use bulk fetch_tickers() for startup
    try:
        tickers = exchange.fetch_tickers()
    except Exception as e:
        logging.error("fetch_tickers failed: %s", e)
        return

    available=[s for s,t in tickers.items() if s.endswith("/USDT") and t.get('quoteVolume',0)>=MIN_24H_VOLUME_USD]
    if not available:
        logging.error("No high-volume USDT symbols found")
        return
    logging.info(f"Found {len(available)} high-volume symbols. Starting scan...")

    q=Queue()
    for s in available: q.put(s)

    def worker():
        while True:
            sym=q.get()
            if sym is None: break
            process_symbol(sym)
            q.task_done()

    threads=[threading.Thread(target=worker) for _ in range(MAX_WORKERS)]
    for t in threads: t.daemon=True; t.start()

    try:
        while True:
            time.sleep(POLL_INTERVAL)
            # check open signals
            with file_lock:
                try: open_df=pd.read_csv(SIGNAL_LOG_FILE)
                except: open_df=pd.DataFrame()
            if not open_df.empty:
                for idx,row in open_df[open_df['Status']=="Open"].iterrows():
                    sym=row['Symbol']; direction=row['Direction']
                    try:
                        tick=exchange.fetch_ticker(sym)
                        last_price=tick.get('last')
                        if last_price and update_signal_status(sym,direction,last_price):
                            send_telegram(f"⚡ Update: {sym} {direction} status updated at price {last_price}")
                    except: continue
            # refill queue for next round
            for s in available: q.put(s)

    except KeyboardInterrupt:
        logging.info("Shutting down...")
        for _ in threads: q.put(None)
        for t in threads: t.join()

if __name__=="__main__":
    run()