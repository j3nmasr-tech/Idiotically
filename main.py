#!/usr/bin/env python3
"""
Advanced BingX Scalp Bot - Full Version
Features:
- Scans all USDT spot pairs on BingX with 24h volume >= 5M USD
- Uses ATR + indicator + support/resistance to dynamically extend TP
- Tracks open signals and uses cooldown to prevent duplicates
- Multi-threaded scanning (MAX_WORKERS adjustable)
- Telegram notifications
"""

import os
import time
import logging
import json
import threading
from datetime import datetime, timedelta
import requests
import pandas as pd
import ccxt
from concurrent.futures import ThreadPoolExecutor, as_completed
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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "60"))
MIN_24H_VOLUME_USD = 5_000_000  # Only pairs with >=5M USD 24h volume

SIGNAL_LOG_FILE = os.getenv("SIGNAL_LOG_FILE", "signals_log.csv")
LAST_SIGNALS_FILE = os.getenv("LAST_SIGNALS_FILE", "last_signals.json")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- Globals & Locks ----------
file_lock = threading.Lock()
last_signals_lock = threading.Lock()

# ---------- CCXT BingX ----------
exchange = ccxt.bingx({'enableRateLimit': True})

# ---------- Telegram Helper ----------
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

# ---------- OHLCV fetcher ----------
def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 300):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not raw:
            return None
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except Exception as e:
        logging.debug("fetch_ohlcv fail %s %s", symbol, e)
        return None

# ---------- Indicators ----------
def ema(series: pd.Series, period: int):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1*delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

def macd(series: pd.Series, fast=12, slow=26, signal=9):
    fast_ema = ema(series, fast)
    slow_ema = ema(series, slow)
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def atr(df: pd.DataFrame, period: int = 14):
    high = df['high']
    low = df['low']
    close = df['close']
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean().iloc[-1]

# ---------- Trend Detection ----------
def detect_trend(df: pd.DataFrame):
    if df is None or len(df) < EMA_LONG:
        return None
    close = df['close']
    e_short = ema(close, EMA_SHORT).iloc[-1]
    e_long = ema(close, EMA_LONG).iloc[-1]
    if e_short > e_long:
        return "BUY"
    elif e_short < e_long:
        return "SELL"
    return None

# ---------- Entry Condition 5m ----------
def check_entry_5m(df5: pd.DataFrame, direction: str):
    if df5 is None or len(df5) < 30:
        return False, {}
    close = df5['close']
    vol = df5['volume']
    last = df5.iloc[-1]
    mean_vol20 = vol[-21:-1].mean() if len(vol) >= 21 else vol.mean()
    rsi_val = rsi(close, RSI_PERIOD).iloc[-1]
    macd_line, macd_sig, macd_hist = macd(close)
    macd_hist_last = macd_hist.iloc[-1]
    if direction == "BUY":
        cond_rsi = rsi_val > 55
        cond_macd = macd_hist_last > 0
    else:
        cond_rsi = rsi_val < 45
        cond_macd = macd_hist_last < 0
    cond_vol = False
    if mean_vol20 > 0:
        cond_vol = last['volume'] > (mean_vol20 * VOL_MULTIPLIER)
    candle_body = abs(last['close'] - last['open'])
    candle_range = last['high'] - last['low'] + 1e-12
    body_ratio = candle_body / candle_range
    cond_body = body_ratio > 0.3
    details = {
        "rsi": float(rsi_val),
        "macd_hist": float(macd_hist_last),
        "vol": float(last['volume']),
        "mean_vol20": float(mean_vol20),
        "body_ratio": float(body_ratio)
    }
    ok = cond_rsi and cond_macd and cond_vol and cond_body
    return ok, details

# ---------- Persistent Signal Memory ----------
def load_last_signals():
    with last_signals_lock:
        if not os.path.exists(LAST_SIGNALS_FILE):
            return {}
        try:
            with open(LAST_SIGNALS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

def save_last_signals(data):
    with last_signals_lock:
        with open(LAST_SIGNALS_FILE, 'w') as f:
            json.dump(data, f)

def can_signal(symbol):
    last = load_last_signals()
    ts_str = last.get(symbol)
    if not ts_str:
        return True
    try:
        prev_ts = datetime.fromisoformat(ts_str)
        return datetime.utcnow() - prev_ts > timedelta(minutes=SIGNAL_COOLDOWN_MINUTES)
    except Exception:
        return True

def mark_signaled(symbol):
    last = load_last_signals()
    last[symbol] = datetime.utcnow().isoformat()
    save_last_signals(last)

# ---------- Signal Log ----------
if not os.path.exists(SIGNAL_LOG_FILE):
    df_init = pd.DataFrame(columns=[
        "Timestamp","Symbol","Direction","EntryPrice","TP1","TP2","TP3","SL","Status","ClosePrice","CloseTime"
    ])
    df_init.to_csv(SIGNAL_LOG_FILE, index=False)

def log_signal(data: dict):
    with file_lock:
        df = pd.read_csv(SIGNAL_LOG_FILE)
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        df.to_csv(SIGNAL_LOG_FILE, index=False)

def update_signal_status(symbol, direction, price):
    updated = False
    with file_lock:
        df = pd.read_csv(SIGNAL_LOG_FILE)
        for idx, row in df.iterrows():
            if row['Symbol']==symbol and row['Direction']==direction and row['Status']=="Open":
                sl = row['SL']
                tp_dynamic = row.get('TP3', row['TP3'])
                status = None
                # dynamic ATR extension for strong moves
                if direction=="BUY":
                    if price >= tp_dynamic:
                        status = "TP Hit"
                    elif price <= sl:
                        status = "SL Hit"
                else:
                    if price <= tp_dynamic:
                        status = "TP Hit"
                    elif price >= sl:
                        status = "SL Hit"
                if status:
                    df.at[idx,'Status'] = status
                    df.at[idx,'ClosePrice'] = price
                    df.at[idx,'CloseTime']  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    updated = True
        if updated:
            df.to_csv(SIGNAL_LOG_FILE, index=False)
    return updated

# ---------- Signal Formatting ----------
def format_signal(symbol, direction, entry_price, tp1, tp2, tp3, sl):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    msg = (
        f"⚡ <b>SCALP SIGNAL: {direction}</b>\n"
        f"🔹 Symbol: <b>{symbol}</b>\n"
        f"⏱ Time: {now}\n"
        f"💵 Entry Price: {entry_price:.8f}\n"
        f"🎯 TP: {tp1:.8f} | {tp2:.8f} | {tp3:.8f}\n"
        f"🛑 SL: {sl:.8f}\n"
        f"ℹ️ Status: Open"
    )
    return msg

# ---------- Symbol Processor ----------
def process_symbol(sym):
    df15 = fetch_ohlcv(sym, "15m", MIN_CANDLES)
    if df15 is None or len(df15) < MIN_CANDLES:
        return False

    df30 = fetch_ohlcv(sym, "30m", 50)
    if df30 is None or df30.empty:
        return False

    # Average quote volume filter
    avg_quote_vol = (df30['close'] * df30['volume']).tail(20).mean()
    if avg_quote_vol < MIN_AVG_VOL_USDT:
        return False

    trend15 = detect_trend(df15)
    trend30 = detect_trend(df30)
    if trend15 is None or trend30 is None or trend15 != trend30:
        return False
    direction = trend15

    df5 = fetch_ohlcv(sym, "5m", 200)
    ok, _ = check_entry_5m(df5, direction)
    if not ok:
        return False

    entry_price = df5['close'].iloc[-1]
    atr_val = atr(df15)

    # Dynamic ATR multipliers for strong moves
    momentum_factor = min(abs(macd(df15['close'])[2].iloc[-1]) / (abs(macd(df15['close'])[2].iloc[-14:]).max() + 1e-12), 3)

    if direction == "BUY":
        sl = entry_price - atr_val
        tp1 = entry_price + 1.5*atr_val*momentum_factor
        tp2 = entry_price + 2.0*atr_val*momentum_factor
        tp3 = entry_price + 3.0*atr_val*momentum_factor  # dynamic extension possible
    else:
        sl = entry_price + atr_val
        tp1 = entry_price - 1.5*atr_val*momentum_factor
        tp2 = entry_price - 2.0*atr_val*momentum_factor
        tp3 = entry_price - 3.0*atr_val*momentum_factor  # dynamic extension possible

    if not can_signal(sym):
        return False

    msg = format_signal(sym, direction, entry_price, tp1, tp2, tp3, sl)
    logging.info("Signal -> %s %s", sym, direction)
    send_telegram(msg)
    log_signal({
        "Timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "Symbol": sym,
        "Direction": direction,
        "EntryPrice": float(entry_price),
        "TP1": float(tp1),
        "TP2": float(tp2),
        "TP3": float(tp3),
        "SL": float(sl),
        "Status": "Open",
        "ClosePrice": "",
        "CloseTime": ""
    })
    mark_signaled(sym)
    return True

# ---------- Main Loop ----------
def run():
    send_telegram("🤖 Bot started. Scanning high-volume BingX /USDT symbols.")
    exchange.load_markets()

    # Load high-volume symbols
    available = []
    for symbol, market in exchange.markets.items():
        if not symbol.endswith("/USDT"):
            continue
        try:
            ticker = exchange.fetch_ticker(symbol)
            quote_volume = ticker.get('quoteVolume') or 0
            if quote_volume >= MIN_24H_VOLUME_USD:
                available.append(symbol)
        except Exception:
            continue

    if not available:
        logging.error("No high-volume USDT symbols found.")
        return

    logging.info(f"Found {len(available)} symbols. Scanning with {MAX_WORKERS} workers...")

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        while True:
            futures = {executor.submit(process_symbol, s): s for s in available}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    logging.debug("Worker error: %s", e)

            # Check open signals for TP/SL updates
            with file_lock:
                try:
                    open_df = pd.read_csv(SIGNAL_LOG_FILE)
                except Exception:
                    open_df = pd.DataFrame()
            if not open_df.empty:
                open_rows = open_df[open_df['Status']=="Open"]
                for idx, row in open_rows.iterrows():
                    sym = row['Symbol']
                    direction = row['Direction']
                    try:
                        tick = exchange.fetch_ticker(sym)
                        last_price = tick.get('last')
                        if last_price is None:
                            continue
                        if update_signal_status(sym, direction, last_price):
                            send_telegram(f"⚡ Update: {sym} {direction} status updated at price {last_price}")
                    except Exception:
                        continue

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down.")
    finally:
        executor.shutdown(wait=False)
        logging.info("Exited.")

if __name__ == "__main__":
    run()