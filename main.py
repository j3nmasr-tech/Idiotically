#!/usr/bin/env python3
"""
Upgraded BingX scalp scanner.
Features:
- Scans ALL BingX native symbols that end with "/USDT" (spot).
- Skips newly-listed / dead pairs by checking candle availability.
- Filters by minimum average quote-volume (USDT) over recent candles.
- Adds a simple momentum filter (MACD) before signaling.
- Uses ThreadPoolExecutor to parallelize symbol processing (configurable).
- Persists last-signals to last_signals.json to avoid duplicates across restarts.
- Uses locks for safe file writes.

Config (via ENV):
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- POLL_INTERVAL (seconds, default 120)
- EMA_SHORT (default 50), EMA_LONG (default 200)
- RSI_PERIOD (default 14)
- VOL_MULTIPLIER (default 1.6)
- MIN_AVG_VOL_USDT (default 1000)  --> skip pairs with avg quote vol < this
- MIN_CANDLES (default 250)       --> require at least this many 15m candles for long EMAs
- MAX_WORKERS (default 6)         --> thread pool size (be conservative)
- SIGNAL_COOLDOWN_MINUTES (default 60) --> do not re-signal same symbol within this many minutes
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

SIGNAL_LOG_FILE = os.getenv("SIGNAL_LOG_FILE", "signals_log.csv")
LAST_SIGNALS_FILE = os.getenv("LAST_SIGNALS_FILE", "last_signals.json")

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- Globals & Locks ----------
file_lock = threading.Lock()
last_signals_lock = threading.Lock()

# ---------- CCXT BingX (shared) ----------
exchange = ccxt.bingx({'enableRateLimit': True})
# we will call exchange.load_markets() at runtime (in run())

# ---------- HTTP helper ----------
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.debug("Telegram token/chat not provided — skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode":"HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning("Telegram send failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("Failed to send telegram message: %s", e)

# ---------- OHLCV fetcher ----------
def fetch_ohlcv_local(symbol: str, timeframe: str, limit: int = 300):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        if df.empty:
            return None
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
    down = -1 * delta.clip(upper=0)
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

# ---------- Strategy helpers ----------
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
    else:
        return None

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

# ---------- Persistent signal memory ----------
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

def can_signal(symbol, candle_ts):
    """
    Prevent duplicate signals within cooldown window.
    candle_ts: pandas.Timestamp or str
    """
    last = load_last_signals()
    ks = last.get(symbol)
    if not ks:
        return True
    try:
        prev_ts = datetime.fromisoformat(ks)
        # require cooldown minutes
        if candle_ts.tzinfo is not None:
            candle_ts = candle_ts.tz_convert(None)
        if prev_ts + timedelta(minutes=SIGNAL_COOLDOWN_MINUTES) <= datetime.utcnow():
            return True
        return False
    except Exception:
        return True

def mark_signaled(symbol, candle_ts):
    last = load_last_signals()
    # store ISO format in UTC
    ts_str = datetime.utcnow().isoformat()
    last[symbol] = ts_str
    save_last_signals(last)

# ---------- Signal Log ----------
if not os.path.exists(SIGNAL_LOG_FILE):
    df_init = pd.DataFrame(columns=[
        "Timestamp","Symbol","Direction","EntryPrice","TP1","TP2","TP3","SL","Status","ClosePrice","CloseTime"
    ])
    df_init.to_csv(SIGNAL_LOG_FILE, index=False)

def log_signal_threadsafe(data: dict):
    with file_lock:
        df = pd.read_csv(SIGNAL_LOG_FILE)
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        df.to_csv(SIGNAL_LOG_FILE, index=False)

def update_signal_status_threadsafe(symbol, direction, price):
    updated = False
    with file_lock:
        df = pd.read_csv(SIGNAL_LOG_FILE)
        for idx, row in df.iterrows():
            if row['Symbol']==symbol and row['Direction']==direction and row['Status']=="Open":
                tp1 = row['TP1']
                tp2 = row['TP2']
                tp3 = row['TP3']
                sl  = row['SL']
                status = None
                if direction=="BUY":
                    if price >= tp3: status="TP3 Hit"
                    elif price >= tp2: status="TP2 Hit"
                    elif price >= tp1: status="TP1 Hit"
                    elif price <= sl: status="SL Hit"
                else:
                    if price <= tp3: status="TP3 Hit"
                    elif price <= tp2: status="TP2 Hit"
                    elif price <= tp1: status="TP1 Hit"
                    elif price >= sl: status="SL Hit"
                if status:
                    df.at[idx,'Status'] = status
                    df.at[idx,'ClosePrice'] = price
                    df.at[idx,'CloseTime']  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    updated=True
        if updated:
            df.to_csv(SIGNAL_LOG_FILE, index=False)
    return updated

# ---------- Message formatting ----------
def format_signal(symbol, direction, entry_price, tp1, tp2, tp3, sl):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    msg = (
        f"⚡ <b>SCALP SIGNAL: {direction}</b>\n"
        f"🔹 Symbol: <b>{symbol}</b>\n"
        f"⏱ Time: {now}\n"
        f"💵 Entry Price: {entry_price:.8f}\n"
        f"🎯 Targets: TP1 {tp1:.8f} | TP2 {tp2:.8f} | TP3 {tp3:.8f}\n"
        f"🛑 Stop Loss: {sl:.8f}\n"
        f"ℹ️ Status: Open"
    )
    return msg

# ---------- Per-symbol worker ----------
def process_symbol(sym):
    """
    Process one symbol: fetch candles, check filters, compute signals.
    Returns True/False if a signal was sent.
    """
    try:
        # get 15m, 30m, 5m
        df15 = fetch_ohlcv_local(sym, "15m", limit=MAX(300, MIN_CANDLES))
    except Exception:
        df15 = fetch_ohlcv_local(sym, "15m", limit=300)
    if df15 is None or df15.empty or len(df15) < MIN_CANDLES:
        return False

    # compute average quote-volume on 30m (approx)
    df30 = fetch_ohlcv_local(sym, "30m", limit=50)
    if df30 is None or df30.empty:
        return False
    # estimate quote volume = close * volume (approx)
    recent = df30.tail(20)
    avg_quote_vol = (recent['close'] * recent['volume']).mean()
    if avg_quote_vol is None or avg_quote_vol < MIN_AVG_VOL_USDT:
        # skip illiquid
        return False

    # trend detection on 15m and 30m
    trend15 = detect_trend(df15)
    trend30 = detect_trend(df30)
    if trend15 is None or trend30 is None or trend15 != trend30:
        return False
    direction = trend15

    # 5m check
    df5 = fetch_ohlcv_local(sym, "5m", limit=200)
    ok, details_5m = check_entry_5m(df5, direction)
    if not ok:
        return False

    # momentum factor (MACD on 15m)
    macd_line, macd_signal, macd_hist = macd(df15['close'])
    if macd_hist is None or len(macd_hist) < 2:
        return False
    # momentum factor normalized
    denom = (abs(macd_hist[-14:]).max() if len(macd_hist) >= 14 else abs(macd_hist).max()) + 1e-12
    momentum_factor = min(abs(macd_hist.iloc[-1]) / denom, 3)

    entry_price = df5['close'].iloc[-1]
    atr_val = atr(df15)

    if direction == "BUY":
        sl = entry_price - atr_val
        tp1 = entry_price + 1.5 * atr_val * momentum_factor
        tp2 = entry_price + 2.0 * atr_val * momentum_factor
        tp3 = entry_price + 3.0 * atr_val * momentum_factor
    else:
        sl = entry_price + atr_val
        tp1 = entry_price - 1.5 * atr_val * momentum_factor
        tp2 = entry_price - 2.0 * atr_val * momentum_factor
        tp3 = entry_price - 3.0 * atr_val * momentum_factor

    # avoid duplicates within cooldown
    candle_ts = df5['ts'].iloc[-1]
    if not can_signal(sym, candle_ts):
        return False

    # final small sanity checks: TP and SL must be sensible distances
    if direction == "BUY":
        if not (tp1 > entry_price and sl < entry_price):
            return False
    else:
        if not (tp1 < entry_price and sl > entry_price):
            return False

    # ok -> send signal
    msg = format_signal(sym, direction, float(entry_price), float(tp1), float(tp2), float(tp3), float(sl))
    logging.info("Signal -> %s %s (avg_quote_vol=%.2f)", sym, direction, avg_quote_vol)
    send_telegram(msg)
    log_signal_threadsafe({
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
    mark_signaled(sym, candle_ts)
    return True

# helper: safe MAX (since used above)
def MAX(a, b):
    return a if a >= b else b

# ---------- Main Loop ----------
def run():
    send_telegram("🤖 Bot started. Scanning ALL BingX /USDT spot symbols.")
    exchange.load_markets()
    # use native BingX format and only spot USDT pairs
    available = [s for s in exchange.symbols if s.endswith("/USDT") and exchange.markets.get(s)]
    if not available:
        logging.error("No USDT symbols found on BingX.")
        return
    logging.info("Found %d USDT symbols on BingX. Starting scanning (workers=%d)...", len(available), MAX_WORKERS)

    # Thread pool executor for per-symbol scanning
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        while True:
            futures = {}
            start = time.time()
            # submit tasks in batches
            for sym in available:
                futures[executor.submit(process_symbol, sym)] = sym

            # gather results (we don't need details per-symbol beyond logging)
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    res = fut.result()
                    # res True if a signal was sent for that symbol
                    if res:
                        logging.info("Signalled %s", sym)
                except Exception as e:
                    logging.debug("Worker error for %s: %s", sym, e)

            # After scanning, check open signals status prices (single-threaded)
            # Update any open signals based on last price
            with file_lock:
                try:
                    open_df = pd.read_csv(SIGNAL_LOG_FILE)
                except Exception:
                    open_df = pd.DataFrame()
            if not open_df.empty:
                # iterate unique open symbols
                open_sym_rows = open_df[open_df['Status']=="Open"]
                unique_open_syms = open_sym_rows['Symbol'].unique().tolist() if not open_sym_rows.empty else []
                for sym in unique_open_syms:
                    try:
                        tick = exchange.fetch_ticker(sym)
                        last_price = tick.get('last')
                        if last_price is None:
                            continue
                        if update_signal_status_threadsafe(sym, open_sym_rows[open_sym_rows['Symbol']==sym]['Direction'].iloc[0], last_price):
                            send_telegram(f"⚡ Update: {sym} status updated based on current price {last_price}")
                    except Exception:
                        continue

            elapsed = time.time() - start
            sleep_for = max(0, POLL_INTERVAL - elapsed)
            logging.info("Scan cycle done in %.1fs. Sleeping %.1fs.", elapsed, sleep_for)
            # Sleep (main loop)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down.")
    finally:
        executor.shutdown(wait=False)
        logging.info("Exited.")

if __name__ == "__main__":
    run()