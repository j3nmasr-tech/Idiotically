import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd
import ccxt

# ---------- Config ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = 120  # every 2 minutes

EMA_SHORT = 50
EMA_LONG = 200
RSI_PERIOD = 14
VOL_MULTIPLIER = 1.6

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- CCXT BingX ----------
exchange = ccxt.bingx({'enableRateLimit': True})
exchange.load_markets()

# ---------- Scan all valid USDT symbols ----------
FIXED_SYMBOLS = [s for s in exchange.symbols if s.endswith("USDT")]
logging.info("Scanning %d USDT symbols on BingX", len(FIXED_SYMBOLS))

# ---------- Telegram helper ----------
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram token/chat not provided — skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode":"HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.warning("Telegram send failed: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("Failed to send telegram message: %s", e)

# ---------- Fetch OHLCV ----------
def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 300):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except ccxt.BaseError as e:
        if "100204" in str(e):
            logging.warning("Symbol not found on BingX, skipping: %s", symbol)
            return None
        logging.warning("fetch_ohlcv fail %s %s", symbol, e)
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
    cond_vol = last['volume'] > (mean_vol20 * VOL_MULTIPLIER) if mean_vol20>0 else False
    candle_body = abs(last['close'] - last['open'])
    candle_range = last['high'] - last['low'] + 1e-12
    cond_body = (candle_body / candle_range) > 0.3
    ok = cond_rsi and cond_macd and cond_vol and cond_body
    return ok, {"rsi": rsi_val, "macd_hist": macd_hist_last, "vol": last['volume'], "mean_vol20": mean_vol20}

# ---------- ATR ----------
def calculate_atr(df: pd.DataFrame, period: int = 14):
    if df is None or len(df) < period+1:
        return None
    high = df['high']
    low  = df['low']
    close = df['close']
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr.iloc[-1]

# ---------- Pivot Points ----------
def pivot_points(df: pd.DataFrame):
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-2]  # previous candle
    high, low, close = last['high'], last['low'], last['close']
    pivot = (high + low + close) / 3
    r1 = 2*pivot - low
    r2 = pivot + (high - low)
    r3 = r1 + (high - low)
    s1 = 2*pivot - high
    s2 = pivot - (high - low)
    s3 = s1 - (high - low)
    return {"PP": pivot, "R1": r1, "R2": r2, "R3": r3, "S1": s1, "S2": s2, "S3": s3}

# ---------- Signal Log ----------
SIGNAL_LOG_FILE = "signals_log.csv"
if not os.path.exists(SIGNAL_LOG_FILE):
    df_init = pd.DataFrame(columns=[
        "Timestamp","Symbol","Direction","EntryPrice","TP1","TP2","TP3","SL","Status","ClosePrice","CloseTime"
    ])
    df_init.to_csv(SIGNAL_LOG_FILE, index=False)

def log_signal(data: dict):
    df = pd.read_csv(SIGNAL_LOG_FILE)
    df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    df.to_csv(SIGNAL_LOG_FILE, index=False)

def update_signal_status(symbol, direction, price):
    df = pd.read_csv(SIGNAL_LOG_FILE)
    updated = False
    for idx, row in df.iterrows():
        if row['Symbol']==symbol and row['Direction']==direction and row['Status']=="Open":
            tp1, tp2, tp3, sl = row['TP1'], row['TP2'], row['TP3'], row['SL']
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
                df.at[idx,'CloseTime'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                updated = True
    if updated:
        df.to_csv(SIGNAL_LOG_FILE, index=False)
    return updated

# ---------- Telegram Message ----------
def format_signal(symbol, direction, entry_price, tp1, tp2, tp3, sl):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"⚡ <b>SCALP SIGNAL: {direction}</b>\n"
        f"🔹 Symbol: <b>{symbol}</b>\n"
        f"⏱ Time: {now}\n"
        f"💵 Entry Price: {entry_price:.6f}\n"
        f"🎯 Targets: TP1 {tp1:.6f} | TP2 {tp2:.6f} | TP3 {tp3:.6f}\n"
        f"🛑 Stop Loss: {sl:.6f}\n"
        f"ℹ️ Status: Open"
    )

# ---------- Daily Summary ----------
def send_daily_summary():
    if not os.path.exists(SIGNAL_LOG_FILE):
        return
    df = pd.read_csv(SIGNAL_LOG_FILE)
    today = datetime.utcnow().date()
    df_today = df[df['Timestamp'].str.startswith(str(today))]
    if df_today.empty:
        return
    total = len(df_today)
    tp1_hits = len(df_today[df_today['Status']=="TP1 Hit"])
    tp2_hits = len(df_today[df_today['Status']=="TP2 Hit"])
    tp3_hits = len(df_today[df_today['Status']=="TP3 Hit"])
    sl_hits  = len(df_today[df_today['Status']=="SL Hit"])
    msg = (f"📊 <b>Daily Summary ({today})</b>\n"
           f"Total Signals: {total}\nTP1 Hits: {tp1_hits}\nTP2 Hits: {tp2_hits}\n"
           f"TP3 Hits: {tp3_hits}\nSL Hits: {sl_hits}")
    send_telegram(msg)

# ---------- Main Loop ----------
def run():
    send_telegram("🤖 Bot started. Scanning all valid USDT symbols on BingX every 2 minutes.")
    seen_signals = set()
    last_summary_day = datetime.utcnow().day
    while True:
        try:
            for sym in FIXED_SYMBOLS:
                df15 = fetch_ohlcv(sym, "15m", 300)
                df30 = fetch_ohlcv(sym, "30m", 300)
                if df15 is None or df30 is None:
                    continue
                trend15, trend30 = detect_trend(df15), detect_trend(df30)
                if trend15 != trend30 or trend15 is None:
                    continue
                direction = trend15
                df5 = fetch_ohlcv(sym, "5m", 200)
                ok, _ = check_entry_5m(df5, direction)
                if not ok:
                    continue
                entry_price = df5['close'].iloc[-1]

                # ----------------- DYNAMIC TP/SL with ATR + Pivot Points -----------------
                atr = calculate_atr(df15)
                if atr is None:
                    atr = entry_price * 0.01
                tp_mult1,tp_mult2,tp_mult3,sl_mult = 2,3,4,1

                macd_line, macd_signal, macd_hist = macd(df15['close'])
                macd_slope = macd_hist.iloc[-1] - macd_hist.iloc[-2]
                if direction=="BUY":
                    if macd_slope>0: tp_mult1,tp_mult2,tp_mult3 = tp_mult1*1.2,tp_mult2*1.2,tp_mult3*1.2
                    else: tp_mult1,tp_mult2,tp_mult3 = tp_mult1*0.8,tp_mult2*0.8,tp_mult3*0.8
                else:
                    if macd_slope<0: tp_mult1,tp_mult2,tp_mult3 = tp_mult1*1.2,tp_mult2*1.2,tp_mult3*1.2
                    else: tp_mult1,tp_mult2,tp_mult3 = tp_mult1*0.8,tp_mult2*0.8,tp_mult3*0.8

                pivots = pivot_points(df15)
                if pivots:
                    if direction=="BUY":
                        tp1 = min(entry_price + tp_mult1*atr, pivots["R1"])
                        tp2 = min(entry_price + tp_mult2*atr, pivots["R2"])
                        tp3 = min(entry_price + tp_mult3*atr, pivots["R3"])
                        sl  = max(entry_price - sl_mult*atr, pivots["S1"])
                    else:
                        tp1 = max(entry_price - tp_mult1*atr, pivots["S1"])
                        tp2 = max(entry_price - tp_mult2*atr, pivots["S2"])
                        tp3 = max(entry_price - tp_mult3*atr, pivots["S3"])
                        sl  = min(entry_price + sl_mult*atr, pivots["R1"])
                else:
                    if direction=="BUY":
                        tp1,tp2,tp3 = entry_price+tp_mult1*atr, entry_price+tp_mult2*atr, entry_price+tp_mult3*atr
                        sl = entry_price - sl_mult*atr
                    else:
                        tp1,tp2,tp3 = entry_price-tp_mult1*atr, entry_price-tp_mult2*atr, entry_price-tp_mult3*atr
                        sl = entry_price + sl_mult*atr
                # -------------------------------------------------------------------------

                key = f"{sym}|{direction}|{df5['ts'].iloc[-1]}"
                if key not in seen_signals:
                    msg = format_signal(sym, direction, entry_price, tp1, tp2, tp3, sl)
                    send_telegram(msg)
                    log_signal({
                        "Timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "Symbol": sym,
                        "Direction": direction,
                        "EntryPrice": entry_price,
                        "TP1": tp1,
                        "TP2": tp2,
                        "TP3": tp3,
                        "SL": sl,
                        "Status": "Open",
                        "ClosePrice": "",
                        "CloseTime": ""
                    })
                    seen_signals.add(key)

                tick = exchange.fetch_ticker(sym)
                last_price = tick['last']
                if update_signal_status(sym, direction, last_price):
                    send_telegram(f"⚡ Update: {sym} {direction} status updated. Last Price: {last_price:.6f}")

            # Daily summary at UTC 23:59
            now = datetime.utcnow()
            if now.day != last_summary_day and now.hour == 23 and now.minute >= 59:
                send_daily_summary()
                last_summary_day = now.day
        except Exception as e:
            logging.exception("Main loop error: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run()