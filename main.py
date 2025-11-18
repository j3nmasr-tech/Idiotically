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
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "120"))  # كل دقيقتين

EMA_SHORT = int(os.getenv("EMA_SHORT", "50"))
EMA_LONG  = int(os.getenv("EMA_LONG", "200"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
VOL_MULTIPLIER = float(os.getenv("VOL_MULTIPLIER", "1.6"))

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- CCXT BingX ----------
exchange = ccxt.bingx({'enableRateLimit': True})
exchange.load_markets()
EXCHANGE_SYMBOLS = set(exchange.symbols)  # جميع الرموز الموجودة على BingX

# ---------- Load custom symbols ----------
def load_custom_symbols(path="symbols.txt", top_n=500):
    symbols = []
    with open(path, "r") as f:
        for line in f:
            sym = line.strip().replace("/", "")  # remove slash if needed
            if sym in EXCHANGE_SYMBOLS:
                symbols.append(sym)
            if len(symbols) >= top_n:
                break
    return symbols

FIXED_SYMBOLS = load_custom_symbols()
logging.info("Using %d symbols from custom list", len(FIXED_SYMBOLS))

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
    except Exception as e:
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
    cond_vol = last['volume'] > (mean_vol20 * VOL_MULTIPLIER) if mean_vol20>0 else False
    candle_body = abs(last['close'] - last['open'])
    candle_range = last['high'] - last['low'] + 1e-12
    cond_body = (candle_body / candle_range) > 0.3
    ok = cond_rsi and cond_macd and cond_vol and cond_body
    details = {"rsi": rsi_val, "macd_hist": macd_hist_last, "vol": last['volume'], "mean_vol20": mean_vol20}
    return ok, details

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

# ---------- Main Loop ----------
def run():
    send_telegram("🤖 Bot started. Scanning custom symbols on BingX.")
    seen_signals = set()
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
                sl, tp1, tp2, tp3 = (entry_price*0.997, entry_price*1.01, entry_price*1.02, entry_price*1.03) if direction=="BUY" else (entry_price*1.003, entry_price*0.99, entry_price*0.98, entry_price*0.97)
                key = f"{sym}|{direction}|{df5['ts'].iloc[-1]}"
                if key in seen_signals:
                    continue
                msg = format_signal(sym, direction, entry_price, tp1, tp2, tp3, sl)
                send_telegram(msg)
                log_signal({
                    "Timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "Symbol": sym, "Direction": direction, "EntryPrice": entry_price,
                    "TP1": tp1, "TP2": tp2, "TP3": tp3, "SL": sl,
                    "Status": "Open", "ClosePrice": "", "CloseTime": ""
                })
                seen_signals.add(key)
                tick = exchange.fetch_ticker(sym)
                last_price = tick['last']
                if update_signal_status(sym, direction, last_price):
                    send_telegram(f"⚡ Update: {sym} {direction} status updated. Last Price: {last_price:.6f}")
                time.sleep(0.2)
        except Exception as e:
            logging.exception("Main loop error: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run()