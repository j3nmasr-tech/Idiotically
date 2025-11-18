import os
import time
import logging
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import ccxt

# ---------- config ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")  
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))  # seconds

EMA_SHORT = int(os.getenv("EMA_SHORT", "50"))
EMA_LONG  = int(os.getenv("EMA_LONG", "200"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
VOL_MULTIPLIER = float(os.getenv("VOL_MULTIPLIER", "1.6"))  
LOW_VOLUME_USDT = float(os.getenv("LOW_VOLUME_USDT", "5000000"))  # <5M USDT 24h considered low vol

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ---------- ccxt BingX ----------
exchange = ccxt.bingx({'enableRateLimit': True})

# ---------- helpers ----------
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

def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 300):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except Exception as e:
        logging.warning("fetch_ohlcv fail %s %s", symbol, e)
        return None

# indicators
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

def get_low_vol_meme_symbols():
    """Fetch markets, filter meme / low volume / exclude majors, keep only existing symbols"""
    majors = ["BTC","ETH","BNB","SOL","XRP","ADA","DOGE","LTC","TON","DOT","LINK"]
    try:
        exchange.load_markets()
        all_symbols = exchange.symbols  # جميع الرموز الموجودة فعليًا
        markets = exchange.fetch_markets()
        usdt_pairs = [m for m in markets if m['quote'] == 'USDT']
        filtered = []
        for m in usdt_pairs:
            base = m['base']
            symbol = m['symbol']
            quote_vol = float(m.get('info', {}).get('quoteVolume', 0) or 0)
            if base not in majors and quote_vol < LOW_VOLUME_USDT and symbol in all_symbols:
                filtered.append(symbol)
        return filtered
    except Exception as e:
        logging.exception("Failed to fetch low-vol meme symbols: %s", e)
        return []

def format_signal(symbol, direction, details_15_30, details_5m):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    conf = []
    conf.append(f"Trend15/30: {details_15_30}")
    conf.append(f"5m: RSI {details_5m.get('rsi'):.1f}, MACDhist {details_5m.get('macd_hist'):.6f}, Vol {details_5m.get('vol'):.0f}")
    msg = (
        f"⚡ <b>SCALP {direction}</b>\n"
        f"🔹 Symbol: <b>{symbol}</b>\n"
        f"⏱ Checked: {now}\n"
        f"📈 Reason:\n" + "\n".join(conf) + "\n\n"
        f"🎯 Suggested Targets: TP1 1%, TP2 2%, TP3 3%+\n"
        f"🛑 Suggested SL: 0.3%-0.6%\n"
        f"ℹ️ Signal only — no orders placed"
    )
    return msg

# ---------- main loop ----------
def run():
    send_telegram("🤖 Bot started successfully. Scanning low-vol meme coins on BingX...")
    symbols = get_low_vol_meme_symbols()
    if not symbols:
        logging.error("No low-vol meme symbols found. Exiting.")
        return
    logging.info("Scanning %d meme/low-vol symbols every %ds", len(symbols), POLL_INTERVAL)
    seen_signals = set()
    while True:
        try:
            for sym in symbols:
                df15 = fetch_ohlcv(sym, "15m", 300)
                df30 = fetch_ohlcv(sym, "30m", 300)
                if df15 is None or df30 is None:
                    continue
                trend15 = detect_trend(df15)
                trend30 = detect_trend(df30)
                if trend15 is None or trend30 is None or trend15 != trend30:
                    continue
                direction = trend15
                df5 = fetch_ohlcv(sym, "5m", 200)
                ok, details_5m = check_entry_5m(df5, direction)
                if ok:
                    last_ts = df5['ts'].iloc[-1].strftime("%Y%m%d%H%M")
                    key = f"{sym}|{direction}|{last_ts}"
                    if key in seen_signals:
                        continue
                    details_15_30 = f"{trend15}/{trend30} (EMA{EMA_SHORT}>{EMA_LONG})"
                    msg = format_signal(sym, direction, details_15_30, details_5m)
                    logging.info("Signal -> %s %s", sym, direction)
                    send_telegram(msg)
                    seen_signals.add(key)
                    if len(seen_signals) > 1000:
                        seen_signals = set(list(seen_signals)[-500:])
                time.sleep(0.2)
        except Exception as e:
            logging.exception("Main loop error: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run()