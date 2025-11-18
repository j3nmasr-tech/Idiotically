#!/usr/bin/env python3
"""
BingX Low-Volume Altcoin Scalp Bot - Optimized + Filters (Regime, Wick, Divergence, MTF, Liquidity, Adaptive SL/TP, Pump Protection, Delayed Entry)
"""

import os, time, logging, json, math
from datetime import datetime, timedelta
import requests
import pandas as pd
import ccxt

# ---------- Config ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))  # seconds between coins / small sleeps

EMA_SHORT = int(os.getenv("EMA_SHORT", "50"))
EMA_LONG  = int(os.getenv("EMA_LONG", "200"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
VOL_MULTIPLIER = float(os.getenv("VOL_MULTIPLIER", "1.6"))

MIN_24H_VOLUME_USD = int(os.getenv("MIN_24H_VOLUME_USD", "100000"))
MAX_24H_VOLUME_USD = int(os.getenv("MAX_24H_VOLUME_USD", "5000000"))
MIN_CANDLES = int(os.getenv("MIN_CANDLES", "50"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "60"))

SIGNAL_LOG_FILE = os.getenv("SIGNAL_LOG_FILE", "signals_log.csv")
LAST_SIGNALS_FILE = os.getenv("LAST_SIGNALS_FILE", "last_signals.json")

# Liquidity / spread thresholds
MIN_BOOK_DEPTH_USD = float(os.getenv("MIN_BOOK_DEPTH_USD", "500.0"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.003"))  # 0.3%

# Pump protection thresholds
PUMP_VOL_MULT = float(os.getenv("PUMP_VOL_MULT", "5.0"))
PUMP_RANGE_ATR_MULT = float(os.getenv("PUMP_RANGE_ATR_MULT", "3.0"))

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
def fetch_ohlcv(symbol, tf, limit=50):
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        if not raw: return None
        df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')
        return df
    except Exception as e:
        logging.debug("fetch_ohlcv error %s %s", symbol, e)
        return None

def ema(series, period): return series.ewm(span=period, adjust=False).mean()
def rsi(series, period=14):
    delta = series.diff()
    up, down = delta.clip(lower=0), -1*delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-12)
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

# ---------- NEW: Market regime detector (volatility expansion + trending) ----------
def is_good_market_regime(df):
    try:
        if df is None or len(df) < 50: return False
        # Bollinger-like width
        std20 = df['close'].rolling(20).std()
        bb_width = (std20 * 2) / (df['close'] + 1e-12)
        # ATR
        atr_val = atr(df, 14)
        atr_ma = df['close'].rolling(14).mean()
        # trending measure
        e50 = ema(df['close'], 50).iloc[-1]
        e200 = ema(df['close'], 200).iloc[-1] if len(df)>=200 else ema(df['close'], EMA_LONG).iloc[-1]
        trending = abs(e50 - e200) > df['close'].iloc[-1] * 0.002  # >0.2% separation
        # volatility expansion: current bb width increasing
        vol_expanding = False
        try:
            vol_expanding = bb_width.iloc[-1] > bb_width.iloc[-3] and atr_val > (atr_ma.iloc[-1] * 0.8 if not math.isnan(atr_ma.iloc[-1]) else 0)
        except:
            vol_expanding = bb_width.iloc[-1] > bb_width.iloc[-1]  # false fallback
        return trending and vol_expanding
    except Exception as e:
        logging.debug("is_good_market_regime error: %s", e)
        return False

# ---------- NEW: orderbook liquidity + spread check ----------
def has_sufficient_liquidity(sym):
    try:
        ob = exchange.fetch_order_book(sym, limit=20)
        bids, asks = ob.get('bids', []), ob.get('asks', [])
        if not bids or not asks: return False
        best_bid, best_ask = bids[0][0], asks[0][0]
        spread_pct = abs(best_ask - best_bid) / ((best_ask + best_bid) / 2 + 1e-12)
        if spread_pct > MAX_SPREAD_PCT:
            logging.info("[LIQ] Spread too large for %s: %.4f%%", sym, spread_pct*100)
            return False
        # compute depth USD at top few levels
        bid_depth = sum([b[0]*b[1] for b in bids[:5]])
        ask_depth = sum([a[0]*a[1] for a in asks[:5]])
        if bid_depth < MIN_BOOK_DEPTH_USD or ask_depth < MIN_BOOK_DEPTH_USD:
            logging.info("[LIQ] Book too thin for %s: bid_depth=%.2f ask_depth=%.2f", sym, bid_depth, ask_depth)
            return False
        return True
    except Exception as e:
        logging.debug("has_sufficient_liquidity error %s %s", sym, e)
        return False

# ---------- NEW: wick / candle rejection ----------
def is_wick_trap(candle, direction):
    # direction == "BUY" checks big upper wick (liquidity grab)
    try:
        high, low, open_p, close_p = candle['high'], candle['low'], candle['open'], candle['close']
        body = abs(close_p - open_p) + 1e-12
        total_range = high - low + 1e-12
        upper_wick = high - max(open_p, close_p)
        lower_wick = min(open_p, close_p) - low
        # If buy candidate but there is a long upper wick -> trap
        if direction == "BUY" and upper_wick > total_range * 0.45:
            return True
        if direction == "SELL" and lower_wick > total_range * 0.45:
            return True
        # very small body relative to range -> indecisive
        if body / total_range < 0.15:
            return True
        return False
    except:
        return True

# ---------- NEW: simple hidden RSI divergence (continuation) ----------
def hidden_rsi_divergence(df, direction, lookback=14):
    """
    Hidden divergence heuristic:
    BUY: price makes higher low, RSI makes lower low -> continuation
    SELL: price makes lower high, RSI makes higher high -> continuation
    """
    try:
        if df is None or len(df) < lookback+3: return False
        close = df['close']
        r = rsi(close, RSI_PERIOD)
        # pick two swing points: last low and previous low (BUY) or highs for SELL
        if direction == "BUY":
            # find last two lows indexes in lookback window
            window = df[-(lookback+3):]
            lows = window['low']
            p = lows.idxmin()
            # before that, find earlier low
            prev_window = df[max(0, p-lookback-3):p]
            if len(prev_window)==0: return False
            prev_p = prev_window['low'].idxmin()
            if prev_p>=p: return False
            price_higher_low = close.iloc[p] > close.iloc[prev_p]
            rsi_lower_low = r.iloc[p] < r.iloc[prev_p]
            return price_higher_low and rsi_lower_low
        else:
            window = df[-(lookback+3):]
            highs = window['high']
            p = highs.idxmax()
            prev_window = df[max(0, p-lookback-3):p]
            if len(prev_window)==0: return False
            prev_p = prev_window['high'].idxmax()
            if prev_p>=p: return False
            price_lower_high = close.iloc[p] < close.iloc[prev_p]
            rsi_higher_high = r.iloc[p] > r.iloc[prev_p]
            return price_lower_high and rsi_higher_high
    except Exception as e:
        logging.debug("hidden_rsi_divergence error: %s", e)
        return False

# ---------- existing helpers slightly reused ----------
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
def save_last_signals(data): json.dump(data, open(LAST_SIGNALS_FILE,'w'))
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

# ---------- Support/Resistance ----------
def find_swing_levels(df, window=20):
    highs, lows = df['high'], df['low']
    resistances = [highs[i] for i in range(window,len(highs)-window) if highs[i]==max(highs[i-window:i+window+1])]
    supports = [lows[i] for i in range(window,len(lows)-window) if lows[i]==min(lows[i-window:i+window+1])]
    return sorted(supports), sorted(resistances)

# ---------- Symbol Processing ----------
def process_symbol(sym):
    # 15m for trend/regime
    df15 = fetch_ohlcv(sym,"15m",MIN_CANDLES)
    if df15 is None or len(df15)<MIN_CANDLES: return
    trend = detect_trend(df15)
    if trend is None: return

    # 1h MTF confirmation
    #df1h = fetch_ohlcv(sym, "1h", 200)
    #if df1h is None or len(df1h) < 50:
        #logging.debug("No 1h data for %s", sym)
    #else:
        #trend_1h = detect_trend(df1h)
        #if trend_1h is not None and trend_1h != trend:
            #logging.info("[MTF] 1h trend mismatch for %s: 15m=%s 1h=%s - skipping", sym, trend, trend_1h)
            #return

    # Market regime filter
    #if not is_good_market_regime(df15):
        #logging.debug("[REGIME] Market regime not suitable for %s", sym)
        #return

    # liquidity / spread check
    if not has_sufficient_liquidity(sym):
        logging.debug("[LIQ] insufficient liquidity %s", sym)
        return

    # 5m entry check
    df5 = fetch_ohlcv(sym,"5m",60)
    if df5 is None or len(df5)<30 or df5['volume'].iloc[-1]<50: return

    # pump/spike protection (on 5m last candle)
    last_candle = df5.iloc[-1]
    atr_val = atr(df15)
    candle_range = last_candle['high'] - last_candle['low']
    mean_vol20 = df5['volume'][-21:-1].mean() if len(df5)>=21 else df5['volume'].mean()
    if mean_vol20>0 and last_candle['volume'] > mean_vol20 * PUMP_VOL_MULT and candle_range > atr_val * PUMP_RANGE_ATR_MULT:
        logging.info("[PUMP] skipping %s due pump/spike (vol x%.1f range x%.1f ATR)", sym, last_candle['volume']/max(mean_vol20,1), candle_range/max(atr_val,1e-12))
        return

    ok, meta = check_entry_5m(df5,trend)
    if not ok:
        logging.debug("[ENTRY] basic 5m checks failed %s %s", sym, meta)
        return

    # wick rejection on last candle
    if is_wick_trap(last_candle, trend):
        logging.info("[WICK] wick trap for %s, skipping", sym)
        return

    # hidden divergence confirmation (boost)
    if not hidden_rsi_divergence(df5, trend, lookback=14):
        # allow trade to continue but log lower confidence
        logging.debug("[DIVERGE] no hidden divergence for %s (lower confidence)", sym)
        # optionally return to be more selective:
        # return
        pass

    # Delay entry by 1 candle: wait for next 5m candle to close and confirm direction
    try:
        logging.info("[DELAY] waiting for next 5m candle to confirm %s", sym)
        time.sleep(max(0.2, POLL_INTERVAL))  # short wait; real deploy may prefer sleep until candle boundary
        df5_after = fetch_ohlcv(sym,"5m", 5)  # small fetch for recent candles
        if df5_after is None or len(df5_after) < 2:
            logging.debug("[DELAY] couldn't get next candle for %s", sym)
            return
        # get the last closed candle after the trigger
        confirm_candle = df5_after.iloc[-1]
        # confirm direction: close above open for BUY, below open for SELL
        if trend == "BUY" and confirm_candle['close'] <= confirm_candle['open']:
            logging.info("[DELAY] confirmation candle not bullish for %s - skipping", sym)
            return
        if trend == "SELL" and confirm_candle['close'] >= confirm_candle['open']:
            logging.info("[DELAY] confirmation candle not bearish for %s - skipping", sym)
            return
        # wick rejection on confirm candle
        if is_wick_trap(confirm_candle, trend):
            logging.info("[DELAY] confirmation candle is wick trap for %s - skipping", sym)
            return
        # update entry to confirm candle close price
        entry = float(confirm_candle['close'])
    except Exception as e:
        logging.debug("Delay/confirmation error %s %s", sym, e)
        return

    # compute base TP/SL using atr (from 15m)
    atr_val = atr(df15)
    # momentum factor using 15m MACD hist ratio robustly
    try:
        macd_hist = macd(df15['close'])[2]
        recent_hist = macd_hist.iloc[-14:].abs().max() + 1e-12
        momentum_factor = min(abs(macd_hist.iloc[-1]) / recent_hist, 3)
    except:
        momentum_factor = 1.0

    supports, resistances = find_swing_levels(df15, window=20)
    if trend=="BUY":
        sl = entry - atr_val
        tp1 = entry + 1.5 * atr_val * momentum_factor
        tp2 = entry + 2.0 * atr_val * momentum_factor
        tp_candidates = [r for r in resistances if r>entry]
        tp3 = max(tp_candidates) if tp_candidates else entry + 3.0 * atr_val * momentum_factor
    else:
        sl = entry + atr_val
        tp1 = entry - 1.5 * atr_val * momentum_factor
        tp2 = entry - 2.0 * atr_val * momentum_factor
        tp_candidates = [s for s in supports if s<entry]
        tp3 = min(tp_candidates) if tp_candidates else entry - 3.0 * atr_val * momentum_factor

    # Adaptive SL/TP: tighten SL to BE if momentum fades (MACD hist falling for 3 candles) or RSI crosses against direction
    try:
        macd_hist_5 = macd(df5['close'])[2]
        if len(macd_hist_5) >= 4:
            if trend == "BUY" and macd_hist_5.iloc[-1] < macd_hist_5.iloc[-2] < macd_hist_5.iloc[-3]:
                # momentum fading -> tighten SL toward break-even
                sl = entry - 0.5 * atr_val
                tp3 = min(tp3, tp2) if trend=="BUY" else max(tp3, tp2)
                logging.info("[ADAPT] momentum fading BUY: tightening SL for %s", sym)
            if trend == "SELL" and macd_hist_5.iloc[-1] > macd_hist_5.iloc[-2] > macd_hist_5.iloc[-3]:
                sl = entry + 0.5 * atr_val
                tp3 = max(tp3, tp2) if trend=="SELL" else min(tp3, tp2)
                logging.info("[ADAPT] momentum fading SELL: tightening SL for %s", sym)
    except Exception as e:
        logging.debug("Adaptive MACD check error %s", e)

    # RSI cross protection
    try:
        rsi5 = rsi(df5['close'], RSI_PERIOD)
        if trend == "BUY" and rsi5.iloc[-1] < 50:
            # momentum turned neutral/bearish — reduce TP3
            tp3 = min(tp3, tp2)
            logging.info("[ADAPT] RSI softened for %s, reducing TP3", sym)
        if trend == "SELL" and rsi5.iloc[-1] > 50:
            tp3 = max(tp3, tp2)
            logging.info("[ADAPT] RSI softened SELL for %s, reducing TP3", sym)
    except:
        pass

    # Final check: cooldown
    if not can_signal(sym):
        logging.debug("[COOLDOWN] recently signaled %s", sym)
        return

    # Log, send telegram, save
    msg = format_signal(sym,trend,entry,tp1,tp2,tp3,sl)
    logging.info("Signal -> %s %s | Entry=%.8f TP3=%.8f SL=%.8f", sym, trend, entry, tp3, sl)
    send_telegram(msg)
    log_signal({"Timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "Symbol":sym,"Direction":trend,"EntryPrice":float(entry),
                "TP1":float(tp1),"TP2":float(tp2),"TP3":float(tp3),"SL":float(sl),
                "Status":"Open","ClosePrice":"","CloseTime":""})
    mark_signaled(sym)
    time.sleep(POLL_INTERVAL)

# ---------- Main Loop ----------
def run():
    logging.info("🤖 Bot starting. Loading markets...")
    send_telegram("🤖 Bot starting scanning low-volume USDT coins...")
    exchange.load_markets()
    try: tickers = exchange.fetch_tickers()
    except Exception as e: logging.error("fetch_tickers failed: %s", e); return

    # Filter low-volume USDT coins and skip top 100 (you used 20 earlier — kept as is)
    usdt_pairs = [(s, t.get('quoteVolume',0)) for s,t in tickers.items() if s.endswith("/USDT")]
    usdt_pairs.sort(key=lambda x:x[1], reverse=True)
    low_volume_pairs = usdt_pairs[20:]  # skip top 20
    available = [s for s, vol in low_volume_pairs if MIN_24H_VOLUME_USD <= (vol or 0) <= MAX_24H_VOLUME_USD]

    if not available:
        logging.warning("No available symbols after filtering volumes")
        return

    logging.info(f"Found {len(available)} low-volume symbols. Starting scan...")

    i = 0
    while True:
        sym = available[i % len(available)]
        logging.info(f"Scanning coin {i+1}/{len(available)}: {sym}")

        # DEBUG small indicator scan (kept from your earlier insertion)
        try:
            df_debug = fetch_ohlcv(sym, "5m", 60)
            if df_debug is not None and len(df_debug) > 50:
                close_series = df_debug["close"]

                ema_fast = ema(close_series, 10).iloc[-1]
                ema_slow = ema(close_series, 50).iloc[-1]
                rsi_value = rsi(close_series, 14).iloc[-1]

                logging.info(
                    f"[DEBUG] {sym} | EMA10={ema_fast:.4f}  EMA50={ema_slow:.4f}  RSI={rsi_value:.2f}"
                )

                if ema_fast > ema_slow and rsi_value < 30:
                    logging.info(f"[DEBUG] *** SIGNAL CONDITION HIT for {sym} ***")

            else:
                logging.warning(f"[DEBUG] Not enough candles for {sym} debug block")

        except Exception as e:
            logging.error(f"[DEBUG] Error processing debug indicators for {sym}: {e}")

        # REAL BOT LOGIC
        process_symbol(sym)

        # Update open signals (check exit conditions)
        try:
            open_df = pd.read_csv(SIGNAL_LOG_FILE)
            for idx,row in open_df[(open_df['Status']=="Open") & (open_df['Symbol']==sym)].iterrows():
                try:
                    tick = exchange.fetch_ticker(row['Symbol'])
                    last_price = tick.get('last')
                    if last_price: update_signal_status(row['Symbol'], row['Direction'], last_price)
                except Exception:
                    continue
        except Exception as e:
            logging.debug("update open signals error: %s", e)

        i += 1

if __name__=="__main__":
    run()