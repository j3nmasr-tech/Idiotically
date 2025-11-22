#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ULTIMATE WINNER SCANNER + BTC DIRECTION FILTER
- All timeframes (1m-30m) 
- Winner pattern filters
- BTC direction alignment for all altcoins
- Top 40 coins
"""

import os, time, asyncio, logging, datetime
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/winner_signals.db"

# SCANNING SETTINGS
SCAN_INTERVAL = 60
TOP_COINS_COUNT = 40
ALL_TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m"]
TREND_CONFIRMATION_TFS = ["15m", "30m", "1h"]

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | WINNER-SCANNER | %(message)s")
log = logging.getLogger("winner_scanner")
db_lock = asyncio.Lock()

# ---------------- TELEGRAM ----------------
async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
            )
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ---------------- BTC DIRECTION DETECTION ----------------
def get_btc_direction(btc_data_15m, btc_data_1h):
    """
    Determine BTC overall direction
    Returns: "BULLISH", "BEARISH", or "NEUTRAL"
    """
    if btc_data_15m is None or btc_data_1h is None:
        return "NEUTRAL"
    
    try:
        current_price = btc_data_15m['close'].iloc[-1]
        
        # 1H EMA for primary trend
        ema_1h_50 = btc_data_1h['close'].ewm(span=50).mean().iloc[-1]
        ema_1h_20 = btc_data_1h['close'].ewm(span=20).mean().iloc[-1]
        
        # 15M EMA for short-term direction
        ema_15m_20 = btc_data_15m['close'].ewm(span=20).mean().iloc[-1]
        
        # Strong bullish: Above all EMAs
        if current_price > ema_1h_50 and current_price > ema_1h_20 and current_price > ema_15m_20:
            return "BULLISH"
        
        # Strong bearish: Below all EMAs  
        elif current_price < ema_1h_50 and current_price < ema_1h_20 and current_price < ema_15m_20:
            return "BEARISH"
        
        # Mild bullish: Above 1H EMAs
        elif current_price > ema_1h_20 and current_price > ema_1h_50:
            return "BULLISH"
            
        # Mild bearish: Below 1H EMAs
        elif current_price < ema_1h_20 and current_price < ema_1h_50:
            return "BEARISH"
        
        else:
            return "NEUTRAL"
            
    except Exception as e:
        log.error(f"BTC direction error: {e}")
        return "NEUTRAL"

def is_trade_allowed(signal_side, btc_direction):
    """
    CRITICAL FILTER: Only allow trades that align with BTC direction
    - BTC BULLISH: Only allow BUY signals
    - BTC BEARISH: Only allow SELL signals  
    - BTC NEUTRAL: Allow both
    """
    if btc_direction == "BULLISH":
        allowed = signal_side == "BUY"
        if not allowed:
            log.info(f"❌ Blocked {signal_side} signal - BTC is BULLISH")
        return allowed
        
    elif btc_direction == "BEARISH":
        allowed = signal_side == "SELL"
        if not allowed:
            log.info(f"❌ Blocked {signal_side} signal - BTC is BEARISH")
        return allowed
        
    else:  # NEUTRAL
        return True

# ---------------- GET TOP COINS ----------------
async def get_top_coins(exchange, top_n=40):
    """Get top N coins by volume"""
    try:
        tickers = await exchange.fetch_tickers()
        usdt_pairs = [(symbol, data.get('quoteVolume', 0)) 
                      for symbol, data in tickers.items() 
                      if symbol.endswith('/USDT')]
        
        # Sort by volume and get top N
        top_coins = sorted(usdt_pairs, key=lambda x: x[1], reverse=True)[:top_n]
        return [coin[0] for coin in top_coins]
        
    except Exception as e:
        log.error(f"Error getting top coins: {e}")
        return ["BTC/USDT"]  # Fallback to BTC only

# ---------------- WINNER PATTERN FILTERS ----------------
def check_higher_tf_alignment(signal, higher_tf_data):
    if higher_tf_data is None or len(higher_tf_data) < 20:
        return False
    try:
        current_price = signal['entry']
        higher_tf_ema_20 = higher_tf_data['close'].ewm(span=20).mean().iloc[-1]
        higher_tf_ema_50 = higher_tf_data['close'].ewm(span=50).mean().iloc[-1]
        
        if signal['side'] == 'BUY':
            return current_price > higher_tf_ema_20 and current_price > higher_tf_ema_50
        else:
            return current_price < higher_tf_ema_20 and current_price < higher_tf_ema_50
    except Exception as e:
        log.error(f"Higher TF alignment error: {e}")
        return False

def check_entry_zone_quality(df, signal_direction):
    if len(df) < 15:
        return False
    try:
        recent_high = df['high'].tail(15).max()
        recent_low = df['low'].tail(15).min()
        current_price = df['close'].iloc[-1]
        
        if recent_high == recent_low:
            return False
            
        range_position = (current_price - recent_low) / (recent_high - recent_low)
        
        if signal_direction == 'BUY':
            return range_position < 0.3
        else:
            return range_position > 0.7
    except Exception as e:
        log.error(f"Zone quality error: {e}")
        return False

def detect_choppy_market(df):
    if len(df) < 25:
        return True
    try:
        high, low, close = df['high'], df['low'], df['close']
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        atr = true_range.rolling(14).mean().iloc[-1]
        
        current_price = close.iloc[-1]
        price_range_pct = (df['high'].tail(20).max() - df['low'].tail(20).min()) / current_price
        
        return (atr < (current_price * 0.002) and price_range_pct < 0.02)
    except Exception as e:
        log.error(f"Market condition error: {e}")
        return True

# ---------------- YOUR EXISTING SMC FUNCTIONS ----------------
def detect_order_blocks(df: pd.DataFrame):
    if len(df) < 3: return None, None, None
    try:
        candle = df.iloc[-3]
        if candle["close"] > candle["open"]:
            return "bullish", candle["open"], candle["low"]
        return "bearish", candle["high"], candle["open"]
    except: return None, None, None

def detect_fvg(df: pd.DataFrame):
    if len(df) < 3: return False, False
    try:
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
        bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
        return bull, bear
    except: return False, False

def detect_sweep(df: pd.DataFrame):
    if len(df) < 6: return False, False
    try:
        last = df.iloc[-1]
        prev = df.iloc[-5:-1]
        return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()
    except: return False, False

def detect_bos_mss(df: pd.DataFrame):
    return detect_sweep(df)

def generate_base_signal(df, symbol, context):
    if df is None or len(df) < 6:
        return None

    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None:
        return None

    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)
    bos_hh, bos_ll = detect_bos_mss(df)

    if not (bos_hh or bos_ll):
        return None

    score = 0
    reasons = []

    if ob_type == "bullish": 
        score += 2; reasons.append("OB Bull +2")
    else: 
        score += 2; reasons.append("OB Bear +2")

    if bull_fvg: 
        score += 2; reasons.append("FVG Bull +2")
    elif bear_fvg: 
        score += 2; reasons.append("FVG Bear +2")

    score += 2; reasons.append("BOS +2")
    
    if sweep_h or sweep_l: 
        score += 1; reasons.append("Sweep +1")
    else: 
        reasons.append("No Sweep +0")

    side = "BUY" if ob_type == "bullish" else "SELL"

    # ATR-based TP/SL
    entry = float(df['close'].iloc[-1])
    
    if side == "BUY":
        sl = entry * 0.995
        tp1 = entry * 1.005
        tp2 = entry * 1.010
        tp3 = entry * 1.015
    else:
        sl = entry * 1.005
        tp1 = entry * 0.995
        tp2 = entry * 0.990
        tp3 = entry * 0.985

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason_list": reasons
    }

# ---------------- ULTIMATE WINNER SIGNAL GENERATOR ----------------
def generate_winner_signal(df, symbol, context, btc_direction):
    """
    Only generate signals that pass ALL winner filters + BTC direction
    """
    # 1. Generate base SMC signal
    base_signal = generate_base_signal(df, symbol, context)
    if not base_signal:
        return None
    
    # 2. BTC DIRECTION FILTER (CRITICAL)
    if not is_trade_allowed(base_signal['side'], btc_direction):
        return None
    
    # 3. Get higher timeframe data
    higher_tf_data = None
    current_tf = context.get('tf', '')
    
    if current_tf in ['1m', '3m']:
        higher_tf_data = context.get('df_15m') or context.get('df_30m')
    elif current_tf == '5m':
        higher_tf_data = context.get('df_30m') or context.get('df_1h')
    elif current_tf in ['15m', '30m']:
        higher_tf_data = context.get('df_1h') or context.get('df_4h')
    
    # 4. Apply winner filters
    if not check_higher_tf_alignment(base_signal, higher_tf_data):
        return None
    
    if not check_entry_zone_quality(df, base_signal['side']):
        return None
    
    if detect_choppy_market(df):
        return None
    
    # 5. Add winner bonus
    base_signal['score'] += 5
    base_signal['reason_list'].append("WINNER PATTERN +5")
    base_signal['reason_list'].append(f"BTC {btc_direction} ✓")
    
    return base_signal

# ---------------- OPTIMIZED SCAN LOOP ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit=200):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if ohlcv:
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
            for col in ["open", "high", "low", "close", "vol"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        return None
    except:
        return None

async def winner_scan_loop(exchange):
    """Scan top 40 coins with BTC direction filter"""
    log.info("🔄 Starting winner scan cycle...")
    
    # Get top coins
    top_coins = await get_top_coins(exchange, TOP_COINS_COUNT)
    log.info(f"📊 Scanning top {len(top_coins)} coins")
    
    # First, get BTC data for direction
    btc_data = {}
    for tf in ["15m", "1h"]:
        btc_data[tf] = await fetch_ohlcv(exchange, "BTC/USDT", tf, 200)
    
    btc_direction = get_btc_direction(btc_data.get("15m"), btc_data.get("1h"))
    log.info(f"🎯 BTC Direction: {btc_direction}")
    
    signals_found = 0
    
    # Scan each coin
    for symbol in top_coins:
        if signals_found >= 10:  # Limit signals per scan
            break
            
        # Fetch data for this coin
        coin_data = {}
        for tf in ALL_TIMEFRAMES:
            df = await fetch_ohlcv(exchange, symbol, tf, 100)  # Smaller limit for speed
            if df is not None:
                coin_data[tf] = df
        
        if not coin_data:
            continue
        
        # Scan all timeframes for this coin
        for tf in ALL_TIMEFRAMES:
            if tf not in coin_data:
                continue
            
            # Prepare context
            context = {
                "tf": tf,
                "df_15m": coin_data.get("15m"),
                "df_30m": coin_data.get("30m"), 
                "df_1h": coin_data.get("1h")
            }
            
            signal = generate_winner_signal(coin_data[tf], symbol, context, btc_direction)
            
            if signal:
                signals_found += 1
                
                message = (
                    f"🏆 {symbol} WINNER ({tf}) | BTC: {btc_direction}\n"
                    f"Direction: {signal['side']}\n"
                    f"Entry: {signal['entry']:.4f}\n"
                    f"SL: {signal['sl']:.4f} | TP: {signal['tp1']:.4f}/{signal['tp2']:.4f}/{signal['tp3']:.4f}\n"
                    f"Score: {signal['score']}/10+\n"
                    f"Filters: {', '.join(signal['reason_list'])}"
                )
                
                await tg(message)
                log.info(f"✅ Winner signal: {symbol} {signal['side']}")
    
    log.info(f"📊 Scan complete: {signals_found} winner signals found (BTC: {btc_direction})")

# ---------------- MAIN LOOP ----------------
async def main_loop():
    log.info("🚀 INITIALIZING WINNER SCANNER + BTC FILTER...")
    
    startup_msg = (
        "🏆 ULTIMATE WINNER SCANNER STARTED\n"
        "• Top 40 coins + all timeframes\n" 
        "• BTC direction alignment enforced\n"
        "• Winner pattern filters active\n"
        "🎯 Target: 80%+ Win Rate"
    )
    await tg(startup_msg)
    
    exchange = ccxt.okx({"enableRateLimit": True})
    
    while True:
        try:
            await winner_scan_loop(exchange)
            log.info(f"💤 Waiting {SCAN_INTERVAL}s for next scan...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"💥 Main loop error: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main_loop())