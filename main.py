#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PRODUCTION SCANNER - OLD SYSTEM + ELITE SYSTEM
- Your exact old winner filters + scoring
- PLUS new elite signals for maximum confidence
- Market regime protection
- Safe Kucoin API with anti-blocking protection
"""

import os, time, asyncio, logging, datetime, random
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
import uvicorn
from collections import defaultdict, deque

# ---------------- EXACT ORIGINAL CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "changeme")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 90))  # Increased for safety
TOP_N = int(os.getenv("TOP_N", 30))  # Reduced for fewer API calls
MIN_VOLUME = float(os.getenv("MIN_VOLUME", 1000000))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", 0.002))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3600))
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", 23))
TIMEFRAMES = ["1m", "3m", "5m", "15m", "1h"]  # Spot timeframes

# ---------------- EXACT ORIGINAL LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("smc_bot")
db_lock = asyncio.Lock()

# ---------------- EXACT ORIGINAL TELEGRAM ----------------
def escape_html(msg: str) -> str:
    if not msg: return "-"
    return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    safe_msg = escape_html(msg)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_msg, "parse_mode":"HTML"})
        except: pass

# ---------------- EXACT ORIGINAL DATABASE ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            timestamp TEXT,
            status TEXT,
            reason TEXT,
            score INTEGER,
            tp1_hit INTEGER DEFAULT 0,
            tp2_hit INTEGER DEFAULT 0,
            tp3_hit INTEGER DEFAULT 0
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pauses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reason TEXT,
            timestamp TEXT
        );
        """)
        await db.commit()

# ---------------- SAFE KUCOIN WRAPPER WITH ANTI-BLOCKING ----------------
class SafeKucoin:
    def __init__(self):
        self.exchange = ccxt.kucoin({
            "enableRateLimit": True,
            "rateLimit": 200,  # KuCoin public endpoints
            "timeout": 30000,
            "options": {
                "defaultType": "spot",
                "adjustForTimeDifference": True,
            }
        })
        self.request_times = deque()
        self.max_requests_per_minute = 45  # Conservative limit
        self.last_request = 0
        self.min_interval = 0.15  # 150ms between requests
        self.consecutive_errors = 0
        
    async def _respect_rate_limit(self):
        now = time.time()
        
        # Remove requests older than 1 minute
        while self.request_times and self.request_times[0] < now - 60:
            self.request_times.popleft()
            
        # Check if we're exceeding rate limits
        if len(self.request_times) >= self.max_requests_per_minute:
            sleep_time = 60 - (now - self.request_times[0])
            if sleep_time > 0:
                log.warning(f"⚠️ Rate limit approaching, sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
        
        # Enforce minimum interval between requests
        elapsed = now - self.last_request
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
            
        self.last_request = time.time()
        self.request_times.append(time.time())
    
    async def safe_fetch_tickers(self):
        """Safe ticker fetch with anti-blocking"""
        await self._respect_rate_limit()
        try:
            tickers = await self.exchange.fetch_tickers()
            # Filter for USDT pairs - KuCoin format
            usdt_tickers = {}
            for s, v in tickers.items():
                if s and (s.endswith("/USDT") or '-USDT' in s.upper()):
                    usdt_tickers[s] = v
            log.info(f"✅ Fetched {len(usdt_tickers)} USDT pairs from KuCoin")
            self.consecutive_errors = 0
            return usdt_tickers
        except ccxt.RateLimitExceeded as e:
            self.consecutive_errors += 1
            wait_time = min(300, 30 * self.consecutive_errors)
            log.warning(f"🛡️ Rate limit exceeded! Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            return {}
        except ccxt.DDoSProtection as e:
            self.consecutive_errors += 1
            wait_time = min(600, 60 * self.consecutive_errors)
            log.warning(f"🛡️ DDoS protection triggered! Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            return {}
        except Exception as e:
            self.consecutive_errors += 1
            log.error(f"❌ KuCoin ticker error: {str(e)}")
            await asyncio.sleep(10)
            return {}
    
    async def safe_fetch_ticker(self, symbol):
        """Safe single ticker fetch with retry logic"""
        return await self.fetch_with_retry(self.exchange.fetch_ticker, symbol)
    
    async def fetch_ohlcv(self, symbol, timeframe, limit=200):
        """OHLCV with anti-blocking protection"""
        return await self.fetch_with_retry(self.exchange.fetch_ohlcv, symbol, 
                                         timeframe=timeframe, limit=limit)
    
    async def fetch_with_retry(self, method, *args, max_retries=3):
        """Retry logic with exponential backoff"""
        for attempt in range(max_retries):
            try:
                await self._respect_rate_limit()
                result = await method(*args)
                self.consecutive_errors = 0
                return result
            except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
                if attempt == max_retries - 1:
                    log.error(f"📡 Network error after {max_retries} attempts: {e}")
                    raise
                wait_time = (2 ** attempt) + random.random()
                log.warning(f"📡 Network error, retry {attempt+1}/{max_retries} in {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
            except ccxt.RateLimitExceeded as e:
                wait_time = min(300, 60 * (attempt + 1))
                log.warning(f"🛡️ Rate limit, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
            except ccxt.DDoSProtection as e:
                wait_time = min(600, 120 * (attempt + 1))
                log.warning(f"🛡️ DDoS protection, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
            except Exception as e:
                if attempt == max_retries - 1:
                    log.error(f"❌ API error after {max_retries} attempts: {e}")
                    return None
                wait_time = (2 ** attempt) + random.random()
                log.warning(f"❌ API error, retry {attempt+1}/{max_retries} in {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
        return None

# ---------------- EXACT ORIGINAL INDICATORS ----------------
def atr(df: pd.DataFrame, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.DataFrame({
        "h-l": high - low,
        "h-pc": (high - close.shift(1)).abs(),
        "l-pc": (low - close.shift(1)).abs()
    }).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def sma(series: pd.Series, period: int):
    return series.rolling(period, min_periods=1).mean()

def rsi(series: pd.Series, period: int):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# ---------------- EXACT ORIGINAL SMC CORE ----------------
def detect_swing_points(df: pd.DataFrame):
    if len(df) < 5: return None
    last = df.iloc[-1]; prev = df.iloc[-3:-1]
    swing_high = last["high"] > prev["high"].max()
    swing_low = last["low"] < prev["low"].min()
    return swing_high, swing_low

def detect_active_range(df: pd.DataFrame, lookback=10):
    last = df.iloc[-lookback:]
    return last["high"].max(), last["low"].min()

def detect_liquidity_pools(df: pd.DataFrame):
    hh, ll = detect_swing_points(df)
    return hh, ll

def detect_sweep(df: pd.DataFrame):
    if len(df) < 6: return False, False
    last = df.iloc[-1]; prev = df.iloc[-5:-1]
    return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

def detect_bos_mss(df: pd.DataFrame):
    hh, ll = detect_sweep(df)
    return hh, ll

def detect_fvg(df: pd.DataFrame):
    if len(df) < 3: return False, False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
    bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
    return bull, bear

def detect_order_blocks(df: pd.DataFrame):
    if len(df) < 3: return None, None, None
    candle = df.iloc[-3]
    if candle["close"] > candle["open"]:
        return "bullish", candle["open"], candle["low"]
    return "bearish", candle["high"], candle["open"]

# ---------------- EXACT ORIGINAL SL-CLUSTER ----------------
recent_sl = defaultdict(lambda: deque())
def record_sl_hit(symbol: str, lookback_minutes=30):
    now = time.time(); dq = recent_sl[symbol]; dq.append(now)
    cutoff = now - lookback_minutes * 60
    while dq and dq[0] < cutoff: dq.popleft()
    
def deprioritized(symbol: str, threshold=3, lookback=30):
    dq = recent_sl[symbol]; now = time.time(); cutoff = now - lookback * 60
    while dq and dq[0] < cutoff: dq.popleft()
    return len(dq) >= threshold

# ---------------- NEW: MARKET REGIME FILTER ----------------
def check_market_regime(symbol, signal_direction, context):
    """Stop Fighting the Tide - Market Regime Filter"""
    df_1h = context.get("df_1h")
    if df_1h is None or len(df_1h) < 50:
        return True  # Allow if no data
    
    current_price = df_1h['close'].iloc[-1]
    ema_20_1h = df_1h['close'].ewm(span=20).mean().iloc[-1]
    ema_50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
    
    # Determine regime
    if current_price > ema_20_1h and ema_20_1h > ema_50_1h:
        regime = "STRONG_BULL"
    elif current_price < ema_20_1h and ema_20_1h < ema_50_1h:
        regime = "STRONG_BEAR"
    else:
        regime = "NEUTRAL"
    
    # Trading rules - Don't fight the tide!
    if regime == "STRONG_BULL":
        return signal_direction == "BUY"  # Only BUY allowed
    elif regime == "STRONG_BEAR":
        return signal_direction == "SELL"  # Only SELL allowed
    else:
        return True  # Both allowed in neutral

# ---------------- NEW: ELITE ENTRY CHECKLIST ----------------
def check_elite_filters(signal, df, context):
    """Elite Entry Checklist - Score 14+"""
    elite_score = 0
    elite_details = []
    
    # 1. Multi-Timeframe Confluence (1m & 5m agree)
    df_5m = context.get("df_5m")
    if df_5m is not None and len(df_5m) > 10:
        ob_type_5m, _, _ = detect_order_blocks(df_5m)
        if ob_type_5m is not None:
            signal_ob_type = "bullish" if signal["side"] == "BUY" else "bearish"
            if ob_type_5m == signal_ob_type:
                elite_score += 1
                elite_details.append("MTF ✓")
    
    # 2. Premium Zone Quality
    df_1h = context.get("df_1h")
    if df_1h is not None and len(df_1h) > 50:
        current_price = signal["entry"]
        ema_20_1h = df_1h['close'].ewm(span=20).mean().iloc[-1]
        ema_50_1h = df_1h['close'].ewm(span=50).mean().iloc[-1]
        price_diff_pct_20 = abs(current_price - ema_20_1h) / current_price
        price_diff_pct_50 = abs(current_price - ema_50_1h) / current_price
        if price_diff_pct_20 < 0.005 or price_diff_pct_50 < 0.005:
            elite_score += 1
            elite_details.append("Premium Zone ✓")
    
    # 3. Momentum Alignment (RSI Filter)
    df_3m = context.get("df_3m", df)
    if len(df_3m) >= 14:
        rsi_3m = rsi(df_3m['close'], 14).iloc[-1]
        if signal["side"] == "BUY":
            if rsi_3m > 40 and rsi_3m < 80:
                elite_score += 1
                elite_details.append("Momentum ✓")
        else:
            if rsi_3m < 60 and rsi_3m > 20:
                elite_score += 1
                elite_details.append("Momentum ✓")
    
    # 4. Volume-Verified Sweep
    if len(df) >= 20:
        sweep_high, sweep_low = detect_sweep(df)
        current_volume = df['vol'].iloc[-1]
        avg_volume = df['vol'].tail(20).mean()
        if (sweep_high or sweep_low) and current_volume > avg_volume * 1.5:
            elite_score += 1
            elite_details.append("Volume ✓")
    
    return elite_score == 4, elite_details

# ---------------- EXACT ORIGINAL SIGNAL GENERATOR ----------------
def generate_signal(df: pd.DataFrame, symbol: str, context=None):
    if context is None: context = {}
    tf = context.get("tf","15m")

    if df is None or len(df) < 6: return None

    last = df["close"].iloc[-1]

    ob_type, ob_hi, ob_lo = detect_order_blocks(df)
    if ob_type is None: return None

    bull_fvg, bear_fvg = detect_fvg(df)
    sweep_h, sweep_l = detect_sweep(df)
    bos_hh, bos_ll = detect_bos_mss(df)

    if not (bos_hh or bos_ll): return None

    score = 0; reasons = []

    if ob_type=="bullish": score+=2; reasons.append("OB Bull +2")
    else: score+=2; reasons.append("OB Bear +2")

    if bull_fvg: score+=2; reasons.append("FVG Bull +2")
    elif bear_fvg: score+=2; reasons.append("FVG Bear +2")

    score+=2; reasons.append("BOS +2")
    if sweep_h or sweep_l: score+=1; reasons.append("Sweep +1")
    else: reasons.append("No Sweep +0")

    side = "BUY" if ob_type=="bullish" else "SELL"

    # EXACT ORIGINAL ATR-based TP/SL
    atr_val = None
    df15 = context.get("df_15m")
    if df15 is not None and len(df15)>=10:
        atr_val = float(atr(df15,14).iloc[-1])
    entry = float(last)
    tp_mult, sl_mult = 0.8, 1.0
    if atr_val:
        if side=="BUY":
            sl = entry - sl_mult*atr_val
            tp1 = entry + tp_mult*atr_val
            tp2 = entry + tp_mult*1.5*atr_val
            tp3 = entry + tp_mult*2.5*atr_val
        else:
            sl = entry + sl_mult*atr_val
            tp1 = entry - tp_mult*atr_val
            tp2 = entry - tp_mult*1.5*atr_val
            tp3 = entry - tp_mult*2.5*atr_val
    else:
        if side=="BUY":
            sl = float(ob_lo)
            tp1 = entry*1.004; tp2 = entry*1.008; tp3 = entry*1.012
        else:
            sl = float(ob_hi)
            tp1 = entry*0.996; tp2 = entry*0.992; tp3 = entry*0.988

    if sl==entry:
        sl = entry - entry*0.002 if side=="BUY" else entry + entry*0.002

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "reason": "Set B SMC Signal",
        "reason_list": reasons
    }

# ---------------- EXACT ORIGINAL LOG SIGNAL ----------------
async def log_signal(sig):
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO signals (symbol,side,entry,sl,tp1,tp2,tp3,timestamp,status,reason,score)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["symbol"],sig["side"],sig["entry"],sig["sl"],sig["tp1"],sig["tp2"],sig["tp3"],
                  datetime.datetime.utcnow().isoformat(),"OPEN",sig["reason"],sig["score"]))
            await db.commit()

# ---------------- WINNER FILTERS ----------------
def get_btc_direction(btc_15m, btc_1h):
    """BTC direction detection"""
    if btc_15m is None or btc_1h is None: return "NEUTRAL"
    try:
        price = btc_15m['close'].iloc[-1]
        ema_1h_50 = btc_1h['close'].ewm(span=50).mean().iloc[-1]
        ema_15m_20 = btc_15m['close'].ewm(span=20).mean().iloc[-1]
        
        if price > ema_1h_50 and price > ema_15m_20: return "BULLISH"
        elif price < ema_1h_50 and price < ema_15m_20: return "BEARISH"
        else: return "NEUTRAL"
    except: return "NEUTRAL"

def is_trade_allowed(signal_side, btc_direction):
    """BTC BULLISH: Only BUY allowed | BTC BEARISH: Only SELL allowed"""
    if btc_direction == "BULLISH": return signal_side == "BUY"
    elif btc_direction == "BEARISH": return signal_side == "SELL"
    else: return True

def check_higher_tf_alignment(signal, higher_tf_data):
    """Higher timeframe alignment filter"""
    if higher_tf_data is None or len(higher_tf_data) < 20:
        return False
    current_price = signal['entry']
    higher_tf_ema_20 = higher_tf_data['close'].ewm(span=20).mean().iloc[-1]
    higher_tf_ema_50 = higher_tf_data['close'].ewm(span=50).mean().iloc[-1]
    if signal['side'] == 'BUY':
        return current_price > higher_tf_ema_20 and current_price > higher_tf_ema_50
    else:
        return current_price < higher_tf_ema_20 and current_price < higher_tf_ema_50

def check_momentum_confirmation(df, signal_direction):
    """Momentum confirmation filter"""
    if len(df) < 3: return False
    current_candle = df.iloc[-1]
    prev_candle = df.iloc[-2]
    if signal_direction == 'BUY':
        return (current_candle['close'] > current_candle['open'] and 
                current_candle['close'] > prev_candle['close'])
    else:
        return (current_candle['close'] < current_candle['open'] and
                current_candle['close'] < prev_candle['close'])

def check_entry_zone_quality(df, signal_direction):
    """Zone quality detection"""
    if len(df) < 15: return False
    recent_high = df['high'].tail(15).max()
    recent_low = df['low'].tail(15).min()
    current_price = df['close'].iloc[-1]
    if recent_high == recent_low: return False
    range_position = (current_price - recent_low) / (recent_high - recent_low)
    if signal_direction == 'BUY':
        return range_position < 0.3
    else:
        return range_position > 0.7

def detect_choppy_market(df):
    """Market condition filter"""
    if len(df) < 25: return True
    high, low, close = df['high'], df['low'], df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    atr = true_range.rolling(14).mean().iloc[-1]
    current_price = close.iloc[-1]
    price_range_pct = (df['high'].tail(20).max() - df['low'].tail(20).min()) / current_price
    return (atr < (current_price * 0.002) and price_range_pct < 0.02)

# ---------------- EXACT ORIGINAL MONITOR ----------------
async def monitor_signals(exchange):
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT id,symbol,side,entry,sl,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit,status 
                    FROM signals WHERE status='OPEN'
                """) as cursor:
                    async for row in cursor:
                        sig_id, symbol, side, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, status = row
                        try:
                            ticker = await exchange.safe_fetch_ticker(symbol)
                            last_price = ticker.get("last") if ticker else None
                            if last_price is None: continue

                            hits=[]; sl_hit=False
                            if side=="BUY":
                                if not tp1_hit and last_price>=tp1: hits.append("TP1"); tp1_hit=1
                                if not tp2_hit and last_price>=tp2: hits.append("TP2"); tp2_hit=1
                                if not tp3_hit and last_price>=tp3: hits.append("TP3"); tp3_hit=1
                                if last_price<=sl: hits.append("SL"); status="CLOSED"; sl_hit=True
                            else:
                                if not tp1_hit and last_price<=tp1: hits.append("TP1"); tp1_hit=1
                                if not tp2_hit and last_price<=tp2: hits.append("TP2"); tp2_hit=1
                                if not tp3_hit and last_price<=tp3: hits.append("TP3"); tp3_hit=1
                                if last_price>=sl: hits.append("SL"); status="CLOSED"; sl_hit=True

                            if hits:
                                await tg(f"🎯 {symbol} {side} update\nEntry:{entry}\nLast:{last_price}\nHits:{','.join(hits)}\nSL:{sl}\nTP1:{tp1} TP2:{tp2} TP3:{tp3}")

                            if sl_hit: record_sl_hit(symbol)

                            async with db_lock:
                                await db.execute("""
                                    UPDATE signals SET tp1_hit=?,tp2_hit=?,tp3_hit=?,status=? WHERE id=?
                                """,(tp1_hit,tp2_hit,tp3_hit,status,sig_id))
                        except Exception as e:
                            log.error(f"Error monitoring {symbol}: {e}")
                            continue
                await db.commit()
        except Exception as e: 
            log.exception("monitor error: %s", e)
        await asyncio.sleep(SCAN_INTERVAL)

# ---------------- DUAL SYSTEM SCAN LOOP (OLD + ELITE) ----------------
last_signal_time = {}
async def scan_loop(exchange):
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    while True:
        t0=time.time()
        try:
            # Get BTC direction first - USING SPOT SYMBOL
            btc_15m_data = await exchange.fetch_ohlcv("BTC/USDT", "15m", 100)
            btc_1h_data = await exchange.fetch_ohlcv("BTC/USDT", "1h", 100)
            btc_15m = pd.DataFrame(btc_15m_data, columns=["ts","open","high","low","close","vol"]) if btc_15m_data else None
            btc_1h = pd.DataFrame(btc_1h_data, columns=["ts","open","high","low","close","vol"]) if btc_1h_data else None
            btc_direction = get_btc_direction(btc_15m, btc_1h)
            log.info(f"🎯 BTC Direction: {btc_direction}")
            
            # Get top coins - USING SAFE FETCH
            tickers = await exchange.safe_fetch_tickers()
            if not tickers:
                log.warning("❌ No tickers received, skipping scan")
                await asyncio.sleep(30)
                continue
                
            top = sorted([(s, v.get("quoteVolume", 0)) for s, v in tickers.items()], 
                        key=lambda x: x[1], reverse=True)[:TOP_N]
            
            signals_found = 0
            elite_signals = 0
            old_signals = 0
            
            for symbol, _ in top:
                if deprioritized(symbol): 
                    continue
                    
                ohlcvs = {}
                for tf in TIMEFRAMES:
                    key = f"{symbol}:{tf}"
                    if key in last_signal_time and time.time() - last_signal_time[key] < 1800: 
                        continue
                        
                    ohlcv = await exchange.fetch_ohlcv(symbol, tf, 200)
                    if not ohlcv: 
                        continue
                        
                    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
                    for c in ["open", "high", "low", "close", "vol"]: 
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    context = {"tf": tf, "df_15m": ohlcvs.get("15m"), "df_1h": ohlcvs.get("1h")}
                    
                    if tf in ("1m", "3m", "5m"):
                        if "15m" not in ohlcvs: 
                            ohlcv15 = await exchange.fetch_ohlcv(symbol, "15m", 200)
                            if ohlcv15: 
                                ohlcvs["15m"] = pd.DataFrame(ohlcv15, columns=["ts", "open", "high", "low", "close", "vol"])
                        if "1h" not in ohlcvs:
                            ohlcv1h = await exchange.fetch_ohlcv(symbol, "1h", 200)
                            if ohlcv1h: 
                                ohlcvs["1h"] = pd.DataFrame(ohlcv1h, columns=["ts", "open", "high", "low", "close", "vol"])
                        context["df_15m"] = ohlcvs.get("15m")
                        context["df_1h"] = ohlcvs.get("1h")
                    
                    # Additional timeframes for elite filters
                    if tf not in ["3m", "5m"]:
                        for add_tf in ["3m", "5m"]:
                            if add_tf not in ohlcvs:
                                add_ohlcv = await exchange.fetch_ohlcv(symbol, add_tf, 200)
                                if add_ohlcv: 
                                    ohlcvs[add_tf] = pd.DataFrame(add_ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
                                    for col in ["open", "high", "low", "close", "vol"]:
                                        ohlcvs[add_tf][col] = pd.to_numeric(ohlcvs[add_tf][col], errors="coerce")
                        context["df_3m"] = ohlcvs.get("3m")
                        context["df_5m"] = ohlcvs.get("5m")
                    
                    # Generate original signal
                    sig = generate_signal(df, symbol, context)
                    
                    if sig:
                        # SYSTEM 1: YOUR EXACT OLD SYSTEM (All original filters + Market Regime)
                        filters_passed = True
                        
                        # 1. BTC Direction Filter
                        if not is_trade_allowed(sig['side'], btc_direction):
                            filters_passed = False
                            
                        # 2. Higher TF Alignment
                        elif not check_higher_tf_alignment(sig, context.get("df_15m")):
                            filters_passed = False
                            
                        # 3. Momentum Confirmation (skip for 1m/3m)
                        elif tf not in ["1m", "3m"] and not check_momentum_confirmation(df, sig['side']):
                            filters_passed = False
                            
                        # 4. Zone Quality
                        elif not check_entry_zone_quality(df, sig['side']):
                            filters_passed = False
                            
                        # 5. Market Condition
                        elif detect_choppy_market(df):
                            filters_passed = False
                        
                        # 6. NEW: Market Regime Filter (only new addition to old system)
                        elif not check_market_regime(sig['symbol'], sig['side'], context):
                            filters_passed = False
                        
                        if filters_passed:
                            # Add winner bonuses (EXACTLY like your old system)
                            sig['reason_list'].extend([
                                f"BTC {btc_direction} ✓", "Higher TF ✓", 
                                "Zone ✓", "Trending ✓"
                            ])
                            if tf not in ["1m", "3m"]:
                                sig['reason_list'].append("Momentum ✓")
                            sig['score'] += 5
                            
                            # Send OLD SYSTEM signal
                            await tg(f"🏆 {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']}\nSL:{sig['sl']}\nTP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}\nScore:{sig['score']}\nBreakdown:{', '.join(sig['reason_list'])}")
                            await log_signal(sig)
                            last_signal_time[key] = time.time()
                            old_signals += 1
                            signals_found += 1
                            
                            # SYSTEM 2: NEW ELITE SYSTEM (Same filters + Elite checklist)
                            is_elite, elite_details = check_elite_filters(sig, df, context)
                            
                            if is_elite:
                                # Create elite version with elite badges
                                elite_reasons = sig['reason_list'] + elite_details
                                elite_score = sig['score'] + 10  # Elite bonus
                                
                                await tg(f"🎯 ELITE | {sig['symbol']} ({tf}) {sig['side']}\nEntry:{sig['entry']}\nSL:{sig['sl']}\nTP1:{sig['tp1']} TP2:{sig['tp2']} TP3:{sig['tp3']}\nScore:{elite_score} (3X SIZE)\nBreakdown:{', '.join(elite_reasons)}")
                                elite_signals += 1
            
            # Reset error counter on successful scan
            consecutive_errors = 0
            log.info(f"📊 Scan complete: {signals_found} total signals ({old_signals} old system, {elite_signals} elite)")
                        
        except ccxt.RateLimitExceeded as e:
            consecutive_errors += 1
            wait_time = min(300, 60 * consecutive_errors)
            log.error(f"🔴 Rate limit exceeded! Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            
        except ccxt.DDoSProtection as e:
            consecutive_errors += 1
            wait_time = min(600, 120 * consecutive_errors)
            log.error(f"🛡️ DDoS protection! Waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            
        except Exception as e: 
            consecutive_errors += 1
            log.exception(f"Scan error #{consecutive_errors}: {e}")
            if consecutive_errors >= max_consecutive_errors:
                log.error("🆘 Too many consecutive errors, waiting 10 minutes")
                await asyncio.sleep(600)
                consecutive_errors = 0
            else:
                await asyncio.sleep(30)
                
        elapsed = time.time() - t0
        await asyncio.sleep(max(1, SCAN_INTERVAL - elapsed))

# ---------------- EXACT ORIGINAL FASTAPI ----------------
app = FastAPI()
@app.post("/webhook")
async def webhook(request: Request):
    token = request.headers.get("X-Auth", "")
    if token != WEBHOOK_SECRET: raise HTTPException(403, "Invalid secret")
    data = await request.json()
    log.info("Webhook received: %s", data)
    return {"ok": True}

# ---------------- UPDATED MAIN WITH SAFE KUCOIN WRAPPER ----------------
async def main():
    await init_db()
    exchange = SafeKucoin()  # Use safe KuCoin wrapper
    
    # Startup message
    startup_msg = (
        "🏆 DUAL SYSTEM SCANNER STARTED\n"
        "• OLD SYSTEM: Your exact 10-score signals + all original filters\n"
        "• ELITE SYSTEM: Ultra-filtered elite signals (3X size)\n" 
        "• Market regime protection for both systems\n"
        "• Safe KuCoin API with anti-blocking protection\n"
        "🎯 OLD: 11-15 score | ELITE: 21-25+ score\n"
        "🔒 Rate limiting: 45 req/min | Min interval: 150ms"
    )
    await tg(startup_msg)
    log.info("✅ Dual system scanner started with SAFE KUCOIN API")
    
    await asyncio.gather(scan_loop(exchange), monitor_signals(exchange))

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--http", action="store_true")
    args = p.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=9000)
    else:
        asyncio.run(main())