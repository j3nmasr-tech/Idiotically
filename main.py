#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Synthesis Scanner - الإصدار المؤكد للإرسال بدون تكرار
"""

import os
import time
import asyncio
import logging
import hashlib
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
from typing import Dict, List, Optional, Tuple, Any

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))

# Easier thresholds for more signals
MIN_SYNTHESIS_SCORE = 0.25  # Lowered from 0.6

# Timeframes - using fewer for reliability
TIMEFRAMES = {
    "DAILY": "1d",
    "H4": "4h",
    "H1": "1h",
    "M15": "15m"
}

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("visual_scanner")
db_lock = asyncio.Lock()
db_conn = None
exchange = None

# ---------------- TELEGRAM ----------------
async def tg(msg: str):
    """Send Telegram message"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML"
            })
        log.info("Telegram message sent")
    except Exception as e:
        log.warning(f"Telegram failed: {e}")

# ---------------- DATABASE ----------------
async def check_and_add_column(column_name: str, column_type: str):
    """Check if a column exists and add it if it doesn't"""
    try:
        # Check if column exists
        async with db_conn.execute(f"PRAGMA table_info(signals)") as cursor:
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if column_name not in column_names:
                log.info(f"Adding missing column: {column_name}")
                await db_conn.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
                await db_conn.commit()
                log.info(f"✅ Column {column_name} added successfully")
                return True
            else:
                log.debug(f"Column {column_name} already exists")
                return True
    except Exception as e:
        log.error(f"Error adding column {column_name}: {e}")
        return False

async def init_db():
    """Initialize database with automatic schema updates"""
    global db_conn
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
        # Create main table if it doesn't exist
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN',
                timeframe_alignment TEXT,
                wave_structure TEXT,
                strength_level TEXT,
                indicators_signal TEXT,
                volume_status TEXT,
                synthesis_score REAL,
                close_reason TEXT,
                close_price REAL,
                close_timestamp DATETIME,
                pnl_percent REAL,
                signal_hash TEXT UNIQUE
            )
        """)
        
        await db_conn.commit()
        log.info("✅ Main table created/verified")
        
        # Check and add missing columns
        required_columns = [
            ("price_hash", "TEXT"),
        ]
        
        for column_name, column_type in required_columns:
            if not await check_and_add_column(column_name, column_type):
                log.error(f"Failed to add required column: {column_name}")
                return False
        
        # Create indexes (will ignore if already exist)
        try:
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_price_hash ON signals(price_hash)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON signals(symbol, timestamp)")
            await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON signals(status)")
            await db_conn.commit()
            log.info("✅ Database indexes created/verified")
        except Exception as e:
            log.warning(f"Index creation warning (likely already exist): {e}")
        
        log.info("✅ Database ready with all required columns")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ SIMPLIFIED ANALYSIS - MORE SIGNALS ================

def analyze_timeframes_simple(data: Dict[str, pd.DataFrame]) -> Tuple[str, float, str]:
    """Simplified timeframe analysis"""
    try:
        scores = []
        reasons = []
        
        for tf_name, df in data.items():
            prices = df['close'].values
            if len(prices) < 10:
                continue
            
            # Simple trend: Compare last price to 10-period average
            recent_avg = np.mean(prices[-10:])
            current = prices[-1]
            
            if current > recent_avg * 1.01:  # 1% above average
                scores.append(1.0)
                reasons.append(f"{tf_name}: UP")
            elif current < recent_avg * 0.99:  # 1% below average
                scores.append(1.0)
                reasons.append(f"{tf_name}: DOWN")
            else:
                scores.append(0.5)
                reasons.append(f"{tf_name}: NEUTRAL")
        
        if not scores:
            return "NEUTRAL", 0.0, "No data"
        
        avg_score = np.mean(scores)
        
        # Determine dominant direction
        up_count = sum(1 for r in reasons if "UP" in r)
        down_count = sum(1 for r in reasons if "DOWN" in r)
        
        if up_count > down_count:
            direction = "UP"
        elif down_count > up_count:
            direction = "DOWN"
        else:
            direction = "NEUTRAL"
        
        reason = " | ".join(reasons[:3])  # First 3 reasons only
        
        return direction, avg_score, reason
        
    except Exception as e:
        log.error(f"Timeframe error: {e}")
        return "NEUTRAL", 0.0, f"Error: {e}"

def analyze_wave_simple(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """Simplified wave analysis"""
    try:
        h4_df = data.get('H4')
        if h4_df is None or len(h4_df) < 20:
            return 0.6, "Insufficient data"
        
        prices = h4_df['close'].values[-20:]
        
        # Simple wave detection: higher highs for uptrend, lower lows for downtrend
        if direction == "UP":
            # Check if recent prices are making higher highs
            recent_high = np.max(prices[-5:])
            prev_high = np.max(prices[-10:-5])
            
            if recent_high > prev_high:
                return 0.8, "Higher highs detected"
            else:
                return 0.5, "No clear higher highs"
        
        elif direction == "DOWN":
            # Check if recent prices are making lower lows
            recent_low = np.min(prices[-5:])
            prev_low = np.min(prices[-10:-5])
            
            if recent_low < prev_low:
                return 0.8, "Lower lows detected"
            else:
                return 0.5, "No clear lower lows"
        
        return 0.6, "Neutral wave pattern"
        
    except Exception as e:
        log.error(f"Wave error: {e}")
        return 0.5, f"Error: {e}"

def analyze_strength_simple(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """Simplified strength analysis"""
    try:
        h1_df = data.get('H1')
        if h1_df is None or len(h1_df) < 10:
            return 0.6, "Insufficient data"
        
        recent = h1_df.iloc[-5:]  # Last 5 candles
        
        # Calculate bullish/bearish pressure
        if direction == "UP":
            bullish_candles = sum(recent['close'] > recent['open'])
            score = bullish_candles / 5
            strength = "Bullish pressure"
        else:  # DOWN
            bearish_candles = sum(recent['close'] < recent['open'])
            score = bearish_candles / 5
            strength = "Bearish pressure"
        
        return score, strength
        
    except Exception as e:
        log.error(f"Strength error: {e}")
        return 0.5, f"Error: {e}"

def analyze_indicators_simple(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """Simplified indicators - just RSI"""
    try:
        m15_df = data.get('M15')
        if m15_df is None or len(m15_df) < 14:
            return 0.6, "Insufficient data"
        
        prices = m15_df['close'].values[-14:]
        
        # Simple RSI calculation
        deltas = np.diff(prices)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = np.mean(gains) if len(gains) > 0 else 0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0
        
        if avg_loss == 0:
            rsi = 100 if avg_gain > 0 else 0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        
        # Check if RSI supports direction
        if direction == "UP":
            if rsi < 50:  # Oversold or neutral
                return 0.8, f"RSI {rsi:.0f} (Supports UP)"
            else:
                return 0.4, f"RSI {rsi:.0f} (Caution)"
        else:  # DOWN
            if rsi > 50:  # Overbought or neutral
                return 0.8, f"RSI {rsi:.0f} (Supports DOWN)"
            else:
                return 0.4, f"RSI {rsi:.0f} (Caution)"
        
    except Exception as e:
        log.error(f"Indicators error: {e}")
        return 0.5, f"Error: {e}"

def analyze_volume_simple(data: Dict[str, pd.DataFrame], direction: str) -> Tuple[float, str]:
    """Simplified volume analysis"""
    try:
        m15_df = data.get('M15')
        if m15_df is None or len(m15_df) < 10:
            return 0.6, "Insufficient data"
        
        recent_volume = m15_df['volume'].values[-5:].mean()
        prev_volume = m15_df['volume'].values[-10:-5].mean()
        
        if prev_volume == 0:
            volume_ratio = 1
        else:
            volume_ratio = recent_volume / prev_volume
        
        if volume_ratio > 1.2:
            return 0.8, f"Volume increasing ({volume_ratio:.1f}x)"
        elif volume_ratio > 0.8:
            return 0.6, f"Volume normal ({volume_ratio:.1f}x)"
        else:
            return 0.4, f"Volume low ({volume_ratio:.1f}x)"
        
    except Exception as e:
        log.error(f"Volume error: {e}")
        return 0.5, f"Error: {e}"

def calculate_simple_sltp(current_price: float, side: str, data: Dict[str, pd.DataFrame]) -> Tuple[float, float, str]:
    """Simple SL/TP calculation"""
    try:
        h4_df = data.get('H4')
        if h4_df is None or len(h4_df) < 20:
            # Default values if no data
            if side == "BUY":
                sl = current_price * 0.98  # 2% stop loss
                tp = current_price * 1.04  # 4% take profit
            else:
                sl = current_price * 1.02  # 2% stop loss
                tp = current_price * 0.96  # 4% take profit
            
            return sl, tp, "Default 1:2 R:R"
        
        if side == "BUY":
            # Find recent support
            recent_low = h4_df['low'].iloc[-10:].min()
            sl = recent_low * 0.99  # 1% below support
            
            risk = current_price - sl
            tp = current_price + (risk * 2)  # 1:2 risk reward
            
            return sl, tp, f"SL below {recent_low:.2f}, TP 1:2 R:R"
        
        else:  # SELL
            # Find recent resistance
            recent_high = h4_df['high'].iloc[-10:].max()
            sl = recent_high * 1.01  # 1% above resistance
            
            risk = sl - current_price
            tp = current_price - (risk * 2)  # 1:2 risk reward
            
            return sl, tp, f"SL above {recent_high:.2f}, TP 1:2 R:R"
        
    except Exception as e:
        log.error(f"SL/TP error: {e}")
        # Fallback values
        if side == "BUY":
            return current_price * 0.98, current_price * 1.04, "Error fallback"
        else:
            return current_price * 1.02, current_price * 0.96, "Error fallback"

# ================ SIGNAL GENERATION WITH DEDUPLICATION ================

async def check_duplicate_signal(symbol: str, side: str, current_price: float, 
                                tf_score: float, wave_score: float) -> Tuple[bool, str]:
    """Check if similar signal was recently sent"""
    try:
        # Create price condition hash (based on market conditions, not time)
        price_conditions = f"{symbol}:{side}:{current_price:.4f}:{tf_score:.2f}:{wave_score:.2f}"
        price_hash = hashlib.md5(price_conditions.encode()).hexdigest()
        
        async with db_lock:
            # Check for same price conditions in last 4 hours
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE price_hash = ? AND timestamp > datetime('now', '-4 hours')
            """, (price_hash,)) as cursor:
                same_conditions = (await cursor.fetchone())[0]
            
            if same_conditions > 0:
                log.debug(f"{symbol}: Same market conditions detected recently")
                return True, price_hash
            
            # Check for same symbol and side in last 6 hours (max 2 signals)
            async with db_conn.execute("""
                SELECT COUNT(*) FROM signals 
                WHERE symbol = ? AND side = ? 
                AND timestamp > datetime('now', '-6 hours')
                AND status = 'OPEN'
            """, (symbol, side)) as cursor:
                recent_signals = (await cursor.fetchone())[0]
            
            if recent_signals >= 2:
                log.debug(f"{symbol}: Too many recent {side} signals ({recent_signals})")
                return True, price_hash
            
            # Check if price hasn't moved much from last signal
            async with db_conn.execute("""
                SELECT entry FROM signals 
                WHERE symbol = ? AND side = ? 
                AND timestamp > datetime('now', '-2 hours')
                ORDER BY timestamp DESC LIMIT 1
            """, (symbol, side)) as cursor:
                result = await cursor.fetchone()
                
                if result:
                    last_entry = result[0]
                    price_change = abs(current_price - last_entry) / last_entry * 100
                    
                    if price_change < 0.5:  # Less than 0.5% price change
                        log.debug(f"{symbol}: Price hasn't moved enough ({price_change:.2f}%)")
                        return True, price_hash
        
        return False, price_hash
        
    except Exception as e:
        log.error(f"Duplicate check error: {e}")
        # If error, create a random hash and allow the signal
        return False, hashlib.md5(f"{symbol}:{time.time_ns()}".encode()).hexdigest()

async def generate_signal_simple(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Generate signal with simple logic and duplicate prevention"""
    try:
        log.info(f"Analyzing {symbol}...")
        
        # 1. Timeframe analysis
        direction, tf_score, tf_reason = analyze_timeframes_simple(data)
        
        if direction == "NEUTRAL":
            log.debug(f"{symbol}: No clear direction")
            return None
        
        log.info(f"{symbol}: Direction {direction}, Score: {tf_score:.2f}")
        
        # 2. Other analyses
        wave_score, wave_reason = analyze_wave_simple(data, direction)
        strength_score, strength_reason = analyze_strength_simple(data, direction)
        indicators_score, indicators_reason = analyze_indicators_simple(data, direction)
        volume_score, volume_reason = analyze_volume_simple(data, direction)
        
        # 3. Calculate total score (simple average)
        scores = [tf_score, wave_score, strength_score, indicators_score, volume_score]
        total_score = np.mean(scores)
        
        log.info(f"{symbol}: Total score {total_score:.2f}")
        
        # 4. Check if score is good enough
        if total_score < MIN_SYNTHESIS_SCORE:
            log.debug(f"{symbol}: Score too low ({total_score:.2f} < {MIN_SYNTHESIS_SCORE})")
            return None
        
        # 5. Get current price
        current_price = data['M15']['close'].iloc[-1]
        side = "BUY" if direction == "UP" else "SELL"
        
        # 6. Check for duplicates BEFORE calculating SL/TP
        is_duplicate, price_hash = await check_duplicate_signal(
            symbol, side, current_price, tf_score, wave_score
        )
        
        if is_duplicate:
            return None
        
        # 7. Calculate SL/TP
        sl, tp, sltp_logic = calculate_simple_sltp(current_price, side, data)
        
        # Calculate risk/reward
        risk = abs(current_price - sl)
        reward = abs(tp - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        # Skip if R:R is too poor
        if rr_ratio < 1.0:
            log.debug(f"{symbol}: Poor R:R ratio ({rr_ratio:.1f}:1)")
            return None
        
        # 8. Create unique signal hash
        unique_id = f"{symbol}:{side}:{current_price:.8f}:{time.time_ns()}"
        signal_hash = hashlib.md5(unique_id.encode()).hexdigest()
        
        # Create signal
        signal = {
            'symbol': symbol,
            'side': side,
            'entry': current_price,
            'sl': sl,
            'tp': tp,
            'status': 'OPEN',
            
            'timeframe_alignment': tf_reason[:80],
            'wave_structure': wave_reason[:80],
            'strength_level': strength_reason[:80],
            'indicators_signal': indicators_reason[:80],
            'volume_status': volume_reason[:80],
            'synthesis_score': total_score,
            
            'signal_hash': signal_hash,
            'price_hash': price_hash
        }
        
        log.info(f"✅ SIGNAL: {symbol} {side} @ {current_price:.4f} | Score: {total_score:.2f} | R:R: {rr_ratio:.1f}:1")
        
        return signal
        
    except Exception as e:
        log.error(f"Signal generation error for {symbol}: {e}")
        return None

# ================ DATA FETCHING ================

async def fetch_ohlcv_data(exchange, symbol: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Fetch OHLCV data for all timeframes"""
    data = {}
    
    for tf_name, tf in TIMEFRAMES.items():
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=50)
            
            if ohlcv and len(ohlcv) >= 20:
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                # Convert to numeric
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Remove any NaN values
                df = df.dropna()
                
                if len(df) >= 15:
                    data[tf_name] = df
                else:
                    log.debug(f"{symbol} {tf}: Not enough data after cleaning")
            else:
                log.debug(f"{symbol} {tf}: No data or insufficient length")
                
        except Exception as e:
            log.debug(f"{symbol} {tf} fetch error: {e}")
            continue
    
    # Check if we have all required timeframes
    required_tfs = ['DAILY', 'H4', 'H1', 'M15']
    for tf in required_tfs:
        if tf not in data:
            log.debug(f"{symbol}: Missing {tf} data")
            return None
    
    return data

# ================ SCANNING LOOP ================

async def scanning_loop(exchange):
    """Main scanning loop"""
    log.info("🚀 Starting scanner with duplicate prevention")
    
    # Test Telegram
    await tg("🚀 Scanner started with duplicate prevention!")
    
    while True:
        try:
            log.info("=" * 50)
            log.info("Starting new scan cycle...")
            
            # Get top volume pairs
            try:
                tickers = await exchange.fetch_tickers()
                usdt_pairs = []
                
                for symbol, ticker in tickers.items():
                    if symbol.endswith('/USDT'):
                        volume = ticker.get('quoteVolume', 0)
                        if volume > 1000000:  # $1M minimum volume
                            usdt_pairs.append((symbol, volume))
                
                usdt_pairs.sort(key=lambda x: x[1], reverse=True)
                top_pairs = usdt_pairs[:TOP_N]
                
                log.info(f"Found {len(top_pairs)} pairs with sufficient volume")
                
            except Exception as e:
                log.error(f"Error fetching tickers: {e}")
                await asyncio.sleep(30)
                continue
            
            signals_found = 0
            
            for symbol, volume in top_pairs:
                try:
                    log.debug(f"Processing {symbol}...")
                    
                    # Fetch data
                    data = await fetch_ohlcv_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # Generate signal
                    signal = await generate_signal_simple(data, symbol)
                    
                    if signal:
                        # Save to database
                        async with db_lock:
                            # Final duplicate check with signal hash
                            async with db_conn.execute(
                                "SELECT COUNT(*) FROM signals WHERE signal_hash = ?",
                                (signal['signal_hash'],)
                            ) as cursor:
                                exists = (await cursor.fetchone())[0]
                            
                            if exists == 0:
                                # Insert new signal
                                await db_conn.execute("""
                                    INSERT INTO signals (
                                        symbol, side, entry, sl, tp, status,
                                        timeframe_alignment, wave_structure, strength_level,
                                        indicators_signal, volume_status, synthesis_score,
                                        signal_hash, price_hash
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    signal['symbol'], signal['side'], signal['entry'], signal['sl'],
                                    signal['tp'], signal['status'], signal['timeframe_alignment'],
                                    signal['wave_structure'], signal['strength_level'],
                                    signal['indicators_signal'], signal['volume_status'],
                                    signal['synthesis_score'], signal['signal_hash'], signal['price_hash']
                                ))
                                
                                await db_conn.commit()
                                
                                # Send Telegram alert
                                side_ar = "شراء" if signal['side'] == "BUY" else "بيع"
                                risk_pct = abs(signal['entry'] - signal['sl']) / signal['entry'] * 100
                                reward_pct = abs(signal['tp'] - signal['entry']) / signal['entry'] * 100
                                rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0
                                
                                message = f"""
🎯 **إشارة تداول جديدة**

**{signal['symbol']}** | **{side_ar}**

📊 **التحليل:**
• الفريمات: {signal['timeframe_alignment']}
• الموجة: {signal['wave_structure']}
• القوة: {signal['strength_level']}
• المؤشرات: {signal['indicators_signal']}
• الفوليوم: {signal['volume_status']}

📈 **الجودة: {signal['synthesis_score']:.1%}**

💰 **التنفيذ:**
• الدخول: {signal['entry']:.4f}
• وقف الخسارة: {signal['sl']:.4f}
• هدف الربح: {signal['tp']:.4f}
• نسبة الربح/المخاطرة: {rr_ratio:.1f}:1

⏰ **المتابعة التلقائية مفعلة**

#{side_ar} #تداول
"""
                                
                                await tg(message)
                                signals_found += 1
                                log.info(f"✅ Signal sent for {symbol}")
                            else:
                                log.debug(f"Final duplicate check failed for {symbol}")
                    
                    # Small delay between symbols
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    log.error(f"Error processing {symbol}: {e}")
                    continue
            
            log.info(f"Scan complete. Found {signals_found} new signals.")
            
            # Wait for next scan
            log.info(f"Waiting {SCAN_INTERVAL} seconds for next scan...")
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scan loop error: {e}")
            await asyncio.sleep(30)

# ================ MONITORING ================

async def monitoring_loop(exchange):
    """Monitor open positions"""
    log.info("Starting monitoring loop...")
    
    while True:
        try:
            async with db_lock:
                async with db_conn.execute("""
                    SELECT id, symbol, side, entry, sl, tp FROM signals 
                    WHERE status='OPEN'
                """) as cursor:
                    open_positions = await cursor.fetchall()
            
            if open_positions:
                log.info(f"Monitoring {len(open_positions)} open positions")
            
            for pos_id, symbol, side, entry, sl, tp in open_positions:
                try:
                    # Get current price
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = ticker.get('last')
                    
                    if not current_price:
                        continue
                    
                    # Check if position should be closed
                    close_reason = None
                    pnl_percent = 0
                    
                    if side == "BUY":
                        if current_price >= tp:
                            close_reason = "TP_HIT"
                            pnl_percent = ((current_price - entry) / entry) * 100
                        elif current_price <= sl:
                            close_reason = "SL_HIT"
                            pnl_percent = ((current_price - entry) / entry) * 100
                    
                    else:  # SELL
                        if current_price <= tp:
                            close_reason = "TP_HIT"
                            pnl_percent = ((entry - current_price) / entry) * 100
                        elif current_price >= sl:
                            close_reason = "SL_HIT"
                            pnl_percent = ((entry - current_price) / entry) * 100
                    
                    # Close position if needed
                    if close_reason:
                        async with db_lock:
                            await db_conn.execute("""
                                UPDATE signals SET 
                                    status = 'CLOSED',
                                    close_reason = ?,
                                    close_price = ?,
                                    close_timestamp = CURRENT_TIMESTAMP,
                                    pnl_percent = ?
                                WHERE id = ?
                            """, (close_reason, current_price, pnl_percent, pos_id))
                            
                            await db_conn.commit()
                        
                        # Send notification
                        side_ar = "شراء" if side == "BUY" else "بيع"
                        result = "هدف الربح" if close_reason == "TP_HIT" else "وقف الخسارة"
                        
                        await tg(f"""
{'✅' if close_reason == 'TP_HIT' else '❌'} **تم إغلاق الصفقة**

**{symbol}** | **{side_ar}**
النتيجة: {result}

• الدخول: {entry:.4f}
• الإغلاق: {current_price:.4f}
• {result}: {tp if close_reason == 'TP_HIT' else sl:.4f}

• الربح/الخسارة: {'+' if pnl_percent > 0 else ''}{pnl_percent:.2f}%

#إغلاق #{"ربح" if pnl_percent > 0 else "خسارة"}
""")
                        
                        log.info(f"{'✅' if close_reason == 'TP_HIT' else '❌'} {symbol}: {close_reason} | PnL: {pnl_percent:.2f}%")
                
                except Exception as e:
                    log.error(f"Monitor error for {symbol}: {e}")
                    continue
            
            # Wait before next check
            await asyncio.sleep(10)
            
        except Exception as e:
            log.error(f"Monitoring loop error: {e}")
            await asyncio.sleep(30)

# ================ WEB API ================

app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "running",
        "scanner": "Visual Synthesis Scanner",
        "version": "2.0 - No Duplicates",
        "min_score": MIN_SYNTHESIS_SCORE,
        "scan_interval": SCAN_INTERVAL
    }

@app.get("/test-telegram")
async def test_telegram():
    """Test Telegram connectivity"""
    try:
        await tg("🔔 Test message from scanner API")
        return {"status": "success", "message": "Test message sent"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/force-scan")
async def force_scan():
    """Force an immediate scan"""
    return {
        "status": "queued",
        "message": "Scan will run on next cycle",
        "note": "Use /test-signal for immediate test"
    }

@app.get("/test-signal")
async def test_signal():
    """Generate and send a test signal"""
    try:
        test_signal = {
            'symbol': 'BTC/USDT',
            'side': 'BUY',
            'entry': 50000.00,
            'sl': 49000.00,
            'tp': 52000.00,
            'synthesis_score': 0.75
        }
        
        side_ar = "شراء" if test_signal['side'] == "BUY" else "بيع"
        
        await tg(f"""
🧪 **إشارة اختبار**

**{test_signal['symbol']}** | **{side_ar}**

هذه إشارة اختبار للتأكد من عمل النظام.

• الدخول: {test_signal['entry']:.2f}
• وقف الخسارة: {test_signal['sl']:.2f}
• هدف الربح: {test_signal['tp']:.2f}
• الجودة: {test_signal['synthesis_score']:.1%}

#اختبار #تست
""")
        
        return {
            "status": "success",
            "message": "Test signal sent",
            "signal": test_signal
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/stats")
async def get_stats():
    """Get scanner statistics"""
    try:
        async with db_lock:
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status='OPEN'") as cursor:
                open_count = (await cursor.fetchone())[0]
            
            async with db_conn.execute("SELECT COUNT(*) FROM signals WHERE status='CLOSED'") as cursor:
                closed_count = (await cursor.fetchone())[0]
            
            async with db_conn.execute("SELECT COUNT(*) FROM signals") as cursor:
                total_count = (await cursor.fetchone())[0]
            
            # Try to get duplicate prevention stats (might fail if price_hash column doesn't exist yet)
            try:
                async with db_conn.execute("""
                    SELECT COUNT(DISTINCT price_hash) as unique_signals,
                           COUNT(*) as total_attempts
                    FROM signals 
                    WHERE timestamp > datetime('now', '-24 hours')
                """) as cursor:
                    dup_stats = await cursor.fetchone()
                    if dup_stats and dup_stats[0] is not None:
                        unique_signals = dup_stats[0]
                        total_attempts = dup_stats[1]
                        duplicates_blocked = total_attempts - unique_signals if total_attempts > unique_signals else 0
                    else:
                        unique_signals = total_count
                        duplicates_blocked = 0
            except:
                unique_signals = total_count
                duplicates_blocked = 0
        
        return {
            "open_positions": open_count,
            "closed_positions": closed_count,
            "total_signals": total_count,
            "unique_signals_24h": unique_signals,
            "duplicates_blocked_24h": duplicates_blocked,
            "scan_interval": SCAN_INTERVAL,
            "min_score": MIN_SYNTHESIS_SCORE,
            "duplicate_prevention": "ACTIVE"
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/recent-signals")
async def get_recent_signals(limit: int = 10):
    """Get recent signals"""
    try:
        async with db_lock:
            # Set row factory to get dictionaries
            db_conn.row_factory = aiosqlite.Row
            
            async with db_conn.execute("""
                SELECT symbol, side, entry, sl, tp, synthesis_score, timestamp, status
                FROM signals 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                
                signals = []
                for row in rows:
                    signals.append(dict(row))
                
                return {
                    "status": "success",
                    "signals": signals,
                    "count": len(signals)
                }
    except Exception as e:
        return {"error": str(e)}

# ================ MAIN ================

async def main():
    global exchange
    
    log.info("=" * 60)
    log.info("🚀 VISUAL SCANNER - NO DUPLICATE SIGNALS")
    log.info("=" * 60)
    
    # Initialize database
    if not await init_db():
        log.error("Failed to initialize database")
        return
    
    # Initialize exchange
    try:
        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000
        })
        
        # Test connection
        ticker = await exchange.fetch_ticker("BTC/USDT")
        log.info(f"✅ Exchange connected. BTC price: {ticker['last']}")
        
    except Exception as e:
        log.error(f"Failed to connect to exchange: {e}")
        return
    
    # Send startup message
    await tg(f"""
🚀 **الماسح الضوئي البصري - بدون إشارات مكررة**

✅ **تم بدء التشغيل بنجاح**

**ميزات مكافحة التكرار:**
• فحص التكرار بالسعر والظروف
• حد أقصى إشارتين لكل زوج في 6 ساعات
• منع الإشارات المتشابهة في 4 ساعات
• تتبع تغير الأسعار

**الإعدادات:**
• الحد الأدنى للجودة: {MIN_SYNTHESIS_SCORE}
• فاصل المسح: {SCAN_INTERVAL} ثانية
• عدد الأزواج: {TOP_N}

جاهز للعمل! سيبدأ المسح الآن...

#بدء #تشغيل
""")
    
    # Start scanning and monitoring loops
    try:
        await asyncio.gather(
            scanning_loop(exchange),
            monitoring_loop(exchange)
        )
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
        await tg("🛑 توقف الماسح يدوياً")
    except Exception as e:
        log.error(f"Scanner crashed: {e}")
        await tg(f"❌ تعطل الماسح: {str(e)[:100]}")
    finally:
        if exchange:
            await exchange.close()
        if db_conn:
            await db_conn.close()
        log.info("Scanner shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())