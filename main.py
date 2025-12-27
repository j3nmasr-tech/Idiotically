#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visual Synthesis Scanner - مع منع الإشارات المكررة
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
from collections import defaultdict

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/signals.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))

# Easier thresholds for more signals
MIN_SYNTHESIS_SCORE = 0.55  # Lowered from 0.6

# Timeframes - using fewer for reliability
TIMEFRAMES = {
    "DAILY": "1d",
    "H4": "4h",
    "H1": "1h",
    "M15": "15m"
}

# ---------------- GLOBAL STATE FOR DEDUPLICATION ----------------
# Store recent signals to prevent duplicates
recent_signals = {}  # symbol -> (side, entry, timestamp)
signal_cooldown = {}  # symbol -> last_signal_time
COOLDOWN_PERIOD = 3600  # 1 hour cooldown for same symbol

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
async def init_db():
    """Initialize database"""
    global db_conn
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        
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
                signal_hash TEXT UNIQUE,
                price_hash TEXT  -- NEW: Hash of price conditions to detect duplicates
            )
        """)
        
        await db_conn.commit()
        log.info("✅ Database ready")
        return True
        
    except Exception as e:
        log.error(f"Database error: {e}")
        return False

# ================ FIXED: SIMPLIFIED ANALYSIS WITH DEDUPLICATION ================

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

# (Keep other analysis functions the same as your original)

# ================ FIXED: SIGNAL GENERATION WITH DEDUPLICATION ================

async def generate_signal_simple(data: Dict[str, pd.DataFrame], symbol: str) -> Optional[Dict]:
    """Generate signal with simple logic and duplicate prevention"""
    try:
        log.info(f"Analyzing {symbol}...")
        
        # 1. Timeframe analysis
        direction, tf_score, tf_reason = analyze_timeframes_simple(data)
        
        if direction == "NEUTRAL":
            log.debug(f"{symbol}: No clear direction")
            return None
        
        # 2. Check cooldown
        current_time = time.time()
        if symbol in signal_cooldown:
            time_since_last = current_time - signal_cooldown[symbol]
            if time_since_last < COOLDOWN_PERIOD:
                log.debug(f"{symbol}: In cooldown ({time_since_last:.0f}s / {COOLDOWN_PERIOD}s)")
                return None
        
        # 3. Other analyses
        wave_score, wave_reason = analyze_wave_simple(data, direction)
        strength_score, strength_reason = analyze_strength_simple(data, direction)
        indicators_score, indicators_reason = analyze_indicators_simple(data, direction)
        volume_score, volume_reason = analyze_volume_simple(data, direction)
        
        # 4. Calculate total score
        scores = [tf_score, wave_score, strength_score, indicators_score, volume_score]
        total_score = np.mean(scores)
        
        log.info(f"{symbol}: Total score {total_score:.2f}")
        
        # 5. Check if score is good enough
        if total_score < MIN_SYNTHESIS_SCORE:
            log.debug(f"{symbol}: Score too low ({total_score:.2f} < {MIN_SYNTHESIS_SCORE})")
            return None
        
        # 6. Get current price
        current_price = data['M15']['close'].iloc[-1]
        side = "BUY" if direction == "UP" else "SELL"
        
        # 7. Create price hash to detect duplicate conditions
        # This hash is based on the actual price conditions, not time
        price_conditions = f"{symbol}:{side}:{current_price:.6f}:{tf_score:.4f}:{wave_score:.4f}"
        price_hash = hashlib.md5(price_conditions.encode()).hexdigest()
        
        # 8. Check if this exact condition was already signaled
        async with db_lock:
            async with db_conn.execute(
                "SELECT COUNT(*) FROM signals WHERE price_hash = ? AND timestamp > datetime('now', '-2 hours')",
                (price_hash,)
            ) as cursor:
                exists = (await cursor.fetchone())[0]
            
            if exists > 0:
                log.debug(f"{symbol}: Duplicate price condition detected")
                return None
        
        # 9. Calculate SL/TP
        sl, tp, sltp_logic = calculate_simple_sltp(current_price, side, data)
        
        # Calculate risk/reward
        risk = abs(current_price - sl)
        reward = abs(tp - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        # Skip if R:R is too poor
        if rr_ratio < 1.0:
            log.debug(f"{symbol}: Poor R:R ratio ({rr_ratio:.1f}:1)")
            return None
        
        # 10. Create unique signal hash with nanosecond precision
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
            'price_hash': price_hash  # Store price hash for duplicate detection
        }
        
        # Update cooldown
        signal_cooldown[symbol] = current_time
        
        log.info(f"✅ SIGNAL: {symbol} {side} @ {current_price:.4f} | Score: {total_score:.2f}")
        
        return signal
        
    except Exception as e:
        log.error(f"Signal generation error for {symbol}: {e}")
        return None

# ================ FIXED: SCANNING LOOP WITH IMPROVED DEDUPLICATION ================

async def scanning_loop(exchange):
    """Main scanning loop with duplicate prevention"""
    log.info("🚀 Starting scanner with duplicate prevention")
    
    await tg("🚀 Scanner started with duplicate prevention!")
    
    while True:
        try:
            log.info("=" * 50)
            log.info("Starting new scan cycle...")
            
            # Clean old cooldowns (older than 2 hours)
            current_time = time.time()
            for symbol in list(signal_cooldown.keys()):
                if current_time - signal_cooldown[symbol] > COOLDOWN_PERIOD * 2:
                    del signal_cooldown[symbol]
            
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
                    
                    # Skip if in cooldown
                    if symbol in signal_cooldown:
                        time_since = current_time - signal_cooldown[symbol]
                        if time_since < 300:  # 5 minutes quick cooldown
                            continue
                    
                    # Fetch data
                    data = await fetch_ohlcv_data(exchange, symbol)
                    if not data:
                        continue
                    
                    # Generate signal
                    signal = await generate_signal_simple(data, symbol)
                    
                    if signal:
                        # Save to database
                        async with db_lock:
                            # Double-check for duplicates
                            async with db_conn.execute(
                                "SELECT COUNT(*) FROM signals WHERE price_hash = ? AND timestamp > datetime('now', '-1 hour')",
                                (signal['price_hash'],)
                            ) as cursor:
                                exists = (await cursor.fetchone())[0]
                            
                            if exists == 0:
                                # Also check for recent similar signals
                                async with db_conn.execute("""
                                    SELECT COUNT(*) FROM signals 
                                    WHERE symbol = ? AND side = ? 
                                    AND timestamp > datetime('now', '-6 hours')
                                """, (signal['symbol'], signal['side'])) as cursor:
                                    recent_count = (await cursor.fetchone())[0]
                                
                                if recent_count >= 3:
                                    log.debug(f"{symbol}: Too many recent signals ({recent_count})")
                                    continue
                                
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
                                
                                # Update cooldown
                                signal_cooldown[symbol] = time.time()
                                
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
                                log.debug(f"Duplicate price condition for {symbol}")
                    
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

# (Keep the rest of your code the same - monitoring loop, web API, etc.)

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
• فترة تبريد لكل زوج
• حد أقصى 3 إشارات في 6 ساعات

**الإعدادات:**
• الحد الأدنى للجودة: {MIN_SYNTHESIS_SCORE}
• فاصل المسح: {SCAN_INTERVAL} ثانية
• فترة التبريد: {COOLDOWN_PERIOD//3600} ساعة

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