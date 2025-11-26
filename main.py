#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🎯 ULTIMATE ROMEOPT SCANNER - BINGX EDITION 🎯
- YOUR PROVEN OLD v3.1 SIGNAL GENERATION
- BINGX API ONLY 
- NO FILTERS, NO RESTRICTIONS
- PURE ROMEOPT INSTITUTIONAL SEQUENCING
- REAL-TIME ACTIONABLE SIGNALS
"""

import os
import time
import asyncio
import logging
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import httpx
import pandas as pd
import numpy as np
from collections import defaultdict, deque

# ==================== BINGX API ====================

class BingXAPI:
    """BingX API - Simple & Effective"""
    
    def __init__(self):
        self.base_url = "https://open-api.bingx.com"
    
    def _get_timestamp(self) -> int:
        return int(time.time() * 1000)
    
    def _format_symbol(self, symbol: str) -> str:
        return symbol.replace('/', '-')
    
    def _safe_float_convert(self, value) -> float:
        try:
            if isinstance(value, str):
                cleaned = value.replace('%', '').replace(',', '').strip()
                return float(cleaned)
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    async def fetch_ohlcv(self, symbol: str, timeframe: str = '15m', limit: int = 100) -> Optional[List]:
        try:
            tf_mapping = {
                '1m': '1m', '3m': '3m', '5m': '5m', 
                '15m': '15m', '30m': '30m', '1h': '1h'
            }
            bingx_tf = tf_mapping.get(timeframe, '15m')
            
            endpoint = "/openApi/spot/v1/market/kline"
            formatted_symbol = self._format_symbol(symbol)
            
            params = {
                'symbol': formatted_symbol,
                'interval': bingx_tf,
                'limit': limit,
                'timestamp': self._get_timestamp()
            }
            
            url = f"{self.base_url}{endpoint}"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == 0 and 'data' in data:
                    ohlcv_data = []
                    for candle in data['data']:
                        ohlcv_data.append([
                            candle[0],
                            self._safe_float_convert(candle[1]),
                            self._safe_float_convert(candle[2]),
                            self._safe_float_convert(candle[3]),
                            self._safe_float_convert(candle[4]),
                            self._safe_float_convert(candle[5])
                        ])
                    return ohlcv_data
                return None
                    
        except Exception as e:
            logging.debug(f"BingX OHLCV error {symbol}: {e}")
            return None
    
    async def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        try:
            endpoint = "/openApi/spot/v1/ticker/24hr"
            formatted_symbol = self._format_symbol(symbol)
            
            params = {
                'symbol': formatted_symbol,
                'timestamp': self._get_timestamp()
            }
            
            url = f"{self.base_url}{endpoint}"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == 0 and 'data' in data:
                    ticker_data = data['data']
                    return {
                        'symbol': symbol,
                        'last': self._safe_float_convert(ticker_data.get('lastPrice', 0)),
                        'bid': self._safe_float_convert(ticker_data.get('bidPrice', 0)),
                        'ask': self._safe_float_convert(ticker_data.get('askPrice', 0)),
                        'volume': self._safe_float_convert(ticker_data.get('volume', 0))
                    }
                return None
                    
        except Exception as e:
            logging.debug(f"BingX ticker error {symbol}: {e}")
            return None

    async def fetch_tickers(self) -> Dict:
        """Fetch all tickers for symbol selection"""
        try:
            endpoint = "/openApi/spot/v1/ticker/24hr"
            
            params = {
                'timestamp': self._get_timestamp()
            }
            
            url = f"{self.base_url}{endpoint}"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                tickers = {}
                if data.get('code') == 0 and 'data' in data:
                    for ticker_data in data['data']:
                        symbol_str = ticker_data.get('symbol', '')
                        if '-' in symbol_str:
                            standard_symbol = symbol_str.replace('-', '/')
                            tickers[standard_symbol] = {
                                'symbol': standard_symbol,
                                'last': self._safe_float_convert(ticker_data.get('lastPrice', 0)),
                                'volume': self._safe_float_convert(ticker_data.get('volume', 0))
                            }
                    return tickers
                return {}
                
        except Exception as e:
            logging.error(f"BingX tickers fetch error: {e}")
            return {}

# ==================== PURE ROMEOPT SEQUENCING ====================

class PureRomeAnalyzer:
    """YOUR PROVEN ROMEOPT LOGIC - UNCHANGED"""
    
    def __init__(self):
        self.sequence_complete = False
        
    def generate_signal(self, df: pd.DataFrame, symbol: str, context=None) -> Optional[Dict]:
        """YOUR EXACT OLD SIGNAL GENERATION"""
        if context is None:
            context = {}
            
        if df is None or len(df) < 15:
            return None

        try:
            current_price = df["close"].iloc[-1]
            context['current_price'] = current_price
            
            # 🔥 STEP 1: Liquidity Sweep
            sweep_result = self._check_liquidity_sweep(df)
            if not sweep_result["valid"]:
                return None
            
            # 🔥 STEP 2: Displacement  
            displacement_result = self._check_displacement(df, sweep_result)
            if not displacement_result["valid"]:
                return None
            
            # 🔥 STEP 3: Retracement Zone
            zone_result = self._check_retracement_zone(df, displacement_result, context)
            if not zone_result["valid"]:
                return None
            
            # 🔥 STEP 4: Premium/Discount
            equilibrium_result = self._check_premium_discount(df, zone_result, context)
            if not equilibrium_result["valid"]:
                return None
            
            # 🔥 STEP 5: HTF Alignment
            htf_result = self._check_htf_alignment(df, equilibrium_result, context)
            if not htf_result["valid"]:
                return None
            
            # 🔥 STEP 6: Momentum
            momentum_result = self._check_momentum(df, htf_result, context)
            if not momentum_result["valid"]:
                return None
            
            # ✅ ALL CONDITIONS MET
            self.sequence_complete = True
            return self._format_signal(momentum_result, symbol, context)
            
        except Exception as e:
            logging.error(f"Rome error {symbol}: {e}")
            return None

    def _check_liquidity_sweep(self, df: pd.DataFrame) -> Dict:
        """STEP 1: Liquidity sweep detection"""
        if len(df) < 10:
            return {"valid": False}
            
        recent_candles = df.iloc[-8:]  # Last 8 candles
        
        for i in range(1, len(recent_candles)):
            current = recent_candles.iloc[i]
            previous = recent_candles.iloc[i-1]
            lookback = recent_candles.iloc[:i]
            
            # Equal highs sweep
            if self._is_equal_high_sweep(current, previous, lookback):
                return {
                    "valid": True, 
                    "type": "equal_high_sweep", 
                    "direction": "bearish"
                }
            
            # Equal lows sweep  
            if self._is_equal_low_sweep(current, previous, lookback):
                return {
                    "valid": True, 
                    "type": "equal_low_sweep", 
                    "direction": "bullish"
                }
            
            # Stop-run
            stop_run = self._is_stop_run_sweep(current, df)
            if stop_run["valid"]:
                return stop_run
        
        return {"valid": False}

    def _is_equal_high_sweep(self, current, previous, lookback_candles) -> bool:
        if current["high"] <= previous["high"]:
            return False
        recent_highs = lookback_candles["high"].tail(4)
        if len(recent_highs) == 0:
            return False
        atr_val = self._calculate_atr(lookback_candles)
        threshold = atr_val * 0.15 if atr_val else current["high"] * 0.0015
        for high_val in recent_highs:
            if abs(current["high"] - high_val) < threshold:
                return True
        return False

    def _is_equal_low_sweep(self, current, previous, lookback_candles) -> bool:
        if current["low"] >= previous["low"]:
            return False
        recent_lows = lookback_candles["low"].tail(4)
        if len(recent_lows) == 0:
            return False
        atr_val = self._calculate_atr(lookback_candles)
        threshold = atr_val * 0.15 if atr_val else current["low"] * 0.0015
        for low_val in recent_lows:
            if abs(current["low"] - low_val) < threshold:
                return True
        return False

    def _is_stop_run_sweep(self, current_candle, df: pd.DataFrame) -> Dict:
        if len(df) < 8:
            return {"valid": False}
        swing_highs = self._find_swing_highs(df.tail(12))
        swing_lows = self._find_swing_lows(df.tail(12))
        
        for swing_high in swing_highs[-2:]:
            if (current_candle["high"] > swing_high and 
                current_candle["close"] < swing_high):
                return {"valid": True, "type": "stop_run_high", "direction": "bearish"}
        
        for swing_low in swing_lows[-2:]:
            if (current_candle["low"] < swing_low and 
                current_candle["close"] > swing_low):
                return {"valid": True, "type": "stop_run_low", "direction": "bullish"}
        
        return {"valid": False}

    def _check_displacement(self, df: pd.DataFrame, sweep_result: Dict) -> Dict:
        """STEP 2: Displacement after sweep"""
        sweep_idx = -5  # Simplified
        start_idx = max(0, len(df) + sweep_idx + 1)
        post_sweep = df.iloc[start_idx:start_idx + 3]
        
        if len(post_sweep) == 0:
            return {"valid": False}
        
        impulse_candle = None
        for i, candle in post_sweep.iterrows():
            body_size = abs(candle["close"] - candle["open"])
            full_range = candle["high"] - candle["low"]
            
            if full_range > 0 and (body_size / full_range) >= 0.5:
                impulse_candle = candle
                break
        
        if impulse_candle is None:
            return {"valid": False}
        
        direction = sweep_result["direction"]
        is_bullish = impulse_candle["close"] > impulse_candle["open"]
        
        if direction == "bullish" and not is_bullish:
            return {"valid": False}
        if direction == "bearish" and is_bullish:
            return {"valid": False}
        
        return {"valid": True, "direction": direction}

    def _check_retracement_zone(self, df: pd.DataFrame, displacement_result: Dict, context: Dict) -> Dict:
        """STEP 3: Retracement into zone"""
        current_price = df["close"].iloc[-1]
        direction = displacement_result["direction"]
        
        fvg_zone = self._find_fvg_zone(df, direction)
        if fvg_zone and self._price_approaching_zone(current_price, fvg_zone):
            return {"valid": True, "zone_type": "fvg", "direction": direction}
        
        ob_zone = self._find_order_block(df, direction)
        if ob_zone and self._price_approaching_zone(current_price, ob_zone):
            return {"valid": True, "zone_type": "order_block", "direction": direction}
        
        return {"valid": False}

    def _check_premium_discount(self, df: pd.DataFrame, zone_result: Dict, context: Dict) -> Dict:
        """STEP 4: Premium/discount filter"""
        current_price = df["close"].iloc[-1]
        direction = zone_result["direction"]
        
        swing_highs = self._find_swing_highs(df.tail(15))
        swing_lows = self._find_swing_lows(df.tail(15))
        
        if not swing_highs or not swing_lows:
            return {"valid": False}
            
        recent_high = max(swing_highs[-2:])
        recent_low = min(swing_lows[-2:])
        equilibrium = (recent_high + recent_low) / 2
        
        if direction == "bullish" and current_price > equilibrium * 1.02:
            return {"valid": False}
        if direction == "bearish" and current_price < equilibrium * 0.98:
            return {"valid": False}
        
        return {"valid": True, "direction": direction}

    def _check_htf_alignment(self, df: pd.DataFrame, equilibrium_result: Dict, context: Dict) -> Dict:
        """STEP 5: HTF alignment"""
        direction = equilibrium_result["direction"]
        htf_data = context.get('df_15m')
        
        # NO HTF DATA? NO PROBLEM - DON'T BLOCK SIGNALS
        if htf_data is None or len(htf_data) < 10:
            return {"valid": True, "direction": direction}  # Allow without HTF
        
        htf_trend = self._detect_trend(htf_data)
        
        if direction == "bullish" and htf_trend == "bearish":
            return {"valid": False}
        if direction == "bearish" and htf_trend == "bullish":
            return {"valid": False}
        
        return {"valid": True, "direction": direction}

    def _check_momentum(self, df: pd.DataFrame, htf_result: Dict, context: Dict) -> Dict:
        """STEP 6: Momentum confirmation"""
        direction = htf_result["direction"]
        current_candle = df.iloc[-1]
        
        if direction == "bullish":
            if not (current_candle["close"] >= current_candle["open"]):
                return {"valid": False}
        else:
            if not (current_candle["close"] <= current_candle["open"]):
                return {"valid": False}
        
        return {"valid": True, "direction": direction}

    def _format_signal(self, final_result: Dict, symbol: str, context: Dict) -> Dict:
        """Format signal with aggressive TP/SL"""
        direction = final_result["direction"]
        side = "BUY" if direction == "bullish" else "SELL"
        current_price = context.get('current_price', 0)
        tf = context.get('tf', '15m')
        
        # AGGRESSIVE TP/SL FOR QUICK MOVES
        if side == "BUY":
            sl = current_price * 0.997
            tp1 = current_price * 1.006
            tp2 = current_price * 1.012
            tp3 = current_price * 1.018
        else:
            sl = current_price * 1.003
            tp1 = current_price * 0.994
            tp2 = current_price * 0.988
            tp3 = current_price * 0.982
        
        return {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "score": 11,
            "reason": "PURE ROMEOPT - NO FILTERS",
            "timeframe": tf,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

    # ==================== UTILITY METHODS ====================
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        if len(df) < period: return 0.0
        high, low, close = df["high"], df["low"], df["close"]
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        return true_range.rolling(period).mean().iloc[-1]

    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 2) -> List[float]:
        if len(df) < lookback * 2 + 1: return []
        highs = []
        for i in range(lookback, len(df) - lookback):
            if (df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max()):
                highs.append(df["high"].iloc[i])
        return highs

    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 2) -> List[float]:
        if len(df) < lookback * 2 + 1: return []
        lows = []
        for i in range(lookback, len(df) - lookback):
            if (df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min()):
                lows.append(df["low"].iloc[i])
        return lows

    def _find_fvg_zone(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        if len(df) < 3: return None
        for i in range(len(df) - 3, max(0, len(df) - 8), -1):
            if i + 2 >= len(df): continue
            c1, c2 = df.iloc[i], df.iloc[i+1]
            if direction == "bullish" and c2["low"] > c1["high"]:
                return {"low": c1["high"], "high": c2["low"]}
            elif direction == "bearish" and c2["high"] < c1["low"]:
                return {"low": c2["high"], "high": c1["low"]}
        return None

    def _find_order_block(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        if len(df) < 5: return None
        for i in range(len(df) - 5, max(0, len(df) - 12), -1):
            if i >= len(df): continue
            candle = df.iloc[i]
            body_size = abs(candle["close"] - candle["open"])
            full_range = candle["high"] - candle["low"]
            if body_size / full_range >= 0.5:
                if direction == "bullish" and candle["close"] > candle["open"]:
                    return {"low": candle["low"], "high": candle["open"]}
                elif direction == "bearish" and candle["close"] < candle["open"]:
                    return {"low": candle["close"], "high": candle["high"]}
        return None

    def _price_approaching_zone(self, price: float, zone: Dict, threshold_pct: float = 0.003) -> bool:
        zone_mid = (zone["low"] + zone["high"]) / 2
        distance_pct = abs(price - zone_mid) / price
        return distance_pct <= threshold_pct

    def _detect_trend(self, df: pd.DataFrame) -> str:
        if len(df) < 20: return "neutral"
        ema_20 = df["close"].ewm(span=20).mean().iloc[-1]
        current_price = df["close"].iloc[-1]
        if current_price > ema_20: return "bullish"
        elif current_price < ema_20: return "bearish"
        return "neutral"

# ==================== LEAN SCANNER ====================

class LeanRomeScanner:
    """ULTRA-LEAN SCANNER - SIGNALS ONLY"""
    
    def __init__(self):
        self.bingx = BingXAPI()
        self.analyzer = PureRomeAnalyzer()
        self.signals_sent = set()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s'
        )
        logging.info("🎯 LEAN ROMEOPT SCANNER STARTED")
        logging.info("✅ Pure RomeOPT sequencing only")
        logging.info("✅ BingX API")
        logging.info("✅ NO FILTERS - PURE SIGNALS")

    async def fetch_ohlcv(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data"""
        try:
            ohlcv = await self.bingx.fetch_ohlcv(symbol, timeframe, 100)
            if not ohlcv or len(ohlcv) < 15: return None
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except:
            return None

    async def scan_symbol(self, symbol: str) -> List[Dict]:
        """Scan single symbol for RomeOPT signals"""
        signals = []
        
        try:
            # SCAN ALL TIMEFRAMES
            for tf in ["1m", "3m", "5m", "15m", "30m"]:
                df = await self.fetch_ohlcv(symbol, tf)
                if df is None: continue
                
                # Get HTF data for context (but don't block if missing)
                htf_data = None
                if tf in ["1m", "3m", "5m"]:
                    htf_data = await self.fetch_ohlcv(symbol, "15m")
                
                context = {
                    'tf': tf,
                    'df_15m': htf_data
                }
                
                signal = self.analyzer.generate_signal(df, symbol, context)
                if signal:
                    signal_id = f"{symbol}_{tf}_{signal['side']}_{int(time.time() / 60)}"  # Unique per minute
                    if signal_id not in self.signals_sent:
                        signals.append(signal)
                        self.signals_sent.add(signal_id)
                        logging.info(f"🎯 SIGNAL: {symbol} {tf} {signal['side']} | Entry: {signal['entry']:.6f}")
        
        except Exception as e:
            logging.error(f"Scan error {symbol}: {e}")
            
        return signals

    async def get_active_symbols(self) -> List[str]:
        """Get active trading symbols with volume"""
        try:
            tickers = await self.bingx.fetch_tickers()
            if not tickers:
                return self._get_fallback_symbols()
                
            symbols_data = []
            for symbol, ticker in tickers.items():
                if not symbol.endswith('/USDT'):
                    continue
                volume = ticker.get('volume', 0)
                symbols_data.append({'symbol': symbol, 'volume': volume})
            
            symbols_data.sort(key=lambda x: x['volume'], reverse=True)
            top_symbols = [s['symbol'] for s in symbols_data[:50]]  # Top 50 by volume
            
            logging.info(f"📊 Selected {len(top_symbols)} symbols by volume")
            return top_symbols
            
        except Exception as e:
            logging.error(f"Error getting symbols: {e}")
            return self._get_fallback_symbols()

    def _get_fallback_symbols(self) -> List[str]:
        """Fallback symbols if API fails"""
        return [
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT',
            'ADA/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT', 'MATIC/USDT',
            'DOGE/USDT', 'LTC/USDT', 'ATOM/USDT', 'ETC/USDT', 'XLM/USDT',
            'ARB/USDT', 'OP/USDT', 'APT/USDT', 'FIL/USDT', 'NEAR/USDT'
        ]

    async def run_scan(self):
        """Run single scan cycle"""
        try:
            symbols = await self.get_active_symbols()
            all_signals = []
            
            logging.info(f"🔍 Scanning {len(symbols)} symbols...")
            
            for symbol in symbols:
                signals = await self.scan_symbol(symbol)
                all_signals.extend(signals)
                await asyncio.sleep(0.05)  # Small delay to avoid rate limits
            
            if all_signals:
                logging.info(f"📈 Scan complete: {len(all_signals)} fresh signals")
                for signal in all_signals:
                    await self.send_telegram_alert(signal)
            else:
                logging.info("📈 Scan complete: No new signals")
                
        except Exception as e:
            logging.error(f"Scan cycle error: {e}")

    async def send_telegram_alert(self, signal: Dict):
        """Send simple Telegram alert"""
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not token or not chat_id:
            print(f"📱 {signal['symbol']} {signal['side']} | Entry: {signal['entry']:.6f}")
            return
        
        message = f"""
🎯 **ROMEOPT SIGNAL**

Symbol: {signal['symbol']}
Timeframe: {signal['timeframe']}
Side: {signal['side']}
Entry: {signal['entry']:.6f}

SL: {signal['sl']:.6f}
TP1: {signal['tp1']:.6f}  
TP2: {signal['tp2']:.6f}
TP3: {signal['tp3']:.6f}

Score: {signal['score']}/11
Time: {signal['timestamp']}
"""
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json={
                    "chat_id": chat_id, 
                    "text": message, 
                    "parse_mode": "Markdown"
                })
            except Exception as e:
                logging.error(f"Telegram send error: {e}")

    async def start_scanning(self):
        """Start continuous scanning"""
        logging.info("🔄 Starting continuous scanning...")
        
        # Clean up old signals every hour
        last_cleanup = time.time()
        
        while True:
            try:
                await self.run_scan()
                
                # Clean up old signals every hour
                if time.time() - last_cleanup > 3600:
                    self._cleanup_old_signals()
                    last_cleanup = time.time()
                
                await asyncio.sleep(30)  # Scan every 30 seconds
            except Exception as e:
                logging.error(f"Scanner error: {e}")
                await asyncio.sleep(60)

    def _cleanup_old_signals(self):
        """Clean up signals older than 1 hour"""
        current_time = time.time()
        old_signals = []
        
        for signal_id in self.signals_sent:
            # Extract timestamp from signal_id (format: symbol_tf_side_timestamp)
            parts = signal_id.split('_')
            if len(parts) >= 4:
                signal_minute = int(parts[-1])
                if current_time / 60 - signal_minute > 60:  # Older than 1 hour
                    old_signals.append(signal_id)
        
        for old_signal in old_signals:
            self.signals_sent.remove(old_signal)
        
        if old_signals:
            logging.info(f"🧹 Cleaned up {len(old_signals)} old signals")

# ==================== MAIN ====================

async def main():
    """Main function"""
    scanner = LeanRomeScanner()
    await scanner.start_scanning()

if __name__ == "__main__":
    asyncio.run(main())