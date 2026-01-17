"""
FIXED BYBIT LIQUIDATION SCANNER
With working API endpoints and better error handling
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import traceback
import warnings
warnings.filterwarnings('ignore')

# ==================== ENVIRONMENT VARIABLES ====================
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
if not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_CHAT_ID environment variable is required")

# ==================== CONFIGURATION ====================
class Config:
    # Telegram Configuration
    TELEGRAM_BOT_TOKEN = TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
    
    # Bybit Public API - FIXED ENDPOINTS
    BYBIT_BASE_URL = "https://api.bybit.com"
    
    # Scanner Settings
    SCAN_INTERVAL_SECONDS = int(os.getenv('SCAN_INTERVAL', '300'))
    MAX_CONCURRENT_REQUESTS = 3  # Reduced to avoid rate limiting
    REQUEST_TIMEOUT = 30
    
    # Market Filters
    MIN_24H_VOLUME_USD = int(os.getenv('MIN_VOLUME', '100000'))
    MAX_24H_VOLUME_USD = int(os.getenv('MAX_VOLUME', '50000000'))
    MIN_PRICE = 0.000001
    
    # Minimal exclusion list
    EXCLUDE_SYMBOLS = ['BTC', 'ETH', 'BNB', 'SOL']
    
    # Detection Parameters
    LIQUIDITY_PROXIMITY_THRESHOLD = 0.02  # 2%
    OI_INCREASE_MINIMUM = 0.03  # 3%
    PRICE_STALL_THRESHOLD = 0.01  # 1%
    FUNDING_SIGNIFICANT = 0.0003  # 0.03%
    
    # Risk Management
    STOP_LOSS_BUFFER = 1.01
    MIN_CONFIDENCE = int(os.getenv('MIN_CONFIDENCE', '5'))

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scanner_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== FIXED BYBIT API ====================
class BybitAPI:
    """Fixed Bybit API client with working endpoints"""
    
    def __init__(self):
        self.base_url = Config.BYBIT_BASE_URL
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT),
            headers=self.headers
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _make_request(self, url: str, params: Dict = None) -> Dict:
        """Make HTTP request with error handling"""
        if params is None:
            params = {}
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.warning(f"HTTP {response.status} from {url}")
                    return {}
        except Exception as e:
            logger.error(f"Request error {url}: {str(e)}")
            return {}
    
    async def get_all_symbols(self) -> List[str]:
        """Get all USDT perpetual symbols"""
        url = f"{self.base_url}/v5/market/tickers"
        params = {'category': 'linear'}
        
        data = await self._make_request(url, params)
        
        symbols = []
        if data and data.get('retCode') == 0:
            result = data.get('result', {})
            if 'list' in result:
                for ticker in result['list']:
                    symbol = ticker.get('symbol', '')
                    if symbol.endswith('USDT'):
                        symbols.append(symbol)
        
        logger.info(f"Retrieved {len(symbols)} symbols")
        return symbols
    
    async def get_ticker(self, symbol: str) -> Optional[Dict]:
        """Get ticker data for a symbol"""
        url = f"{self.base_url}/v5/market/tickers"
        params = {'category': 'linear', 'symbol': symbol}
        
        data = await self._make_request(url, params)
        
        if data and data.get('retCode') == 0:
            result = data.get('result', {})
            if 'list' in result and len(result['list']) > 0:
                ticker = result['list'][0]
                return {
                    'symbol': symbol,
                    'last_price': float(ticker.get('lastPrice', 0)),
                    'volume_24h': float(ticker.get('volume24h', 0)),
                    'turnover_24h': float(ticker.get('turnover24h', 0)),
                    'high_24h': float(ticker.get('highPrice24h', 0)),
                    'low_24h': float(ticker.get('lowPrice24h', 0)),
                    'funding_rate': float(ticker.get('fundingRate', 0)),
                    'open_interest': float(ticker.get('openInterest', 0))
                }
        return None
    
    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """Get OHLCV data"""
        url = f"{self.base_url}/v5/market/kline"
        
        # Map interval
        interval_map = {
            '1m': '1', '5m': '5', '15m': '15', '1h': '60',
            '4h': '240', '1d': 'D'
        }
        
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': interval_map.get(interval, '5'),
            'limit': limit
        }
        
        data = await self._make_request(url, params)
        
        if data and data.get('retCode') == 0:
            result = data.get('result', {})
            if 'list' in result:
                df = pd.DataFrame(result['list'], columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
                ])
                
                # Convert types
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float).astype(int), unit='ms')
                for col in ['open', 'high', 'low', 'close', 'volume', 'turnover']:
                    df[col] = df[col].astype(float)
                
                df = df.sort_values('timestamp').reset_index(drop=True)
                return df
        
        return None
    
    async def get_funding_rate(self, symbol: str, limit: int = 10) -> Optional[pd.DataFrame]:
        """Get funding rate history"""
        url = f"{self.base_url}/v5/market/funding/history"
        params = {
            'category': 'linear',
            'symbol': symbol,
            'limit': limit
        }
        
        data = await self._make_request(url, params)
        
        if data and data.get('retCode') == 0:
            result = data.get('result', {})
            if 'list' in result:
                df = pd.DataFrame(result['list'])
                if 'fundingRate' in df.columns and 'fundingRateTimestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['fundingRateTimestamp'].astype(float).astype(int), unit='ms')
                    df['fundingRate'] = df['fundingRate'].astype(float)
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    return df
        
        return None

# ==================== SIMPLIFIED LIQUIDATION DETECTION ====================
class LiquidationDetector:
    """Simplified liquidation detection"""
    
    @staticmethod
    async def analyze_symbol(api: BybitAPI, symbol: str) -> Optional[Dict]:
        """Analyze a single symbol for liquidation setup"""
        try:
            logger.debug(f"Analyzing {symbol}")
            
            # Get basic data
            ticker = await api.get_ticker(symbol)
            if not ticker:
                return None
            
            # Volume filter
            volume = ticker['turnover_24h']
            if volume < Config.MIN_24H_VOLUME_USD or volume > Config.MAX_24H_VOLUME_USD:
                return None
            
            # Exclude symbols
            base = symbol.replace('USDT', '').upper()
            if base in Config.EXCLUDE_SYMBOLS:
                return None
            
            current_price = ticker['last_price']
            if current_price < Config.MIN_PRICE:
                return None
            
            logger.debug(f"{symbol}: Price ${current_price:.6f}, Volume ${volume:,.0f}")
            
            # Get additional data
            df_1h = await api.get_klines(symbol, '1h', 50)
            df_5m = await api.get_klines(symbol, '5m', 30)
            funding_df = await api.get_funding_rate(symbol, 8)
            
            if df_1h is None or df_5m is None or funding_df is None:
                return None
            
            if len(df_1h) < 20 or len(df_5m) < 10 or len(funding_df) < 5:
                return None
            
            # 1. Check Liquidity (WHERE)
            liquidity_result = LiquidationDetector._check_liquidity(df_1h, current_price)
            if not liquidity_result['has_liquidity']:
                return None
            
            # 2. Check Trap (WHO IS TRAPPED)
            trap_result = LiquidationDetector._check_trap(df_5m, current_price)
            if not trap_result['has_trap']:
                return None
            
            # 3. Check Funding (WHO IS BLEEDING)
            funding_result = LiquidationDetector._check_funding(funding_df)
            if not funding_result['has_pressure']:
                return None
            
            # 4. Determine Direction
            direction = LiquidationDetector._determine_direction(
                liquidity_result, trap_result, funding_result
            )
            
            if not direction:
                return None
            
            # 5. Generate Signal
            signal = LiquidationDetector._create_signal(
                symbol, current_price, direction,
                liquidity_result, trap_result, funding_result
            )
            
            logger.info(f"✅ SIGNAL: {symbol} {direction}")
            return signal
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {str(e)}")
            return None
    
    @staticmethod
    def _check_liquidity(df_1h: pd.DataFrame, current_price: float) -> Dict:
        """Check if price is near liquidity zone"""
        if len(df_1h) < 24:
            return {'has_liquidity': False, 'zone': None, 'distance': None}
        
        # Get recent high and low
        recent = df_1h.tail(24)
        recent_high = recent['high'].max()
        recent_low = recent['low'].min()
        
        # Check distance to recent high
        high_distance = abs(recent_high - current_price) / current_price
        low_distance = abs(recent_low - current_price) / current_price
        
        if high_distance < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
            return {
                'has_liquidity': True,
                'zone': 'high',
                'price': recent_high,
                'distance': high_distance,
                'type': 'recent_high'
            }
        
        if low_distance < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
            return {
                'has_liquidity': True,
                'zone': 'low',
                'price': recent_low,
                'distance': low_distance,
                'type': 'recent_low'
            }
        
        return {'has_liquidity': False, 'zone': None, 'distance': None}
    
    @staticmethod
    def _check_trap(df_5m: pd.DataFrame, current_price: float) -> Dict:
        """Check if traders are trapped"""
        if len(df_5m) < 10:
            return {'has_trap': False, 'side': None}
        
        # Calculate price movement
        price_start = df_5m['close'].iloc[0]
        price_end = df_5m['close'].iloc[-1]
        price_change = (price_end - price_start) / price_start
        
        # Calculate position in recent range
        recent_high = df_5m['high'].max()
        recent_low = df_5m['low'].min()
        
        # If price is barely moving but near extremes, traders are trapped
        if abs(price_change) < Config.PRICE_STALL_THRESHOLD:
            near_high = (recent_high - current_price) / current_price < 0.01
            near_low = (current_price - recent_low) / current_price < 0.01
            
            if near_high:
                return {'has_trap': True, 'side': 'longs', 'position': 'high'}
            elif near_low:
                return {'has_trap': True, 'side': 'shorts', 'position': 'low'}
        
        return {'has_trap': False, 'side': None}
    
    @staticmethod
    def _check_funding(funding_df: pd.DataFrame) -> Dict:
        """Check funding pressure"""
        if len(funding_df) < 5:
            return {'has_pressure': False, 'side': None}
        
        current_funding = funding_df['fundingRate'].iloc[-1]
        
        if abs(current_funding) < Config.FUNDING_SIGNIFICANT:
            return {'has_pressure': False, 'side': None}
        
        if current_funding > 0:
            return {'has_pressure': True, 'side': 'longs', 'rate': current_funding}
        else:
            return {'has_pressure': True, 'side': 'shorts', 'rate': current_funding}
    
    @staticmethod
    def _determine_direction(liquidity: Dict, trap: Dict, funding: Dict) -> Optional[str]:
        """Determine trade direction"""
        # SHORT: near highs, longs trapped, longs bleeding
        if (liquidity['zone'] == 'high' and 
            trap['side'] == 'longs' and 
            funding['side'] == 'longs'):
            return 'SHORT'
        
        # LONG: near lows, shorts trapped, shorts bleeding
        if (liquidity['zone'] == 'low' and 
            trap['side'] == 'shorts' and 
            funding['side'] == 'shorts'):
            return 'LONG'
        
        return None
    
    @staticmethod
    def _create_signal(symbol: str, current_price: float, direction: str,
                      liquidity: Dict, trap: Dict, funding: Dict) -> Dict:
        """Create trade signal"""
        # Entry zone
        if direction == 'SHORT':
            entry_max = min(current_price * 1.003, liquidity['price'] * 0.998)
            entry_min = entry_max * 0.995
            stop_loss = liquidity['price'] * Config.STOP_LOSS_BUFFER
            tp1 = current_price * 0.99
            tp2 = current_price * 0.98
            tp3 = current_price * 0.96
        else:  # LONG
            entry_min = max(current_price * 0.997, liquidity['price'] * 1.002)
            entry_max = entry_min * 1.003
            stop_loss = liquidity['price'] / Config.STOP_LOSS_BUFFER
            tp1 = current_price * 1.01
            tp2 = current_price * 1.02
            tp3 = current_price * 1.04
        
        # Confidence score
        confidence = 5  # Base
        if abs(funding['rate']) > 0.0005:
            confidence += 1
        if liquidity['distance'] < 0.01:
            confidence += 1
        
        return {
            'symbol': symbol,
            'direction': direction,
            'current_price': current_price,
            'entry_min': entry_min,
            'entry_max': entry_max,
            'stop_loss': stop_loss,
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'liquidity_price': liquidity['price'],
            'liquidity_type': liquidity['type'],
            'trapped_side': trap['side'],
            'bleeding_side': funding['side'],
            'funding_rate': funding['rate'],
            'confidence': min(max(confidence, 1), 10),
            'reason': f"Near {liquidity['type']} | {trap['side']} trapped | {funding['side']} bleeding ({funding['rate']:.5%})",
            'timestamp': datetime.now()
        }

# ==================== TELEGRAM ALERTER ====================
class TelegramBot:
    """Simple Telegram bot"""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
    
    async def send_alert(self, signal: Dict):
        """Send trade alert"""
        try:
            message = self._format_message(signal)
            
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"📨 Telegram alert sent for {signal['symbol']}")
                    else:
                        error = await response.text()
                        logger.error(f"Telegram error: {error}")
                        
        except Exception as e:
            logger.error(f"Telegram send error: {str(e)}")
    
    def _format_message(self, signal: Dict) -> str:
        """Format signal message"""
        emoji = "🔴" if signal['direction'] == 'SHORT' else "🟢"
        
        return f"""
{emoji} <b>LIQUIDATION SIGNAL</b> {emoji}

<b>Symbol:</b> {signal['symbol']}
<b>Direction:</b> {signal['direction']}
<b>Confidence:</b> {signal['confidence']}/10

<b>Price:</b> ${signal['current_price']:.8f}
<b>Liquidity:</b> ${signal['liquidity_price']:.8f} ({signal['liquidity_type']})

<b>Entry Zone:</b>
${signal['entry_min']:.8f} - ${signal['entry_max']:.8f}

<b>Stop Loss:</b> ${signal['stop_loss']:.8f}
<b>Take Profit:</b>
TP1: ${signal['tp1']:.8f}
TP2: ${signal['tp2']:.8f}
TP3: ${signal['tp3']:.8f}

<b>Setup:</b>
{signal['reason']}

<b>Time:</b> {signal['timestamp'].strftime('%H:%M:%S UTC')}

#{signal['direction']} #{signal['symbol'].replace('USDT', '')}
        """

# ==================== MAIN SCANNER ====================
class SimpleLiquidationScanner:
    """Simple but effective liquidation scanner"""
    
    def __init__(self):
        self.telegram = TelegramBot(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.detector = LiquidationDetector()
        self.recent_signals = {}
        self.scan_count = 0
    
    async def run(self):
        """Main scanner loop"""
        async with BybitAPI() as api:
            while True:
                try:
                    self.scan_count += 1
                    logger.info(f"\n{'='*60}")
                    logger.info(f"SCAN #{self.scan_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"{'='*60}")
                    
                    # Get symbols
                    symbols = await api.get_all_symbols()
                    if not symbols:
                        logger.warning("No symbols retrieved, waiting 60 seconds...")
                        await asyncio.sleep(60)
                        continue
                    
                    logger.info(f"Found {len(symbols)} symbols, filtering...")
                    
                    # Filter for low-cap
                    filtered_symbols = []
                    for symbol in symbols:
                        base = symbol.replace('USDT', '').upper()
                        if base not in Config.EXCLUDE_SYMBOLS:
                            filtered_symbols.append(symbol)
                    
                    logger.info(f"Scanning {len(filtered_symbols)} low-cap symbols")
                    
                    # Scan symbols
                    signals_found = 0
                    batch_size = min(Config.MAX_CONCURRENT_REQUESTS, len(filtered_symbols))
                    
                    for i in range(0, len(filtered_symbols), batch_size):
                        batch = filtered_symbols[i:i+batch_size]
                        
                        # Scan batch
                        tasks = []
                        for symbol in batch:
                            # Check cooldown
                            if symbol in self.recent_signals:
                                last_time = self.recent_signals[symbol]
                                if (datetime.now() - last_time).seconds < 3600:
                                    continue
                            
                            tasks.append(self.detector.analyze_symbol(api, symbol))
                        
                        if tasks:
                            results = await asyncio.gather(*tasks)
                            
                            # Process signals
                            for signal in results:
                                if signal and signal['confidence'] >= Config.MIN_CONFIDENCE:
                                    # Send alert
                                    await self.telegram.send_alert(signal)
                                    signals_found += 1
                                    
                                    # Update cooldown
                                    self.recent_signals[signal['symbol']] = datetime.now()
                        
                        # Rate limiting
                        await asyncio.sleep(2)
                    
                    logger.info(f"Scan complete. Signals found: {signals_found}")
                    logger.info(f"Next scan in {Config.SCAN_INTERVAL_SECONDS} seconds...")
                    
                    # Wait for next scan
                    await asyncio.sleep(Config.SCAN_INTERVAL_SECONDS)
                    
                except KeyboardInterrupt:
                    logger.info("Scanner stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Scan error: {str(e)}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(60)

# ==================== MAIN ====================
async def main():
    """Main function"""
    logger.info("🚀 SIMPLE LIQUIDATION SCANNER")
    logger.info("=" * 50)
    logger.info(f"Telegram: Configured")
    logger.info(f"Volume: ${Config.MIN_24H_VOLUME_USD:,} - ${Config.MAX_24H_VOLUME_USD:,}")
    logger.info(f"Interval: {Config.SCAN_INTERVAL_SECONDS}s")
    logger.info(f"Min Confidence: {Config.MIN_CONFIDENCE}/10")
    logger.info("=" * 50)
    logger.info("Waiting for liquidation setups...")
    
    scanner = SimpleLiquidationScanner()
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())