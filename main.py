"""
MEXC LIQUIDATION SCANNER
Using MEXC Public API - No authentication needed
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import traceback

# ==================== ENVIRONMENT VARIABLES ====================
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
if not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_CHAT_ID environment variable is required")

# Optional variables
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '300'))
MIN_VOLUME = int(os.getenv('MIN_VOLUME', '500000'))
MAX_VOLUME = int(os.getenv('MAX_VOLUME', '50000000'))

# ==================== CONFIGURATION ====================
class Config:
    # Telegram
    TELEGRAM_BOT_TOKEN = TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
    
    # MEXC API Endpoints
    MEXC_BASE_URL = "https://api.mexc.com"
    MEXC_API_V3 = "https://api.mexc.com/api/v3"
    
    # Scanner Settings
    SCAN_INTERVAL_SECONDS = SCAN_INTERVAL
    MAX_CONCURRENT_REQUESTS = 5
    REQUEST_TIMEOUT = 30
    
    # Market Filters
    MIN_24H_VOLUME_USD = MIN_VOLUME
    MAX_24H_VOLUME_USD = MAX_VOLUME
    MIN_PRICE = 0.000001
    
    # Exclude only absolute majors
    EXCLUDE_SYMBOLS = ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA']
    
    # Detection Parameters (MEXC specific)
    LIQUIDITY_DISTANCE = 0.025      # 2.5% from liquidity
    PRICE_STALL_THRESHOLD = 0.015   # 1.5% price movement
    FUNDING_THRESHOLD = 0.0003      # 0.03% funding
    
    # Risk Management
    STOP_LOSS_BUFFER = 1.02        # 2% buffer
    MIN_CONFIDENCE = 5              # 5/10 minimum
    
    # MEXC specific
    PERPETUAL_PREFIX = "_SWAP"  # MEXC perpetual contracts suffix

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== MEXC API CLIENT ====================
class MexcAPI:
    """MEXC Public API Client"""
    
    def __init__(self):
        self.base_url = Config.MEXC_BASE_URL
        self.api_v3 = Config.MEXC_API_V3
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
    
    async def make_request(self, url: str, params: dict = None) -> dict:
        """Make HTTP request to MEXC"""
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"HTTP {response.status} from {url}")
        except Exception as e:
            logger.debug(f"Request error {url}: {str(e)}")
        return {}
    
    async def get_all_perpetual_symbols(self) -> List[str]:
        """Get all perpetual swap symbols from MEXC"""
        logger.info("Fetching perpetual symbols from MEXC...")
        
        # MEXC has different endpoint for perpetuals
        url = f"{self.base_url}/api/v3/defaultSymbols"
        data = await self.make_request(url)
        
        symbols = []
        if isinstance(data, list):
            for item in data:
                symbol = item.get('symbol', '')
                if symbol.endswith(Config.PERPETUAL_PREFIX):
                    # Convert to standard format: BTC_USDT
                    clean_symbol = symbol.replace(Config.PERPETUAL_PREFIX, "").replace("_", "")
                    symbols.append(f"{clean_symbol}USDT")
        
        logger.info(f"Found {len(symbols)} perpetual symbols")
        return symbols
    
    async def get_ticker_24h(self, symbol: str) -> Optional[Dict]:
        """Get 24h ticker data"""
        # Convert symbol format: PEPEUSDT -> PEPE_USDT
        base_symbol = symbol.replace('USDT', '')
        mex_symbol = f"{base_symbol}_USDT"
        
        url = f"{self.api_v3}/ticker/24hr"
        params = {'symbol': mex_symbol}
        
        data = await self.make_request(url, params)
        
        if data and 'symbol' in data:
            return {
                'symbol': symbol,
                'last_price': float(data.get('lastPrice', 0)),
                'volume': float(data.get('volume', 0)),
                'quote_volume': float(data.get('quoteVolume', 0)),
                'high_price': float(data.get('highPrice', 0)),
                'low_price': float(data.get('lowPrice', 0)),
                'price_change': float(data.get('priceChange', 0)),
                'price_change_percent': float(data.get('priceChangePercent', 0))
            }
        return None
    
    async def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> Optional[pd.DataFrame]:
        """Get OHLCV data from MEXC"""
        # Convert symbol format
        base_symbol = symbol.replace('USDT', '')
        mex_symbol = f"{base_symbol}_USDT"
        
        url = f"{self.api_v3}/klines"
        params = {
            'symbol': mex_symbol,
            'interval': interval,
            'limit': limit
        }
        
        data = await self.make_request(url, params)
        
        if isinstance(data, list):
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
            ])
            
            # Convert to proper types
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            df = df.sort_values('open_time').reset_index(drop=True)
            return df
        
        return None
    
    async def get_funding_rate(self, symbol: str) -> Optional[Dict]:
        """Get funding rate for perpetual"""
        try:
            # MEXC uses different endpoint for funding
            base_symbol = symbol.replace('USDT', '')
            
            # Try multiple possible endpoints
            endpoints = [
                f"{self.base_url}/api/v3/fundingRate?symbol={base_symbol}_USDT",
                f"{self.base_url}/api/v3/premiumIndex?symbol={base_symbol}_USDT"
            ]
            
            for endpoint in endpoints:
                data = await self.make_request(endpoint)
                if data and isinstance(data, dict):
                    if 'fundingRate' in data:
                        return {
                            'funding_rate': float(data['fundingRate']),
                            'next_funding_time': data.get('nextFundingTime', 0)
                        }
                    elif 'lastFundingRate' in data:
                        return {
                            'funding_rate': float(data['lastFundingRate']),
                            'next_funding_time': data.get('nextFundingTime', 0)
                        }
            
            # If no funding rate found, return default
            return {'funding_rate': 0.0, 'next_funding_time': 0}
            
        except Exception as e:
            logger.debug(f"Funding rate error for {symbol}: {str(e)}")
            return {'funding_rate': 0.0, 'next_funding_time': 0}
    
    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """Get open interest (approximation from volume)"""
        # For MEXC, we'll use volume as proxy since OI not directly available
        ticker = await self.get_ticker_24h(symbol)
        if ticker:
            return ticker['quote_volume'] / 100  # Rough approximation
        return None

# ==================== DETECTION ENGINE ====================
class LiquidationDetector:
    """Detect liquidation setups"""
    
    @staticmethod
    def find_liquidity_zones(df_1h: pd.DataFrame, current_price: float) -> Tuple[bool, Dict]:
        """Find nearby liquidity zones"""
        if df_1h is None or len(df_1h) < 24:
            return False, {}
        
        # Recent highs and lows (last 24 candles)
        recent = df_1h.tail(24)
        recent_high = recent['high'].max()
        recent_low = recent['low'].min()
        
        # Calculate distances
        high_distance = abs(recent_high - current_price) / current_price
        low_distance = abs(recent_low - current_price) / current_price
        
        # Check if near high
        if high_distance < Config.LIQUIDITY_DISTANCE:
            return True, {
                'zone': 'high',
                'price': recent_high,
                'distance': high_distance,
                'type': 'recent_high'
            }
        
        # Check if near low
        if low_distance < Config.LIQUIDITY_DISTANCE:
            return True, {
                'zone': 'low',
                'price': recent_low,
                'distance': low_distance,
                'type': 'recent_low'
            }
        
        return False, {}
    
    @staticmethod
    def check_trap(df_5m: pd.DataFrame, current_price: float) -> Tuple[bool, Dict]:
        """Check if traders are trapped"""
        if df_5m is None or len(df_5m) < 10:
            return False, {}
        
        # Calculate price movement over last 30 minutes
        lookback = min(6, len(df_5m))
        price_start = df_5m['close'].iloc[-lookback]
        price_end = df_5m['close'].iloc[-1]
        price_change = (price_end - price_start) / price_start
        
        # Get recent range
        recent_high = df_5m['high'].tail(10).max()
        recent_low = df_5m['low'].tail(10).min()
        
        # Calculate position in range
        range_size = recent_high - recent_low
        if range_size > 0:
            position = (current_price - recent_low) / range_size
        else:
            position = 0.5
        
        # Check for trap conditions
        if abs(price_change) < Config.PRICE_STALL_THRESHOLD:
            if position > 0.7:  # Near top
                return True, {
                    'side': 'longs',
                    'position': 'high',
                    'price_change': price_change
                }
            elif position < 0.3:  # Near bottom
                return True, {
                    'side': 'shorts',
                    'position': 'low',
                    'price_change': price_change
                }
        
        return False, {}
    
    @staticmethod
    def check_funding_pressure(funding_rate: float) -> Tuple[bool, Dict]:
        """Check funding pressure"""
        if abs(funding_rate) < Config.FUNDING_THRESHOLD:
            return False, {}
        
        if funding_rate > 0:
            return True, {
                'side': 'longs',
                'rate': funding_rate,
                'trend': 'positive'
            }
        else:
            return True, {
                'side': 'shorts',
                'rate': funding_rate,
                'trend': 'negative'
            }

# ==================== SIGNAL GENERATOR ====================
class SignalGenerator:
    """Generate trade signals"""
    
    @staticmethod
    def create_signal(symbol: str, ticker: Dict, direction: str,
                     liquidity: Dict, trap: Dict, funding: Dict) -> Dict:
        """Create complete trade signal"""
        current_price = ticker['last_price']
        
        # Calculate parameters based on direction
        if direction == 'SHORT':
            entry_max = min(current_price * 1.008, liquidity['price'] * 0.995)
            entry_min = entry_max * 0.99
            stop_loss = liquidity['price'] * Config.STOP_LOSS_BUFFER
            tp1 = current_price * 0.98
            tp2 = current_price * 0.96
            tp3 = current_price * 0.92
        else:  # LONG
            entry_min = max(current_price * 0.992, liquidity['price'] * 1.005)
            entry_max = entry_min * 1.008
            stop_loss = liquidity['price'] / Config.STOP_LOSS_BUFFER
            tp1 = current_price * 1.02
            tp2 = current_price * 1.04
            tp3 = current_price * 1.08
        
        # Calculate confidence
        confidence = SignalGenerator.calculate_confidence(
            liquidity['distance'],
            abs(funding['rate']),
            trap['price_change']
        )
        
        # Create reason
        reason = (
            f"Price near {liquidity['type']} at ${liquidity['price']:.6f} | "
            f"{trap['side'].capitalize()} trapped (price ∆{trap['price_change']:.2%}) | "
            f"{funding['side'].capitalize()} paying {funding['rate']:.5%} funding"
        )
        
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
            'confidence': confidence,
            'reason': reason,
            'timestamp': datetime.now()
        }
    
    @staticmethod
    def calculate_confidence(liquidity_distance: float, funding_rate: float, 
                            price_change: float) -> int:
        """Calculate confidence score 1-10"""
        score = 5  # Base
        
        # Liquidity proximity
        if liquidity_distance < 0.015:
            score += 2
        elif liquidity_distance < 0.025:
            score += 1
        
        # Funding magnitude
        if funding_rate > 0.0008:
            score += 2
        elif funding_rate > 0.0004:
            score += 1
        
        # Price stall
        if abs(price_change) < 0.01:
            score += 1
        
        return min(max(score, 1), 10)

# ==================== TELEGRAM BOT ====================
class TelegramBot:
    """Send alerts to Telegram"""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
    
    async def send_alert(self, signal: Dict):
        """Send trade alert"""
        try:
            message = self.format_message(signal)
            
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
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
    
    def format_message(self, signal: Dict) -> str:
        """Format signal for Telegram"""
        emoji = "🔴" if signal['direction'] == 'SHORT' else "🟢"
        
        return f"""
{emoji} <b>LIQUIDATION SIGNAL - MEXC</b> {emoji}

<b>Symbol:</b> {signal['symbol']}
<b>Direction:</b> {signal['direction']}
<b>Confidence:</b> {signal['confidence']}/10

<b>Current Price:</b> ${signal['current_price']:.8f}
<b>Liquidity Zone:</b> ${signal['liquidity_price']:.8f} ({signal['liquidity_type']})

<b>Entry Zone:</b>
${signal['entry_min']:.8f} - ${signal['entry_max']:.8f}

<b>Stop Loss:</b> ${signal['stop_loss']:.8f}
<b>Take Profit:</b>
TP1: ${signal['tp1']:.8f}
TP2: ${signal['tp2']:.8f}
TP3: ${signal['tp3']:.8f}

<b>Setup Logic:</b>
{signal['reason']}

<b>Time:</b> {signal['timestamp'].strftime('%H:%M:%S UTC')}

#{signal['direction']} #{signal['symbol'].replace('USDT', '')} #MEXC
        """

# ==================== MAIN SCANNER ====================
class MexcLiquidationScanner:
    """Main scanner for MEXC"""
    
    def __init__(self):
        self.telegram = TelegramBot(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.detector = LiquidationDetector()
        self.signal_gen = SignalGenerator()
        self.recent_signals = {}
        self.scan_count = 0
    
    async def scan_symbol(self, api: MexcAPI, symbol: str) -> Optional[Dict]:
        """Scan a single symbol"""
        try:
            logger.debug(f"Scanning {symbol}")
            
            # Get ticker data
            ticker = await api.get_ticker_24h(symbol)
            if not ticker:
                return None
            
            # Volume filter
            volume = ticker['quote_volume']
            if volume < Config.MIN_24H_VOLUME_USD or volume > Config.MAX_24H_VOLUME_USD:
                return None
            
            # Exclude majors
            base_symbol = symbol.replace('USDT', '').upper()
            if base_symbol in Config.EXCLUDE_SYMBOLS:
                return None
            
            current_price = ticker['last_price']
            if current_price < Config.MIN_PRICE:
                return None
            
            logger.debug(f"{symbol}: ${current_price:.8f}, Volume: ${volume:,.0f}")
            
            # Get price data
            df_1h = await api.get_klines(symbol, '1h', 50)
            df_5m = await api.get_klines(symbol, '5m', 30)
            
            if df_1h is None or df_5m is None:
                return None
            
            if len(df_1h) < 24 or len(df_5m) < 10:
                return None
            
            # Get funding rate
            funding_data = await api.get_funding_rate(symbol)
            funding_rate = funding_data['funding_rate'] if funding_data else 0.0
            
            # Check three conditions
            has_liquidity, liquidity = self.detector.find_liquidity_zones(df_1h, current_price)
            has_trap, trap = self.detector.check_trap(df_5m, current_price)
            has_funding, funding = self.detector.check_funding_pressure(funding_rate)
            
            # Log conditions for debugging
            logger.debug(f"{symbol}: Liquidity={has_liquidity}, Trap={has_trap}, Funding={has_funding}")
            
            # All three must be present
            if not (has_liquidity and has_trap and has_funding):
                return None
            
            # Determine direction
            direction = None
            if liquidity['zone'] == 'high' and trap['side'] == 'longs' and funding['side'] == 'longs':
                direction = 'SHORT'
            elif liquidity['zone'] == 'low' and trap['side'] == 'shorts' and funding['side'] == 'shorts':
                direction = 'LONG'
            
            if not direction:
                return None
            
            # Create signal
            signal = self.signal_gen.create_signal(symbol, ticker, direction, liquidity, trap, funding)
            
            logger.info(f"✅ MEXC SIGNAL: {symbol} {direction} (Confidence: {signal['confidence']}/10)")
            return signal
            
        except Exception as e:
            logger.error(f"Error scanning {symbol}: {str(e)}")
            return None
    
    async def run(self):
        """Main scanning loop"""
        async with MexcAPI() as api:
            while True:
                try:
                    self.scan_count += 1
                    start_time = datetime.now()
                    
                    logger.info(f"\n{'='*60}")
                    logger.info(f"MEXC SCAN #{self.scan_count} - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"{'='*60}")
                    
                    # Get symbols
                    symbols = await api.get_all_perpetual_symbols()
                    if not symbols:
                        logger.warning("No symbols found from MEXC")
                        await asyncio.sleep(60)
                        continue
                    
                    logger.info(f"Found {len(symbols)} symbols, filtering...")
                    
                    # Filter and scan
                    signals_found = 0
                    batch_size = Config.MAX_CONCURRENT_REQUESTS
                    
                    for i in range(0, len(symbols), batch_size):
                        batch = symbols[i:i+batch_size]
                        
                        # Create tasks for batch
                        tasks = []
                        for symbol in batch:
                            # Check cooldown
                            if symbol in self.recent_signals:
                                last_time = self.recent_signals[symbol]
                                if (datetime.now() - last_time).seconds < 7200:  # 2 hours
                                    continue
                            tasks.append(self.scan_symbol(api, symbol))
                        
                        # Execute batch
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
                        await asyncio.sleep(1)
                    
                    # Log scan results
                    scan_duration = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Scan completed in {scan_duration:.1f}s")
                    logger.info(f"Signals found: {signals_found}")
                    logger.info(f"Next scan in {Config.SCAN_INTERVAL_SECONDS} seconds...")
                    
                    # Wait for next scan
                    await asyncio.sleep(Config.SCAN_INTERVAL_SECONDS)
                    
                except KeyboardInterrupt:
                    logger.info("Scanner stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Scan error: {str(e)}")
                    traceback.print_exc()
                    await asyncio.sleep(60)

# ==================== MAIN ====================
async def main():
    """Main entry point"""
    logger.info("🚀 MEXC LIQUIDATION SCANNER")
    logger.info("=" * 50)
    logger.info("Exchange: MEXC")
    logger.info(f"Volume Range: ${Config.MIN_24H_VOLUME_USD:,} - ${Config.MAX_24H_VOLUME_USD:,}")
    logger.info(f"Scan Interval: {Config.SCAN_INTERVAL_SECONDS}s")
    logger.info(f"Min Confidence: {Config.MIN_CONFIDENCE}/10")
    logger.info("=" * 50)
    logger.info("Starting scanner...")
    
    scanner = MexcLiquidationScanner()
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())