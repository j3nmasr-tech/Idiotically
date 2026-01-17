"""
OPTIMIZED LIQUIDATION SCANNER - FIXED VERSION
With detailed logging and improved detection
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import logging
from dataclasses import dataclass
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
    
    # Bybit Public API
    BYBIT_BASE_URL = "https://api.bybit.com"
    
    # Scanner Settings
    SCAN_INTERVAL_SECONDS = int(os.getenv('SCAN_INTERVAL', '300'))  # 5 minutes default
    MAX_CONCURRENT_REQUESTS = 5  # Reduced for stability
    REQUEST_TIMEOUT = 10
    
    # Market Filters - LOOSENED for testing
    MIN_24H_VOLUME_USD = int(os.getenv('MIN_VOLUME', '500000'))    # $500K minimum
    MAX_24H_VOLUME_USD = int(os.getenv('MAX_VOLUME', '50000000'))  # $50M maximum
    MIN_PRICE = 0.000001
    
    # Exclude only major majors
    EXCLUDE_SYMBOLS = [
        'BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'AVAX', 'DOT',
        'DOGE', 'SHIB', 'TRX', 'LINK'  # Reduced exclusion list
    ]
    
    # Liquidity Detection - LOOSENED
    SWING_LOOKBACK = 15
    EQUAL_HIGH_LOW_TOLERANCE = 0.005  # 0.5%
    LIQUIDITY_PROXIMITY_THRESHOLD = 0.025  # 2.5% from liquidity
    
    # Trap Detection - LOOSENED
    OI_INCREASE_MINIMUM = 0.03  # 3% minimum
    PRICE_STALL_THRESHOLD = 0.01  # 1% price move allowed
    
    # Funding Pressure
    FUNDING_SIGNIFICANT = 0.0002  # 0.02%
    FUNDING_TREND_WINDOW = 6
    
    # Risk Management
    STOP_LOSS_BUFFER = 1.01  # 1% buffer
    TP1_ALLOCATION = 0.5
    TP2_ALLOCATION = 0.3
    TP3_ALLOCATION = 0.2
    
    # Signal Confidence
    MIN_CONFIDENCE = int(os.getenv('MIN_CONFIDENCE', '5'))  # Lowered for testing
    
    # Debug Mode
    DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    level=logging.DEBUG if Config.DEBUG_MODE else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scanner_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== DATA STRUCTURES ====================
@dataclass
class LiquidityZone:
    price: float
    zone_type: str
    timeframe: str
    strength: int
    timestamp: datetime

@dataclass
class ScanResult:
    symbol: str
    has_liquidity: bool
    liquidity_info: str
    has_trap: bool
    trap_info: str
    has_funding: bool
    funding_info: str
    direction: Optional[str]
    confidence: int
    signal: Optional[Dict]

# ==================== BYBIT PUBLIC API ====================
class BybitPublicAPI:
    def __init__(self):
        self.base_url = Config.BYBIT_BASE_URL
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_data(self, endpoint: str, params: Dict = None) -> Dict:
        """Fetch data with retry logic"""
        if params is None:
            params = {}
        
        url = f"{self.base_url}{endpoint}"
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('retCode') == 0:
                            return data.get('result', {})
                        else:
                            logger.warning(f"API error {endpoint}: {data.get('retMsg')}")
                    else:
                        logger.warning(f"HTTP {response.status} on {endpoint}")
                    
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                        
            except asyncio.TimeoutError:
                logger.warning(f"Timeout on {endpoint}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Error on {endpoint}: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
        
        return {}
    
    async def get_all_perpetual_symbols(self) -> List[str]:
        """Get all USDT perpetual symbols"""
        logger.debug("Fetching all perpetual symbols...")
        data = await self.fetch_data('/v5/market/tickers', {'category': 'linear'})
        
        symbols = []
        if data and 'list' in data:
            for ticker in data['list']:
                symbol = ticker.get('symbol', '')
                if symbol.endswith('USDT'):
                    symbols.append(symbol)
        
        logger.info(f"Retrieved {len(symbols)} perpetual symbols")
        return symbols
    
    async def get_ticker_info(self, symbol: str) -> Optional[Dict]:
        """Get ticker info for a symbol"""
        data = await self.fetch_data('/v5/market/tickers', {
            'category': 'linear',
            'symbol': symbol
        })
        
        if data and 'list' in data and len(data['list']) > 0:
            ticker = data['list'][0]
            return {
                'symbol': symbol,
                'last_price': float(ticker.get('lastPrice', 0)),
                'volume_24h': float(ticker.get('volume24h', 0)),
                'turnover_24h': float(ticker.get('turnover24h', 0)),
                'high_24h': float(ticker.get('highPrice24h', 0)),
                'low_24h': float(ticker.get('lowPrice24h', 0)),
                'funding_rate': float(ticker.get('fundingRate', 0)),
                'open_interest': float(ticker.get('openInterest', 0)),
                'price_change_24h': float(ticker.get('price24hPcnt', 0)),
                'mark_price': float(ticker.get('markPrice', 0))
            }
        return None
    
    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
        """Get OHLCV data"""
        interval_map = {
            '1m': '1', '3m': '3', '5m': '5', '15m': '15',
            '30m': '30', '1h': '60', '2h': '120', '4h': '240',
            '1d': 'D', '1w': 'W', '1M': 'M'
        }
        
        data = await self.fetch_data('/v5/market/kline', {
            'category': 'linear',
            'symbol': symbol,
            'interval': interval_map.get(interval, interval),
            'limit': limit
        })
        
        if data and 'list' in data:
            df = pd.DataFrame(data['list'], columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])
            
            # Convert types
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float).astype(int), unit='ms')
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            for col in numeric_cols:
                df[col] = df[col].astype(float)
            
            df = df.sort_values('timestamp').reset_index(drop=True)
            return df
        
        return pd.DataFrame()
    
    async def get_open_interest(self, symbol: str, interval: str = '5m', limit: int = 30) -> pd.DataFrame:
        """Get open interest history"""
        interval_map = {'5m': '5min', '15m': '15min', '1h': '1h'}
        
        data = await self.fetch_data('/v5/market/open-interest', {
            'category': 'linear',
            'symbol': symbol,
            'intervalTime': interval_map.get(interval, interval),
            'limit': limit
        })
        
        if data and 'list' in data:
            df = pd.DataFrame(data['list'])
            if 'timestamp' in df.columns and 'openInterest' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float).astype(int), unit='ms')
                df['openInterest'] = df['openInterest'].astype(float)
                df = df.sort_values('timestamp').reset_index(drop=True)
                return df
        
        return pd.DataFrame()
    
    async def get_funding_rate_history(self, symbol: str, limit: int = 12) -> pd.DataFrame:
        """Get funding rate history"""
        data = await self.fetch_data('/v5/market/funding/history', {
            'category': 'linear',
            'symbol': symbol,
            'limit': limit
        })
        
        if data and 'list' in data:
            df = pd.DataFrame(data['list'])
            if 'fundingRateTimestamp' in df.columns and 'fundingRate' in df.columns:
                df['timestamp'] = pd.to_datetime(df['fundingRateTimestamp'].astype(float).astype(int), unit='ms')
                df['fundingRate'] = df['fundingRate'].astype(float)
                df = df.sort_values('timestamp').reset_index(drop=True)
                return df
        
        return pd.DataFrame()

# ==================== LIQUIDITY DETECTION ====================
class LiquidityDetector:
    """Detect liquidity zones"""
    
    @staticmethod
    def find_liquidity_zones(price_data: Dict, current_price: float) -> Tuple[bool, str, Optional[Dict]]:
        """
        Find nearby liquidity zones
        Returns: (has_liquidity, info, zone_data)
        """
        df_1h = price_data.get('1h')
        df_15m = price_data.get('15m')
        
        if df_1h is None or len(df_1h) < 20 or df_15m is None or len(df_15m) < 20:
            return False, "Insufficient price data", None
        
        zones = []
        
        # 1. Recent highs and lows (last 24 hours)
        if len(df_1h) >= 24:
            recent_1h = df_1h.tail(24)
            recent_high = recent_1h['high'].max()
            recent_low = recent_1h['low'].min()
            
            high_distance = abs(recent_high - current_price) / current_price
            low_distance = abs(recent_low - current_price) / current_price
            
            if high_distance < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
                zones.append({
                    'price': recent_high,
                    'type': 'recent_high',
                    'distance_pct': high_distance * 100,
                    'strength': 2
                })
                logger.debug(f"Near recent high: ${recent_high:.6f} ({high_distance:.2%} away)")
            
            if low_distance < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
                zones.append({
                    'price': recent_low,
                    'type': 'recent_low',
                    'distance_pct': low_distance * 100,
                    'strength': 2
                })
                logger.debug(f"Near recent low: ${recent_low:.6f} ({low_distance:.2%} away)")
        
        # 2. Range highs and lows (last 4 hours on 15m)
        if len(df_15m) >= 16:
            recent_15m = df_15m.tail(16)
            range_high = recent_15m['high'].max()
            range_low = recent_15m['low'].min()
            
            high_distance = abs(range_high - current_price) / current_price
            low_distance = abs(range_low - current_price) / current_price
            
            if high_distance < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
                zones.append({
                    'price': range_high,
                    'type': 'range_high',
                    'distance_pct': high_distance * 100,
                    'strength': 1
                })
            
            if low_distance < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
                zones.append({
                    'price': range_low,
                    'type': 'range_low',
                    'distance_pct': low_distance * 100,
                    'strength': 1
                })
        
        # 3. Round numbers
        round_zones = LiquidityDetector._find_round_numbers(current_price)
        zones.extend(round_zones)
        
        if zones:
            # Sort by distance
            zones.sort(key=lambda x: x['distance_pct'])
            nearest = zones[0]
            
            info = f"Near {nearest['type']} at ${nearest['price']:.6f} ({nearest['distance_pct']:.2f}% away)"
            return True, info, nearest
        
        return False, f"No liquidity within {Config.LIQUIDITY_PROXIMITY_THRESHOLD*100:.1f}%", None
    
    @staticmethod
    def _find_round_numbers(price: float) -> List[Dict]:
        """Find nearby round psychological levels"""
        zones = []
        
        if price <= 0:
            return zones
        
        # Define round levels based on price magnitude
        if price >= 1:
            levels = [0.5, 1, 5, 10, 50, 100]
        elif price >= 0.1:
            levels = [0.1, 0.5, 1]
        elif price >= 0.01:
            levels = [0.01, 0.05, 0.1]
        elif price >= 0.001:
            levels = [0.001, 0.005, 0.01]
        else:
            levels = [0.0001, 0.0005, 0.001]
        
        for level in levels:
            # Calculate nearest multiples
            above = round(price / level) * level
            below = round(price / level) * level
            
            # Check distance
            for test_price in [above, below]:
                if test_price > 0:
                    distance_pct = abs(test_price - price) / price
                    if distance_pct < Config.LIQUIDITY_PROXIMITY_THRESHOLD:
                        zones.append({
                            'price': test_price,
                            'type': 'round_number',
                            'distance_pct': distance_pct * 100,
                            'strength': 1
                        })
        
        return zones

# ==================== TRAP DETECTION ====================
class TrapDetector:
    """Detect trapped traders"""
    
    @staticmethod
    def analyze_trap(oi_data: pd.DataFrame, price_5m: pd.DataFrame, current_price: float) -> Tuple[bool, str, Optional[Dict]]:
        """
        Analyze if traders are trapped
        Returns: (has_trap, info, trap_data)
        """
        if oi_data is None or len(oi_data) < 10:
            return False, "Insufficient OI data", None
        
        if price_5m is None or len(price_5m) < 10:
            return False, "Insufficient 5m price data", None
        
        # Calculate OI change
        oi_start = oi_data['openInterest'].iloc[0]
        oi_end = oi_data['openInterest'].iloc[-1]
        oi_change_pct = (oi_end - oi_start) / oi_start if oi_start > 0 else 0
        
        # Calculate price change during same period
        price_start = price_5m['close'].iloc[0]
        price_end = price_5m['close'].iloc[-1]
        price_change_pct = (price_end - price_start) / price_start
        
        # Calculate price range
        price_high = price_5m['high'].max()
        price_low = price_5m['low'].min()
        current_position = (current_price - price_low) / (price_high - price_low) if price_high > price_low else 0.5
        
        logger.debug(f"OI change: {oi_change_pct:.2%}, Price change: {price_change_pct:.2%}, Position: {current_position:.2%}")
        
        # Check for trap conditions
        is_oi_increasing = oi_change_pct > Config.OI_INCREASE_MINIMUM
        is_price_stalled = abs(price_change_pct) < Config.PRICE_STALL_THRESHOLD
        
        if not is_oi_increasing:
            return False, f"OI not increasing ({oi_change_pct:.2%} < {Config.OI_INCREASE_MINIMUM*100:.1f}%)", None
        
        if not is_price_stalled:
            return False, f"Price moving ({price_change_pct:.2%}), not stalled", None
        
        # Determine which side is trapped based on price position
        if current_position > 0.7:  # Near highs
            return True, f"Longs trapped near highs (OI ↗{oi_change_pct:.1%}, price ∆{price_change_pct:.1%})", {
                'side': 'longs',
                'oi_change': oi_change_pct,
                'price_change': price_change_pct,
                'position': 'high'
            }
        elif current_position < 0.3:  # Near lows
            return True, f"Shorts trapped near lows (OI ↗{oi_change_pct:.1%}, price ∆{price_change_pct:.1%})", {
                'side': 'shorts',
                'oi_change': oi_change_pct,
                'price_change': price_change_pct,
                'position': 'low'
            }
        else:
            return False, f"OI ↗{oi_change_pct:.1%} but price mid-range (position: {current_position:.1%})", None

# ==================== FUNDING ANALYSIS ====================
class FundingAnalyzer:
    """Analyze funding pressure"""
    
    @staticmethod
    def analyze_funding(funding_data: pd.DataFrame) -> Tuple[bool, str, Optional[Dict]]:
        """
        Analyze funding pressure
        Returns: (has_pressure, info, funding_data)
        """
        if funding_data is None or len(funding_data) < 6:
            return False, "Insufficient funding data", None
        
        current_funding = funding_data['fundingRate'].iloc[-1]
        
        # Check if funding is significant
        if abs(current_funding) < Config.FUNDING_SIGNIFICANT:
            return False, f"Funding insignificant ({current_funding:.5%} < {Config.FUNDING_SIGNIFICANT*100:.3f}%)", None
        
        # Analyze trend
        recent_avg = funding_data['fundingRate'].tail(3).mean()
        previous_avg = funding_data['fundingRate'].iloc[-6:-3].mean() if len(funding_data) >= 6 else 0
        
        if current_funding > 0:
            # Positive funding
            if recent_avg > previous_avg:
                return True, f"Longs bleeding (funding ↗{current_funding:.5%}, increasing)", {
                    'side': 'longs',
                    'rate': current_funding,
                    'trend': 'increasing'
                }
            else:
                return True, f"Longs paying but stable ({current_funding:.5%})", {
                    'side': 'longs',
                    'rate': current_funding,
                    'trend': 'stable'
                }
        else:
            # Negative funding
            if recent_avg < previous_avg:
                return True, f"Shorts bleeding (funding ↘{current_funding:.5%}, increasing)", {
                    'side': 'shorts',
                    'rate': current_funding,
                    'trend': 'increasing'
                }
            else:
                return True, f"Shorts paying but stable ({current_funding:.5%})", {
                    'side': 'shorts',
                    'rate': current_funding,
                    'trend': 'stable'
                }

# ==================== SIGNAL GENERATOR ====================
class SignalGenerator:
    """Generate trade signals from analysis"""
    
    @staticmethod
    def generate_signal(symbol: str, ticker: Dict, 
                       liquidity_result: Tuple[bool, str, Optional[Dict]],
                       trap_result: Tuple[bool, str, Optional[Dict]],
                       funding_result: Tuple[bool, str, Optional[Dict]]) -> Optional[Dict]:
        """
        Generate trade signal if all conditions align
        """
        has_liquidity, liquidity_info, liquidity_data = liquidity_result
        has_trap, trap_info, trap_data = trap_result
        has_funding, funding_info, funding_data = funding_result
        
        # Log individual results for debugging
        logger.debug(f"{symbol}: Liquidity - {has_liquidity} ({liquidity_info})")
        logger.debug(f"{symbol}: Trap - {has_trap} ({trap_info})")
        logger.debug(f"{symbol}: Funding - {has_funding} ({funding_info})")
        
        # Check all conditions
        if not (has_liquidity and has_trap and has_funding):
            return None
        
        # Extract data
        liquidity_price = liquidity_data['price']
        liquidity_type = liquidity_data['type']
        trapped_side = trap_data['side']
        bleeding_side = funding_data['side']
        current_price = ticker['last_price']
        
        # Determine direction
        direction = None
        
        # SHORT: near highs, longs trapped, longs bleeding
        if (liquidity_type in ['recent_high', 'range_high'] and 
            trapped_side == 'longs' and 
            bleeding_side == 'longs'):
            direction = 'SHORT'
        
        # LONG: near lows, shorts trapped, shorts bleeding
        elif (liquidity_type in ['recent_low', 'range_low'] and 
              trapped_side == 'shorts' and 
              bleeding_side == 'shorts'):
            direction = 'LONG'
        
        if not direction:
            logger.debug(f"{symbol}: Conditions not aligned for trade direction")
            return None
        
        # Calculate parameters
        entry_min, entry_max = SignalGenerator._calculate_entry_zone(
            direction, current_price, liquidity_price
        )
        
        stop_loss, tp1, tp2, tp3 = SignalGenerator._calculate_risk_parameters(
            direction, current_price, liquidity_price
        )
        
        # Calculate confidence
        confidence = SignalGenerator._calculate_confidence(
            trap_data['oi_change'], abs(funding_data['rate']), liquidity_data['strength']
        )
        
        # Create signal
        signal = {
            'symbol': symbol,
            'direction': direction,
            'entry_min': entry_min,
            'entry_max': entry_max,
            'stop_loss': stop_loss,
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'reason': f"{trap_info} | {funding_info}",
            'liquidity_price': liquidity_price,
            'liquidity_type': liquidity_type,
            'trapped_side': trapped_side,
            'oi_change_pct': trap_data['oi_change'],
            'bleeding_side': bleeding_side,
            'funding_rate': funding_data['rate'],
            'confidence': confidence,
            'current_price': current_price,
            'timestamp': datetime.now()
        }
        
        return signal
    
    @staticmethod
    def _calculate_entry_zone(direction: str, current_price: float, liquidity_price: float) -> Tuple[float, float]:
        """Calculate entry zone"""
        if direction == 'SHORT':
            entry_max = min(current_price * 1.002, liquidity_price * 0.999)
            entry_min = entry_max * 0.997
        else:  # LONG
            entry_min = max(current_price * 0.998, liquidity_price * 1.001)
            entry_max = entry_min * 1.003
        
        return entry_min, entry_max
    
    @staticmethod
    def _calculate_risk_parameters(direction: str, entry_price: float, liquidity_price: float) -> Tuple[float, float, float, float]:
        """Calculate stop loss and take profit levels"""
        if direction == 'SHORT':
            stop_loss = liquidity_price * Config.STOP_LOSS_BUFFER
            tp1 = entry_price * 0.99
            tp2 = entry_price * 0.98
            tp3 = entry_price * 0.96
        else:  # LONG
            stop_loss = liquidity_price / Config.STOP_LOSS_BUFFER
            tp1 = entry_price * 1.01
            tp2 = entry_price * 1.02
            tp3 = entry_price * 1.04
        
        return stop_loss, tp1, tp2, tp3
    
    @staticmethod
    def _calculate_confidence(oi_change: float, funding_abs: float, liquidity_strength: int) -> int:
        """Calculate confidence score 1-10"""
        confidence = 5
        
        # OI change contribution
        if oi_change > 0.1:
            confidence += 2
        elif oi_change > 0.05:
            confidence += 1
        
        # Funding contribution
        if funding_abs > 0.001:
            confidence += 2
        elif funding_abs > 0.0005:
            confidence += 1
        
        # Liquidity strength
        confidence += liquidity_strength - 1
        
        return min(max(confidence, 1), 10)

# ==================== TELEGRAM ALERTER ====================
class TelegramAlerter:
    """Send alerts to Telegram"""
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
    
    async def send_signal(self, signal: Dict):
        """Send trade signal to Telegram"""
        try:
            message = self._format_message(signal)
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"✅ Telegram alert sent for {signal['symbol']}")
                    else:
                        error_text = await response.text()
                        logger.error(f"Telegram send failed: {error_text}")
                        
        except Exception as e:
            logger.error(f"Telegram error: {str(e)}")
    
    def _format_message(self, signal: Dict) -> str:
        """Format the trade signal"""
        direction_emoji = "🔴" if signal['direction'] == "SHORT" else "🟢"
        
        return f"""
{direction_emoji} <b>LIQUIDATION SIGNAL</b> {direction_emoji}

<b>Symbol:</b> {signal['symbol']}
<b>Direction:</b> {signal['direction']}
<b>Confidence:</b> {signal['confidence']}/10

<b>Current Price:</b> ${signal['current_price']:.8f}
<b>Liquidity:</b> ${signal['liquidity_price']:.8f} ({signal['liquidity_type']})

<b>Entry Zone:</b>
Min: ${signal['entry_min']:.8f}
Max: ${signal['entry_max']:.8f}

<b>Risk Management:</b>
🛑 Stop Loss: ${signal['stop_loss']:.8f}
✅ TP1: ${signal['tp1']:.8f}
✅ TP2: ${signal['tp2']:.8f}
🎯 TP3: ${signal['tp3']:.8f}

<b>Setup:</b>
{signal['reason']}

<b>Time:</b> {signal['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}

#{signal['direction']} #{signal['symbol'].replace('USDT', '')}
        """

# ==================== MAIN SCANNER ====================
class OptimizedLiquidationScanner:
    """Optimized scanner with better detection"""
    
    def __init__(self):
        self.telegram = TelegramAlerter(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.liquidity_detector = LiquidityDetector()
        self.trap_detector = TrapDetector()
        self.funding_analyzer = FundingAnalyzer()
        self.signal_generator = SignalGenerator()
        
        self.recent_signals = {}
        self.signal_cooldown = 7200  # 2 hours cooldown
        
        # Statistics
        self.scan_count = 0
        self.signal_count = 0
    
    async def scan_symbol(self, client: BybitPublicAPI, symbol: str) -> Optional[Dict]:
        """Scan a single symbol"""
        try:
            self.scan_count += 1
            
            # Get ticker data
            ticker = await client.get_ticker_info(symbol)
            if not ticker:
                logger.debug(f"{symbol}: No ticker data")
                return None
            
            # Check volume filter
            volume_24h = ticker['turnover_24h']
            if volume_24h < Config.MIN_24H_VOLUME_USD or volume_24h > Config.MAX_24H_VOLUME_USD:
                logger.debug(f"{symbol}: Volume ${volume_24h:,.0f} outside range")
                return None
            
            # Check exclusion list
            base_symbol = symbol.replace('USDT', '').upper()
            if base_symbol in Config.EXCLUDE_SYMBOLS:
                return None
            
            current_price = ticker['last_price']
            logger.debug(f"Scanning {symbol}: ${current_price:.6f}, Volume: ${volume_24h:,.0f}")
            
            # Fetch data concurrently
            tasks = [
                client.get_klines(symbol, '1h', limit=50),
                client.get_klines(symbol, '15m', limit=50),
                client.get_klines(symbol, '5m', limit=30),
                client.get_open_interest(symbol, '5m', limit=15),
                client.get_funding_rate_history(symbol, limit=8)
            ]
            
            results = await asyncio.gather(*tasks)
            
            df_1h, df_15m, df_5m, oi_data, funding_data = results
            
            # Check data quality
            if len(df_1h) < 24 or len(df_15m) < 16 or len(df_5m) < 10:
                logger.debug(f"{symbol}: Insufficient price data")
                return None
            
            if len(oi_data) < 10 or len(funding_data) < 6:
                logger.debug(f"{symbol}: Insufficient OI/funding data")
                return None
            
            # Analyze conditions
            price_data = {'1h': df_1h, '15m': df_15m}
            
            liquidity_result = self.liquidity_detector.find_liquidity_zones(
                price_data, current_price
            )
            
            trap_result = self.trap_detector.analyze_trap(
                oi_data, df_5m, current_price
            )
            
            funding_result = self.funding_analyzer.analyze_funding(
                funding_data
            )
            
            # Generate signal if all conditions met
            signal = self.signal_generator.generate_signal(
                symbol, ticker, liquidity_result, trap_result, funding_result
            )
            
            if signal:
                self.signal_count += 1
                logger.info(f"✅ SIGNAL FOUND: {symbol} {signal['direction']}")
                logger.info(f"   Reason: {signal['reason']}")
                return signal
            
            # Log why no signal
            has_liquidity, liquidity_info, _ = liquidity_result
            has_trap, trap_info, _ = trap_result
            has_funding, funding_info, _ = funding_result
            
            conditions = []
            if not has_liquidity:
                conditions.append(f"No liquidity: {liquidity_info}")
            if not has_trap:
                conditions.append(f"No trap: {trap_info}")
            if not has_funding:
                conditions.append(f"No funding pressure: {funding_info}")
            
            if conditions:
                logger.debug(f"{symbol}: No signal - {' | '.join(conditions)}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error scanning {symbol}: {str(e)}")
            return None
    
    async def scan_market(self):
        """Main scanning loop"""
        async with BybitPublicAPI() as client:
            while True:
                try:
                    cycle_start = datetime.now()
                    logger.info("=" * 70)
                    logger.info(f"SCAN CYCLE {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info("=" * 70)
                    
                    # Get all symbols
                    all_symbols = await client.get_all_perpetual_symbols()
                    if not all_symbols:
                        logger.warning("No symbols retrieved")
                        await asyncio.sleep(60)
                        continue
                    
                    # Filter symbols
                    symbols_to_scan = []
                    for symbol in all_symbols:
                        base = symbol.replace('USDT', '').upper()
                        if base not in Config.EXCLUDE_SYMBOLS:
                            symbols_to_scan.append(symbol)
                    
                    logger.info(f"Scanning {len(symbols_to_scan)} symbols")
                    
                    # Scan symbols in batches
                    batch_size = Config.MAX_CONCURRENT_REQUESTS
                    signals_found = 0
                    
                    for i in range(0, len(symbols_to_scan), batch_size):
                        batch = symbols_to_scan[i:i+batch_size]
                        logger.debug(f"Processing batch {i//batch_size + 1}: {len(batch)} symbols")
                        
                        # Create scanning tasks
                        tasks = []
                        for symbol in batch:
                            # Check cooldown
                            if symbol in self.recent_signals:
                                last_time = self.recent_signals[symbol]
                                if (datetime.now() - last_time).seconds < self.signal_cooldown:
                                    continue
                            
                            tasks.append(self.scan_symbol(client, symbol))
                        
                        # Execute batch
                        if tasks:
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            
                            # Process results
                            for result in results:
                                if isinstance(result, Exception):
                                    continue
                                
                                if result and isinstance(result, dict):
                                    # Check confidence threshold
                                    if result['confidence'] >= Config.MIN_CONFIDENCE:
                                        # Send alert
                                        await self.telegram.send_signal(result)
                                        signals_found += 1
                                        
                                        # Update cooldown
                                        self.recent_signals[result['symbol']] = datetime.now()
                        
                        # Rate limiting between batches
                        await asyncio.sleep(2)
                    
                    # Log statistics
                    cycle_time = (datetime.now() - cycle_start).total_seconds()
                    logger.info(f"Cycle completed in {cycle_time:.1f}s")
                    logger.info(f"Signals found this cycle: {signals_found}")
                    logger.info(f"Total scans: {self.scan_count}, Total signals: {self.signal_count}")
                    
                    # Wait for next cycle
                    logger.info(f"Next scan in {Config.SCAN_INTERVAL_SECONDS} seconds...")
                    await asyncio.sleep(Config.SCAN_INTERVAL_SECONDS)
                    
                except KeyboardInterrupt:
                    logger.info("Scanner stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Fatal error in scan cycle: {str(e)}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(60)

# ==================== MAIN ====================
async def main():
    """Main entry point"""
    
    # Log configuration
    logger.info("🚀 OPTIMIZED LIQUIDATION SCANNER")
    logger.info("=" * 60)
    logger.info(f"Telegram Bot: Configured")
    logger.info(f"Chat ID: {Config.TELEGRAM_CHAT_ID[:10]}...")
    logger.info(f"Volume Range: ${Config.MIN_24H_VOLUME_USD:,.0f} - ${Config.MAX_24H_VOLUME_USD:,.0f}")
    logger.info(f"Scan Interval: {Config.SCAN_INTERVAL_SECONDS}s")
    logger.info(f"Min Confidence: {Config.MIN_CONFIDENCE}/10")
    logger.info(f"Debug Mode: {Config.DEBUG_MODE}")
    logger.info("=" * 60)
    
    # Create and run scanner
    scanner = OptimizedLiquidationScanner()
    
    try:
        await scanner.scan_market()
    except KeyboardInterrupt:
        logger.info("Scanner stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    # Run the scanner
    asyncio.run(main())