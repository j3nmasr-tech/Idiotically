"""
INSTITUTIONAL LIQUIDATION SCANNER & EXECUTION ENGINE
Bybit Public API + Telegram Alerts
Environment Variables Configuration

STRICT FRAMEWORK:
1. WHERE liquidity is clustered
2. WHO is trapped in positions
3. WHO is bleeding from funding
ALL THREE → TRADE SIGNAL
ANY MISSING → NO TRADE

⚠️ NO INDICATORS, NO PATTERNS, NO NEWS, NO BIAS
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
# Load from environment variables or .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required

# Required Environment Variables
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
if not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_CHAT_ID environment variable is required")

# Optional Environment Variables
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '180'))
MIN_VOLUME = int(os.getenv('MIN_VOLUME', '1000000'))
MAX_VOLUME = int(os.getenv('MAX_VOLUME', '30000000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# ==================== CONFIGURATION ====================
class Config:
    # Telegram Configuration (FROM ENVIRONMENT VARIABLES)
    TELEGRAM_BOT_TOKEN = TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
    
    # Bybit Public API Endpoints
    BYBIT_BASE_URL = "https://api.bybit.com"
    
    # Scanner Settings (from environment or defaults)
    SCAN_INTERVAL_SECONDS = SCAN_INTERVAL  # From env
    MAX_CONCURRENT_REQUESTS = 10
    REQUEST_TIMEOUT = 15
    
    # Market Filters - STRICT LOW-CAP ONLY
    MIN_24H_VOLUME_USD = MIN_VOLUME      # From env
    MAX_24H_VOLUME_USD = MAX_VOLUME      # From env
    MIN_PRICE = 0.000001
    
    # Exclude ALL major coins
    EXCLUDE_SYMBOLS = [
        'BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'AVAX', 'DOT',
        'DOGE', 'MATIC', 'SHIB', 'TRX', 'LINK', 'UNI', 'ATOM',
        'LTC', 'BCH', 'FIL', 'ETC', 'APT', 'NEAR', 'ARB', 'OP',
        'SUI', 'SEI', 'INJ', 'RNDR', 'IMX', 'AAVE', 'ALGO', 'APE',
        'AXS', 'BATUSDT', 'COMP', 'CRV', 'EOS', 'FET', 'FLOW',
        'FTM', 'GALA', 'GRT', 'HBAR', 'ICP', 'KAS', 'KAVA', 'KSM',
        'LDO', 'MANA', 'MKR', 'ONE', 'QNT', 'SAND', 'SNX', 'STX',
        'THETA', 'VET', 'XTZ', 'YFI', 'ZEC', 'ZIL', 'ZRX'
    ]
    
    # Additional exclude symbols from environment
    ADDITIONAL_EXCLUDE = os.getenv('EXCLUDE_SYMBOLS', '')
    if ADDITIONAL_EXCLUDE:
        EXCLUDE_SYMBOLS.extend([s.strip().upper() for s in ADDITIONAL_EXCLUDE.split(',')])
    
    # Liquidity Detection Parameters
    SWING_LOOKBACK = 20
    EQUAL_HIGH_LOW_TOLERANCE = 0.002
    LIQUIDITY_PROXIMITY_THRESHOLD = 0.015
    
    # Trap Detection Parameters
    OI_INCREASE_MINIMUM = 0.05
    PRICE_STALL_THRESHOLD = 0.005
    
    # Funding Pressure Parameters
    FUNDING_SIGNIFICANT = 0.0003
    FUNDING_TREND_WINDOW = 8
    
    # Risk Management
    STOP_LOSS_BUFFER = 1.005
    TP1_ALLOCATION = 0.5
    TP2_ALLOCATION = 0.3
    TP3_ALLOCATION = 0.2
    
    # Signal Confidence Threshold (from env or default)
    MIN_CONFIDENCE = int(os.getenv('MIN_CONFIDENCE', '6'))

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'liquidation_scanner_{datetime.now().strftime("%Y%m%d")}.log'),
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
class TradeSignal:
    symbol: str
    direction: str
    entry_min: float
    entry_max: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    reason: str
    liquidity_price: float
    liquidity_type: str
    trapped_side: str
    oi_change_pct: float
    bleeding_side: str
    funding_rate: float
    confidence: int
    timestamp: datetime

# ==================== BYBIT PUBLIC API CLIENT ====================
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
    
    async def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        if params is None:
            params = {}
        url = f"{self.base_url}{endpoint}"
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('retCode') == 0:
                        return data.get('result', {})
                return {}
        except Exception as e:
            logger.error(f"Request error {endpoint}: {str(e)}")
            return {}
    
    async def get_all_perpetual_symbols(self) -> List[str]:
        params = {'category': 'linear'}
        data = await self._make_request('/v5/market/tickers', params)
        symbols = []
        if data and 'list' in data:
            for ticker in data['list']:
                symbol = ticker.get('symbol', '')
                if symbol.endswith('USDT'):
                    symbols.append(symbol)
        return symbols
    
    async def get_ticker_24h(self, symbol: str) -> Optional[Dict]:
        params = {'category': 'linear', 'symbol': symbol}
        data = await self._make_request('/v5/market/tickers', params)
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
            }
        return None
    
    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
        interval_map = {
            '1m': '1', '5m': '5', '15m': '15',
            '1h': '60', '4h': '240', '1d': 'D'
        }
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': interval_map.get(interval, interval),
            'limit': limit
        }
        data = await self._make_request('/v5/market/kline', params)
        if data and 'list' in data:
            df = pd.DataFrame(data['list'], columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float).astype(int), unit='ms')
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            for col in numeric_cols:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp').reset_index(drop=True)
            return df
        return pd.DataFrame()
    
    async def get_open_interest(self, symbol: str, interval: str = '5m', limit: int = 50) -> pd.DataFrame:
        interval_map = {'5m': '5min', '15m': '15min', '1h': '1h'}
        params = {
            'category': 'linear',
            'symbol': symbol,
            'intervalTime': interval_map.get(interval, interval),
            'limit': limit
        }
        data = await self._make_request('/v5/market/open-interest', params)
        if data and 'list' in data:
            df = pd.DataFrame(data['list'])
            if 'timestamp' in df.columns and 'openInterest' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float).astype(int), unit='ms')
                df['openInterest'] = df['openInterest'].astype(float)
                df = df.sort_values('timestamp').reset_index(drop=True)
                return df
        return pd.DataFrame()
    
    async def get_funding_rate_history(self, symbol: str, limit: int = 20) -> pd.DataFrame:
        params = {
            'category': 'linear',
            'symbol': symbol,
            'limit': limit
        }
        data = await self._make_request('/v5/market/funding/history', params)
        if data and 'list' in data:
            df = pd.DataFrame(data['list'])
            if 'fundingRateTimestamp' in df.columns and 'fundingRate' in df.columns:
                df['timestamp'] = pd.to_datetime(df['fundingRateTimestamp'].astype(float).astype(int), unit='ms')
                df['fundingRate'] = df['fundingRate'].astype(float)
                df = df.sort_values('timestamp').reset_index(drop=True)
                return df
        return pd.DataFrame()

# ==================== LIQUIDITY ENGINE ====================
class LiquidityEngine:
    @staticmethod
    def find_all_liquidity_zones(df_1h: pd.DataFrame, df_15m: pd.DataFrame, current_price: float) -> List[LiquidityZone]:
        zones = []
        if len(df_1h) < Config.SWING_LOOKBACK * 2 or len(df_15m) < 50:
            return zones
        
        # Swing points
        zones.extend(LiquidityEngine._find_swing_points(df_1h, '1h'))
        zones.extend(LiquidityEngine._find_swing_points(df_15m, '15m'))
        
        # Equal highs/lows
        zones.extend(LiquidityEngine._find_equal_highs_lows(df_15m))
        
        # Range levels
        zones.extend(LiquidityEngine._find_range_levels(df_15m))
        
        # Recent extremes
        zones.extend(LiquidityEngine._find_recent_extremes(df_1h))
        
        # Round numbers
        zones.extend(LiquidityEngine._find_round_numbers(current_price))
        
        # Remove duplicates
        unique_zones = []
        seen_prices = set()
        for zone in zones:
            price_rounded = round(zone.price, 8)
            if price_rounded not in seen_prices:
                seen_prices.add(price_rounded)
                unique_zones.append(zone)
        
        return unique_zones
    
    @staticmethod
    def _find_swing_points(df: pd.DataFrame, timeframe: str) -> List[LiquidityZone]:
        zones = []
        lookback = Config.SWING_LOOKBACK
        if len(df) < lookback * 2:
            return zones
        
        for i in range(lookback, len(df) - lookback):
            if df['high'].iloc[i] == df['high'].iloc[i-lookback:i+lookback+1].max():
                zones.append(LiquidityZone(
                    price=df['high'].iloc[i],
                    zone_type='swing_high',
                    timeframe=timeframe,
                    strength=2,
                    timestamp=df['timestamp'].iloc[i]
                ))
            if df['low'].iloc[i] == df['low'].iloc[i-lookback:i+lookback+1].min():
                zones.append(LiquidityZone(
                    price=df['low'].iloc[i],
                    zone_type='swing_low',
                    timeframe=timeframe,
                    strength=2,
                    timestamp=df['timestamp'].iloc[i]
                ))
        return zones
    
    @staticmethod
    def _find_equal_highs_lows(df: pd.DataFrame) -> List[LiquidityZone]:
        zones = []
        if len(df) < 20:
            return zones
        
        highs = df['high'].values
        for i in range(len(highs) - 10):
            cluster = highs[i:i+10]
            if max(cluster) - min(cluster) < Config.EQUAL_HIGH_LOW_TOLERANCE * np.mean(cluster):
                avg_price = np.mean(cluster)
                zones.append(LiquidityZone(
                    price=avg_price,
                    zone_type='equal_high',
                    timeframe='15m',
                    strength=3,
                    timestamp=df['timestamp'].iloc[i+5]
                ))
        
        lows = df['low'].values
        for i in range(len(lows) - 10):
            cluster = lows[i:i+10]
            if max(cluster) - min(cluster) < Config.EQUAL_HIGH_LOW_TOLERANCE * np.mean(cluster):
                avg_price = np.mean(cluster)
                zones.append(LiquidityZone(
                    price=avg_price,
                    zone_type='equal_low',
                    timeframe='15m',
                    strength=3,
                    timestamp=df['timestamp'].iloc[i+5]
                ))
        return zones
    
    @staticmethod
    def _find_range_levels(df: pd.DataFrame) -> List[LiquidityZone]:
        zones = []
        if len(df) < 20:
            return zones
        
        recent = df.tail(20)
        range_high = recent['high'].max()
        range_low = recent['low'].min()
        
        if (range_high - range_low) / range_low > 0.01:
            zones.append(LiquidityZone(
                price=range_high,
                zone_type='range_high',
                timeframe='15m',
                strength=2,
                timestamp=df['timestamp'].iloc[-1]
            ))
            zones.append(LiquidityZone(
                price=range_low,
                zone_type='range_low',
                timeframe='15m',
                strength=2,
                timestamp=df['timestamp'].iloc[-1]
            ))
        return zones
    
    @staticmethod
    def _find_recent_extremes(df: pd.DataFrame) -> List[LiquidityZone]:
        zones = []
        if len(df) < 24:
            return zones
        
        recent = df.tail(24)
        zones.append(LiquidityZone(
            price=recent['high'].max(),
            zone_type='recent_high',
            timeframe='1h',
            strength=1,
            timestamp=recent['timestamp'].iloc[-1]
        ))
        zones.append(LiquidityZone(
            price=recent['low'].min(),
            zone_type='recent_low',
            timeframe='1h',
            strength=1,
            timestamp=recent['timestamp'].iloc[-1]
        ))
        return zones
    
    @staticmethod
    def _find_round_numbers(price: float) -> List[LiquidityZone]:
        zones = []
        if price <= 0:
            return zones
        
        if price >= 1:
            levels = [0.1, 0.5, 1, 5, 10]
        elif price >= 0.1:
            levels = [0.01, 0.05, 0.1, 0.5]
        elif price >= 0.01:
            levels = [0.001, 0.005, 0.01, 0.05]
        elif price >= 0.001:
            levels = [0.0001, 0.0005, 0.001, 0.005]
        else:
            levels = [0.00001, 0.00005, 0.0001, 0.0005]
        
        for level in levels:
            above = (int(price / level) + 1) * level
            below = (int(price / level)) * level
            
            if abs(above - price) / price < 0.05:
                zones.append(LiquidityZone(
                    price=above,
                    zone_type='round_number',
                    timeframe='all',
                    strength=1,
                    timestamp=datetime.now()
                ))
            
            if below > 0 and abs(below - price) / price < 0.05:
                zones.append(LiquidityZone(
                    price=below,
                    zone_type='round_number',
                    timeframe='all',
                    strength=1,
                    timestamp=datetime.now()
                ))
        return zones
    
    @staticmethod
    def find_nearest_significant_liquidity(zones: List[LiquidityZone], current_price: float) -> Optional[LiquidityZone]:
        if not zones:
            return None
        
        valid_zones = []
        for zone in zones:
            distance_pct = abs(zone.price - current_price) / current_price
            if distance_pct <= Config.LIQUIDITY_PROXIMITY_THRESHOLD:
                valid_zones.append((zone, distance_pct))
        
        if not valid_zones:
            return None
        
        valid_zones.sort(key=lambda x: (x[1], -x[0].strength))
        return valid_zones[0][0]

# ==================== TRAP DETECTION ENGINE ====================
class TrapDetector:
    @staticmethod
    def analyze_trap(oi_df: pd.DataFrame, price_5m: pd.DataFrame) -> Tuple[Optional[str], float, str]:
        if len(oi_df) < 10 or len(price_5m) < 10:
            return None, 0.0, "Insufficient data"
        
        oi_start = oi_df['openInterest'].iloc[0]
        oi_end = oi_df['openInterest'].iloc[-1]
        oi_change_pct = (oi_end - oi_start) / oi_start if oi_start > 0 else 0
        
        price_start = price_5m['close'].iloc[0]
        price_end = price_5m['close'].iloc[-1]
        price_change_pct = (price_end - price_start) / price_start
        
        is_oi_increasing = oi_change_pct > Config.OI_INCREASE_MINIMUM
        is_price_stalled = abs(price_change_pct) < Config.PRICE_STALL_THRESHOLD
        
        if not is_oi_increasing:
            return None, oi_change_pct, f"OI not increasing ({oi_change_pct:.1%})"
        
        if not is_price_stalled:
            return None, oi_change_pct, f"Price moving ({price_change_pct:.1%}), not stalled"
        
        current_price = price_5m['close'].iloc[-1]
        recent_high = price_5m['high'].tail(10).max()
        recent_low = price_5m['low'].tail(10).min()
        
        near_high = (recent_high - current_price) / current_price < 0.005
        near_low = (current_price - recent_low) / current_price < 0.005
        
        if near_high:
            return 'longs', oi_change_pct, f"Longs trapped near highs (OI ↗{oi_change_pct:.1%})"
        elif near_low:
            return 'shorts', oi_change_pct, f"Shorts trapped near lows (OI ↗{oi_change_pct:.1%})"
        else:
            return None, oi_change_pct, f"OI ↗{oi_change_pct:.1%} but price mid-range"

# ==================== FUNDING PRESSURE ENGINE ====================
class FundingAnalyzer:
    @staticmethod
    def analyze_funding_pressure(funding_df: pd.DataFrame) -> Tuple[Optional[str], float, str]:
        if len(funding_df) < Config.FUNDING_TREND_WINDOW:
            return None, 0.0, "Insufficient funding history"
        
        current_funding = funding_df['fundingRate'].iloc[-1]
        
        if abs(current_funding) < Config.FUNDING_SIGNIFICANT:
            return None, current_funding, f"Funding insignificant ({current_funding:.5%})"
        
        if len(funding_df) >= Config.FUNDING_TREND_WINDOW:
            recent = funding_df['fundingRate'].tail(Config.FUNDING_TREND_WINDOW//2).mean()
            previous = funding_df['fundingRate'].iloc[-Config.FUNDING_TREND_WINDOW:-Config.FUNDING_TREND_WINDOW//2].mean()
            
            if current_funding > 0:
                if recent > previous:
                    return 'longs', current_funding, f"Longs bleeding (funding ↗{current_funding:.5%})"
                else:
                    return 'longs', current_funding, f"Longs paying but decreasing ({current_funding:.5%})"
            else:
                if recent < previous:
                    return 'shorts', current_funding, f"Shorts bleeding (funding ↘{current_funding:.5%})"
                else:
                    return 'shorts', current_funding, f"Shorts paying but improving ({current_funding:.5%})"
        
        if current_funding > 0:
            return 'longs', current_funding, f"Longs paying ({current_funding:.5%})"
        else:
            return 'shorts', current_funding, f"Shorts paying ({current_funding:.5%})"

# ==================== ENTRY TIMING ENGINE ====================
class EntryTiming:
    @staticmethod
    async def analyze_entry_setup(client: BybitPublicAPI, symbol: str, 
                                 direction: str, liquidity_price: float) -> Optional[Tuple[float, float]]:
        df_1m = await client.get_klines(symbol, '1m', limit=30)
        if len(df_1m) < 10:
            return None
        
        current_price = df_1m['close'].iloc[-1]
        
        if direction == 'SHORT':
            recent = df_1m.tail(5)
            for i in range(len(recent)):
                candle = recent.iloc[i]
                if candle['high'] > liquidity_price * 0.999 and candle['close'] < candle['open']:
                    entry_max = min(liquidity_price, candle['close'] * 1.002)
                    entry_min = entry_max * 0.998
                    return entry_min, entry_max
            
            if current_price < liquidity_price * 0.998:
                entry_max = current_price * 1.002
                entry_min = current_price
                return entry_min, entry_max
        
        else:
            recent = df_1m.tail(5)
            for i in range(len(recent)):
                candle = recent.iloc[i]
                if candle['low'] < liquidity_price * 1.001 and candle['close'] > candle['open']:
                    entry_min = max(liquidity_price, candle['close'] * 0.998)
                    entry_max = entry_min * 1.002
                    return entry_min, entry_max
            
            if current_price > liquidity_price * 1.002:
                entry_min = current_price
                entry_max = current_price * 1.002
                return entry_min, entry_max
        
        return None

# ==================== RISK & TARGET ENGINE ====================
class RiskTargetEngine:
    @staticmethod
    def calculate_parameters(direction: str, entry_price: float, liquidity_price: float,
                            all_zones: List[LiquidityZone]) -> Tuple[float, float, float, float]:
        if direction == 'SHORT':
            stop_loss = liquidity_price * Config.STOP_LOSS_BUFFER
            support_zones = [z for z in all_zones if z.zone_type in [
                'swing_low', 'equal_low', 'range_low', 'recent_low'
            ]]
            supports_below = [z for z in support_zones if z.price < entry_price]
            
            if supports_below:
                supports_below.sort(key=lambda x: x.price, reverse=True)
                tp1 = supports_below[0].price
                tp2 = supports_below[1].price if len(supports_below) > 1 else entry_price * 0.985
                tp3 = supports_below[2].price if len(supports_below) > 2 else min(tp2 * 0.99, entry_price * 0.97)
            else:
                tp1 = entry_price * 0.99
                tp2 = entry_price * 0.98
                tp3 = entry_price * 0.96
        
        else:
            stop_loss = liquidity_price / Config.STOP_LOSS_BUFFER
            resistance_zones = [z for z in all_zones if z.zone_type in [
                'swing_high', 'equal_high', 'range_high', 'recent_high'
            ]]
            resistances_above = [z for z in resistance_zones if z.price > entry_price]
            
            if resistances_above:
                resistances_above.sort(key=lambda x: x.price)
                tp1 = resistances_above[0].price
                tp2 = resistances_above[1].price if len(resistances_above) > 1 else entry_price * 1.015
                tp3 = resistances_above[2].price if len(resistances_above) > 2 else max(tp2 * 1.01, entry_price * 1.03)
            else:
                tp1 = entry_price * 1.01
                tp2 = entry_price * 1.02
                tp3 = entry_price * 1.04
        
        return stop_loss, tp1, tp2, tp3

# ==================== TELEGRAM ALERTER ====================
class TelegramAlerter:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    async def send_signal(self, signal: TradeSignal):
        try:
            message = self._format_signal_message(signal)
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.base_url}/sendMessage", json=payload) as response:
                    if response.status == 200:
                        logger.info(f"Telegram alert sent for {signal.symbol}")
                    else:
                        logger.error(f"Telegram send failed: {await response.text()}")
        except Exception as e:
            logger.error(f"Telegram error: {str(e)}")
    
    def _format_signal_message(self, signal: TradeSignal) -> str:
        direction_emoji = "🔴" if signal.direction == "SHORT" else "🟢"
        confidence_stars = "★" * signal.confidence + "☆" * (10 - signal.confidence)
        
        return f"""
{direction_emoji} <b>LIQUIDATION SIGNAL DETECTED</b> {direction_emoji}

<b>SYMBOL:</b> {signal.symbol}
<b>DIRECTION:</b> {signal.direction}
<b>CONFIDENCE:</b> {confidence_stars} ({signal.confidence}/10)

<b>PRICE DATA:</b>
Current: ${signal.entry_min:.8f}
Liquidity: ${signal.liquidity_price:.8f} ({signal.liquidity_type})

<b>ENTRY ZONE:</b>
Min: ${signal.entry_min:.8f}
Max: ${signal.entry_max:.8f}

<b>RISK MANAGEMENT:</b>
🛑 <b>Stop Loss:</b> ${signal.stop_loss:.8f}
✅ <b>Take Profit 1:</b> ${signal.tp1:.8f}
✅ <b>Take Profit 2:</b> ${signal.tp2:.8f}
🎯 <b>Take Profit 3:</b> ${signal.tp3:.8f}

<b>SETUP LOGIC:</b>
• <b>LIQUIDITY:</b> {signal.liquidity_type} at ${signal.liquidity_price:.8f}
• <b>TRAPPED:</b> {signal.trapped_side} (OI ↗{signal.oi_change_pct:.1%})
• <b>BLEEDING:</b> {signal.bleeding_side} (Funding: {signal.funding_rate:.5%})

<b>REASON:</b> {signal.reason}

<b>Time:</b> {signal.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}

#Liquidation #{signal.direction} #{signal.symbol.replace('USDT', '')}
        """

# ==================== MAIN SCANNER ENGINE ====================
class LiquidationScanner:
    def __init__(self):
        self.telegram = TelegramAlerter(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.liquidity_engine = LiquidityEngine()
        self.trap_detector = TrapDetector()
        self.funding_analyzer = FundingAnalyzer()
        self.entry_timing = EntryTiming()
        self.risk_engine = RiskTargetEngine()
        
        self.recent_signals = {}
        self.signal_cooldown = 3600
    
    async def scan_symbol(self, client: BybitPublicAPI, symbol: str) -> Optional[TradeSignal]:
        try:
            # Get basic data
            ticker = await client.get_ticker_24h(symbol)
            if not ticker:
                return None
            
            # Volume filter
            volume_24h = ticker['turnover_24h']
            if volume_24h < Config.MIN_24H_VOLUME_USD or volume_24h > Config.MAX_24H_VOLUME_USD:
                return None
            
            # Exclude symbols
            base_symbol = symbol.replace('USDT', '').upper()
            if base_symbol in Config.EXCLUDE_SYMBOLS:
                return None
            
            current_price = ticker['last_price']
            if current_price < Config.MIN_PRICE:
                return None
            
            # Get multi-timeframe data
            df_1h = await client.get_klines(symbol, '1h', limit=100)
            df_15m = await client.get_klines(symbol, '15m', limit=100)
            df_5m = await client.get_klines(symbol, '5m', limit=50)
            
            if len(df_1h) < 50 or len(df_15m) < 50 or len(df_5m) < 20:
                return None
            
            # Get OI and funding
            oi_df = await client.get_open_interest(symbol, '5m', limit=20)
            funding_df = await client.get_funding_rate_history(symbol, limit=12)
            
            if len(oi_df) < 10 or len(funding_df) < 8:
                return None
            
            # 1. LIQUIDITY MAPPING
            liquidity_zones = self.liquidity_engine.find_all_liquidity_zones(df_1h, df_15m, current_price)
            nearest_liquidity = self.liquidity_engine.find_nearest_significant_liquidity(
                liquidity_zones, current_price
            )
            
            if not nearest_liquidity:
                return None
            
            # 2. TRAP DETECTION
            trapped_side, oi_change_pct, trap_desc = self.trap_detector.analyze_trap(oi_df, df_5m)
            if not trapped_side:
                return None
            
            # 3. FUNDING PRESSURE
            bleeding_side, funding_rate, funding_desc = self.funding_analyzer.analyze_funding_pressure(funding_df)
            if not bleeding_side:
                return None
            
            # 4. DIRECTION LOGIC
            direction = None
            liquidity_type = nearest_liquidity.zone_type
            
            is_near_high = liquidity_type in ['swing_high', 'equal_high', 'range_high', 'recent_high']
            if is_near_high and trapped_side == 'longs' and bleeding_side == 'longs':
                direction = 'SHORT'
            
            is_near_low = liquidity_type in ['swing_low', 'equal_low', 'range_low', 'recent_low']
            if is_near_low and trapped_side == 'shorts' and bleeding_side == 'shorts':
                direction = 'LONG'
            
            if not direction:
                return None
            
            # 5. ENTRY TIMING
            entry_range = await self.entry_timing.analyze_entry_setup(
                client, symbol, direction, nearest_liquidity.price
            )
            if not entry_range:
                return None
            
            entry_min, entry_max = entry_range
            entry_price = (entry_min + entry_max) / 2
            
            # 6. RISK PARAMETERS
            stop_loss, tp1, tp2, tp3 = self.risk_engine.calculate_parameters(
                direction, entry_price, nearest_liquidity.price, liquidity_zones
            )
            
            # 7. CONFIDENCE SCORING
            confidence = LiquidationScanner._calculate_confidence(
                oi_change_pct, abs(funding_rate), nearest_liquidity.strength
            )
            
            # Create signal
            signal = TradeSignal(
                symbol=symbol,
                direction=direction,
                entry_min=entry_min,
                entry_max=entry_max,
                stop_loss=stop_loss,
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                reason=f"{trap_desc} | {funding_desc}",
                liquidity_price=nearest_liquidity.price,
                liquidity_type=liquidity_type,
                trapped_side=trapped_side,
                oi_change_pct=oi_change_pct,
                bleeding_side=bleeding_side,
                funding_rate=funding_rate,
                confidence=confidence,
                timestamp=datetime.now()
            )
            
            logger.info(f"✅ SIGNAL: {symbol} {direction} (OI ↗{oi_change_pct:.1%}, Funding: {funding_rate:.5%}, Conf: {confidence}/10)")
            return signal
            
        except Exception as e:
            logger.error(f"Error scanning {symbol}: {str(e)}")
            return None
    
    @staticmethod
    def _calculate_confidence(oi_change: float, funding_abs: float, liquidity_strength: int) -> int:
        confidence = 5
        if oi_change > 0.15:
            confidence += 2
        elif oi_change > 0.08:
            confidence += 1
        if funding_abs > 0.001:
            confidence += 2
        elif funding_abs > 0.0005:
            confidence += 1
        if liquidity_strength >= 3:
            confidence += 2
        elif liquidity_strength >= 2:
            confidence += 1
        return min(max(confidence, 1), 10)
    
    async def scan_market(self):
        async with BybitPublicAPI() as client:
            while True:
                try:
                    logger.info("=" * 70)
                    logger.info(f"SCAN CYCLE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info("=" * 70)
                    
                    # Get symbols
                    all_symbols = await client.get_all_perpetual_symbols()
                    if not all_symbols:
                        await asyncio.sleep(30)
                        continue
                    
                    # Filter
                    symbols_to_scan = []
                    for symbol in all_symbols:
                        base = symbol.replace('USDT', '').upper()
                        if base not in Config.EXCLUDE_SYMBOLS:
                            symbols_to_scan.append(symbol)
                    
                    logger.info(f"Scanning {len(symbols_to_scan)} low-cap symbols")
                    
                    # Scan in batches
                    batch_size = Config.MAX_CONCURRENT_REQUESTS
                    signals_found = 0
                    
                    for i in range(0, len(symbols_to_scan), batch_size):
                        batch = symbols_to_scan[i:i+batch_size]
                        tasks = []
                        for symbol in batch:
                            if symbol in self.recent_signals:
                                last_signal = self.recent_signals[symbol]
                                if (datetime.now() - last_signal).seconds < self.signal_cooldown:
                                    continue
                            tasks.append(self.scan_symbol(client, symbol))
                        
                        if tasks:
                            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                            for result in batch_results:
                                if isinstance(result, Exception):
                                    continue
                                if result and isinstance(result, TradeSignal):
                                    if result.confidence >= Config.MIN_CONFIDENCE:
                                        await self.telegram.send_signal(result)
                                        signals_found += 1
                                        self.recent_signals[result.symbol] = datetime.now()
                        
                        await asyncio.sleep(1)
                    
                    logger.info(f"Cycle complete. Found {signals_found} signals.")
                    logger.info(f"Next scan in {Config.SCAN_INTERVAL_SECONDS} seconds...")
                    await asyncio.sleep(Config.SCAN_INTERVAL_SECONDS)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Scan cycle error: {str(e)}")
                    await asyncio.sleep(30)

# ==================== MAIN EXECUTION ====================
async def main():
    """Main entry point with environment variable validation"""
    
    logger.info("🚀 INSTITUTIONAL LIQUIDATION SCANNER")
    logger.info("=" * 60)
    logger.info(f"TELEGRAM BOT: Configured")
    logger.info(f"CHAT ID: {Config.TELEGRAM_CHAT_ID[:10]}...")
    logger.info(f"VOLUME FILTER: ${Config.MIN_24H_VOLUME_USD:,.0f} - ${Config.MAX_24H_VOLUME_USD:,.0f}")
    logger.info(f"SCAN INTERVAL: {Config.SCAN_INTERVAL_SECONDS} seconds")
    logger.info(f"MIN CONFIDENCE: {Config.MIN_CONFIDENCE}/10")
    logger.info(f"EXCLUDING: {len(Config.EXCLUDE_SYMBOLS)} major coins")
    logger.info("=" * 60)
    logger.info("FRAMEWORK: Liquidity → Trap → Bleeding → Trade")
    logger.info("NO alignment = NO trade")
    logger.info("=" * 60)
    
    scanner = LiquidationScanner()
    
    try:
        await scanner.scan_market()
    except KeyboardInterrupt:
        logger.info("Scanner stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())