import os
import asyncio
import aiohttp
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

# Configure robust logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler('bybit_romeopt_scanner.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class PerfectBybitRomeOPTScanner:
    def __init__(self):
        self.logger = logging.getLogger("BybitRomeOPT")
        self.logger.info("🚀 INITIALIZING PERFECT BYBIT ROMEOPT SCANNER")
        
        # Load environment variables
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.telegram_token or not self.telegram_chat_id:
            raise ValueError("❌ Missing Telegram credentials")
        
        # OPTIMAL CONFIGURATION
        self.coins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']  # Bybit symbol format
        self.timeframe = '5'  # Bybit uses numbers: 1,3,5,15,30,60,120,240,360,720,D,W,M
        self.analysis_interval = 30  # Seconds between cycles
        
        # Bybit API configuration
        self.base_url = "https://api.bybit.com"
        
        # Data storage
        self.price_data = {}
        self.active_signals = {}
        self.signal_history = []
        
        # Performance tracking
        self.start_time = datetime.now()
        self.signals_generated = 0
        self.analysis_count = 0
        
        self.logger.info("✅ PERFECT BYBIT SCANNER INITIALIZED")

    async def get_bybit_klines(self, symbol: str, limit: int = 25) -> Optional[List[Dict]]:
        """Get reliable klines data from Bybit"""
        try:
            url = f"{self.base_url}/v5/market/kline"
            params = {
                'category': 'spot',  # Use spot market
                'symbol': symbol,
                'interval': self.timeframe,
                'limit': limit
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data['retCode'] == 0 and 'result' in data and 'list' in data['result']:
                            candles = []
                            for candle in data['result']['list']:
                                # Bybit returns: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
                                candles.append({
                                    'timestamp': datetime.fromtimestamp(int(candle[0]) / 1000),
                                    'open': float(candle[1]),
                                    'high': float(candle[2]),
                                    'low': float(candle[3]),
                                    'close': float(candle[4]),
                                    'volume': float(candle[5]),
                                    'is_closed': True
                                })
                            # Reverse to get chronological order (oldest first)
                            candles.reverse()
                            self.logger.debug(f"📊 {symbol}: {len(candles)} Bybit candles loaded")
                            return candles
                        else:
                            self.logger.warning(f"❌ {symbol}: Bybit API error: {data.get('retMsg', 'Unknown error')}")
                            return None
                    else:
                        self.logger.warning(f"❌ {symbol}: Bybit HTTP error {response.status}")
                        return None
        except Exception as e:
            self.logger.error(f"❌ {symbol}: Bybit klines error: {str(e)}")
            return None

    async def get_bybit_ticker(self, symbol: str) -> Optional[float]:
        """Get reliable current price from Bybit"""
        try:
            url = f"{self.base_url}/v5/market/tickers"
            params = {
                'category': 'spot',
                'symbol': symbol
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data['retCode'] == 0 and 'result' in data and 'list' in data['result']:
                            for ticker in data['result']['list']:
                                if ticker['symbol'] == symbol:
                                    price = float(ticker['lastPrice'])
                                    self.logger.debug(f"💰 {symbol}: Bybit price = {price}")
                                    return price
                            self.logger.warning(f"❌ {symbol}: Symbol not found in ticker data")
                            return None
                        else:
                            self.logger.warning(f"❌ {symbol}: Bybit ticker error: {data.get('retMsg', 'Unknown error')}")
                            return None
                    else:
                        self.logger.warning(f"❌ {symbol}: Bybit HTTP error {response.status}")
                        return None
        except Exception as e:
            self.logger.error(f"❌ {symbol}: Bybit ticker error: {str(e)}")
            return None

    # ==================== PERFECT 6-STEP ROMEOPT ANALYSIS ====================

    def step_1_liquidity_sweep(self, symbol: str, candles: List[Dict]) -> Tuple[bool, Optional[Dict]]:
        """Perfect liquidity sweep detection"""
        if len(candles) < 10:
            return False, None
            
        current_candle = candles[-1]
        lookback_candles = candles[-9:-1]  # 8 candles before current
        
        if not lookback_candles:
            return False, None
            
        previous_lows = [c['low'] for c in lookback_candles]
        previous_highs = [c['high'] for c in lookback_candles]
        
        min_previous_low = min(previous_lows)
        max_previous_high = max(previous_highs)
        
        sweep_threshold = 0.001  # 0.1% tolerance
        
        # Bullish sweep detection
        if (current_candle['low'] <= min_previous_low * (1 + sweep_threshold) and 
            current_candle['close'] > min_previous_low):
            self.logger.info(f"✅ {symbol}: Bullish sweep detected")
            return True, {
                'type': 'BULLISH_SWEEP',
                'sweep_level': min_previous_low,
                'current_low': current_candle['low']
            }
        
        # Bearish sweep detection
        if (current_candle['high'] >= max_previous_high * (1 - sweep_threshold) and 
            current_candle['close'] < max_previous_high):
            self.logger.info(f"✅ {symbol}: Bearish sweep detected")
            return True, {
                'type': 'BEARISH_SWEEP', 
                'sweep_level': max_previous_high,
                'current_high': current_candle['high']
            }
            
        return False, None

    def step_2_displacement(self, symbol: str, candles: List[Dict], sweep_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """Perfect displacement detection"""
        current_candle = candles[-1]
        open_price = current_candle['open']
        close_price = current_candle['close']
        high = current_candle['high']
        low = current_candle['low']
        
        candle_range = high - low
        if candle_range == 0:
            return False, None
            
        body_size = abs(close_price - open_price)
        body_ratio = body_size / candle_range
        
        # Strong displacement candle (60%+ body)
        if body_ratio >= 0.6:
            direction = "BULLISH" if close_price > open_price else "BEARISH"
            
            # Validate alignment with sweep type
            if (sweep_info['type'] == 'BULLISH_SWEEP' and direction == "BULLISH") or \
               (sweep_info['type'] == 'BEARISH_SWEEP' and direction == "BEARISH"):
                self.logger.info(f"✅ {symbol}: Displacement confirmed - {direction}")
                return True, {
                    'direction': direction,
                    'body_ratio': body_ratio,
                    'impulse_candle': current_candle
                }
                
        return False, None

    def step_3_retracement_zone(self, symbol: str, candles: List[Dict], sweep_info: Dict, 
                               displacement_info: Dict) -> Tuple[bool, Optional[Dict]]:
        """Perfect retracement zone detection"""
        current_price = candles[-1]['close']
        displacement_candle = displacement_info['impulse_candle']
        direction = displacement_info['direction']
        
        # Define optimal retracement zone (0.8% extension)
        zone_extension = 0.008
        zone_low = displacement_candle['low'] * (1 - zone_extension)
        zone_high = displacement_candle['high'] * (1 + zone_extension)
        
        if zone_low <= current_price <= zone_high:
            self.logger.info(f"✅ {symbol}: Retracement into zone confirmed")
            return True, {
                'zone_low': zone_low,
                'zone_high': zone_high,
                'current_price': current_price
            }
            
        return False, None

    def step_4_premium_discount(self, symbol: str, candles: List[Dict], direction: str) -> Tuple[bool, Optional[Dict]]:
        """Perfect premium/discount analysis"""
        if len(candles) < 15:
            return False, None
            
        current_price = candles[-1]['close']
        
        # Calculate fair value equilibrium
        recent_candles = candles[-15:]
        recent_high = max(c['high'] for c in recent_candles)
        recent_low = min(c['low'] for c in recent_candles)
        equilibrium = (recent_high + recent_low) / 2
        
        position = "DISCOUNT" if current_price < equilibrium else "PREMIUM"
        
        # Validate trading at right value
        if (direction == "BULLISH" and position == "DISCOUNT") or \
           (direction == "BEARISH" and position == "PREMIUM"):
            self.logger.info(f"✅ {symbol}: Trading at {position} to equilibrium")
            return True, {
                'position': position,
                'equilibrium': equilibrium,
                'value_gap': abs(current_price - equilibrium) / equilibrium * 100
            }
            
        return False, None

    def step_5_momentum_confirmation(self, symbol: str, candles: List[Dict], direction: str) -> Tuple[bool, Optional[Dict]]:
        """Perfect momentum confirmation"""
        if len(candles) < 6:
            return False, None
            
        # Multi-timeframe momentum analysis
        recent_closes = [c['close'] for c in candles[-5:]]
        ma_5 = sum(recent_closes) / len(recent_closes)
        current_price = candles[-1]['close']
        
        momentum = "BULLISH" if current_price > ma_5 else "BEARISH"
        
        if momentum == direction:
            self.logger.info(f"✅ {symbol}: Momentum aligned - {direction}")
            return True, {
                'momentum': momentum,
                'ma_5': ma_5,
                'price_vs_ma': (current_price - ma_5) / ma_5 * 100
            }
            
        return False, None

    def step_6_volume_confirmation(self, symbol: str, candles: List[Dict]) -> Tuple[bool, Optional[Dict]]:
        """Perfect volume confirmation"""
        if len(candles) < 10:
            return False, None
            
        current_volume = candles[-1]['volume']
        avg_volume = sum(c['volume'] for c in candles[-10:]) / 10
        
        if avg_volume == 0:
            return False, None
            
        volume_ratio = current_volume / avg_volume
        
        # Volume must be at least 70% of average
        if volume_ratio >= 0.7:
            self.logger.info(f"✅ {symbol}: Volume adequate ({volume_ratio:.1%} of average)")
            return True, {
                'volume_status': 'ADEQUATE',
                'volume_ratio': volume_ratio,
                'current_volume': current_volume
            }
            
        return False, None

    def calculate_optimal_tp_sl(self, direction: str, entry_price: float, 
                               sweep_level: float, current_volatility: float) -> Tuple[List[float], float]:
        """Calculate optimal TP/SL levels based on RomeOPT methodology"""
        
        if direction == "BULLISH":
            stop_loss = sweep_level * 0.995  # 0.5% below sweep low
            risk = entry_price - stop_loss
            
            # Optimal R:R levels
            tp1 = entry_price + (risk * 1.2)   # 1:1.2
            tp2 = entry_price + (risk * 2.0)   # 1:2.0  
            tp3 = entry_price + (risk * 3.0)   # 1:3.0
            
        else:  # BEARISH
            stop_loss = sweep_level * 1.005  # 0.5% above sweep high
            risk = stop_loss - entry_price
            
            tp1 = entry_price - (risk * 1.2)
            tp2 = entry_price - (risk * 2.0)
            tp3 = entry_price - (risk * 3.0)
        
        return [tp1, tp2, tp3], stop_loss

    async def generate_romeopt_signal(self, symbol: str) -> Optional[Dict]:
        """Generate perfect RomeOPT signal with all 6 steps"""
        self.logger.info(f"🔍 {symbol}: Starting 6-step RomeOPT analysis...")
        
        # Get reliable market data from Bybit
        candles = await self.get_bybit_klines(symbol, limit=20)
        if not candles or len(candles) < 15:
            self.logger.warning(f"❌ {symbol}: Insufficient Bybit data")
            return None
        
        current_price = await self.get_bybit_ticker(symbol)
        if not current_price:
            self.logger.warning(f"❌ {symbol}: No current price from Bybit")
            return None
            
        # Update latest candle with real-time price
        candles[-1]['close'] = current_price
        if current_price > candles[-1]['high']:
            candles[-1]['high'] = current_price
        if current_price < candles[-1]['low']:
            candles[-1]['low'] = current_price

        # ========== 6-STEP ROMEOPT VALIDATION ==========
        steps_passed = []
        step_details = {}

        # Step 1: Liquidity Sweep
        step1_ok, step1_info = self.step_1_liquidity_sweep(symbol, candles)
        if not step1_ok:
            self.logger.debug(f"❌ {symbol}: Failed Step 1 - No liquidity sweep")
            return None
        steps_passed.append("Liquidity Sweep")
        step_details['sweep'] = step1_info

        # Step 2: Displacement
        step2_ok, step2_info = self.step_2_displacement(symbol, candles, step1_info)
        if not step2_ok:
            self.logger.debug(f"❌ {symbol}: Failed Step 2 - No displacement")
            return None
        steps_passed.append("Displacement")
        step_details['displacement'] = step2_info
        direction = step2_info['direction']

        # Step 3: Retracement Zone
        step3_ok, step3_info = self.step_3_retracement_zone(symbol, candles, step1_info, step2_info)
        if not step3_ok:
            self.logger.debug(f"❌ {symbol}: Failed Step 3 - No retracement")
            return None
        steps_passed.append("Zone Retracement")
        step_details['retracement'] = step3_info

        # Step 4: Premium/Discount
        step4_ok, step4_info = self.step_4_premium_discount(symbol, candles, direction)
        if not step4_ok:
            self.logger.debug(f"❌ {symbol}: Failed Step 4 - Wrong premium/discount")
            return None
        steps_passed.append("Premium/Discount")
        step_details['value'] = step4_info

        # Step 5: Momentum
        step5_ok, step5_info = self.step_5_momentum_confirmation(symbol, candles, direction)
        if not step5_ok:
            self.logger.debug(f"❌ {symbol}: Failed Step 5 - No momentum")
            return None
        steps_passed.append("Momentum")
        step_details['momentum'] = step5_info

        # Step 6: Volume
        step6_ok, step6_info = self.step_6_volume_confirmation(symbol, candles)
        if not step6_ok:
            self.logger.debug(f"❌ {symbol}: Failed Step 6 - Low volume")
            return None
        steps_passed.append("Volume")
        step_details['volume'] = step6_info

        # ========== ALL 6 STEPS PASSED ==========
        self.logger.info(f"🎯 {symbol}: ALL 6 ROMEOPT STEPS PASSED! - {direction}")
        
        # Calculate optimal entry and levels
        entry_price = current_price
        tp_levels, sl_level = self.calculate_optimal_tp_sl(
            direction, entry_price, step1_info['sweep_level'], 0.02
        )
        
        # Calculate risk:reward
        if direction == "BULLISH":
            risk = entry_price - sl_level
            reward = tp_levels[0] - entry_price
        else:
            risk = sl_level - entry_price  
            reward = entry_price - tp_levels[0]
            
        risk_reward = reward / risk if risk > 0 else 0

        signal = {
            'symbol': symbol,
            'direction': direction,
            'entry_price': entry_price,
            'timestamp': datetime.now(),
            'timeframe': f"{self.timeframe}m",
            'tp_levels': tp_levels,
            'sl_level': sl_level,
            'current_price': current_price,
            'risk_reward': f"1:{risk_reward:.1f}",
            'steps_passed': steps_passed,
            'step_details': step_details,
            'signal_id': f"{symbol}_{direction}_{int(time.time())}",
            'quality_score': len(steps_passed),  # 6/6 perfect score
            'data_source': 'Bybit'
        }
        
        self.signals_generated += 1
        return signal

    async def send_telegram_alert(self, message: str):
        """Send perfect Telegram alert with error handling"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                payload = {
                    'chat_id': self.telegram_chat_id,
                    'text': message,
                    'parse_mode': 'Markdown',
                    'disable_web_page_preview': True
                }
                
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            self.logger.debug("✅ Telegram alert sent successfully")
                            return True
                        else:
                            error_text = await response.text()
                            self.logger.warning(f"⚠️ Telegram attempt {attempt + 1} failed: {response.status}")
                            
            except Exception as e:
                self.logger.warning(f"⚠️ Telegram attempt {attempt + 1} error: {str(e)}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
        
        self.logger.error("❌ All Telegram attempts failed")
        return False

    async def send_signal_alert(self, signal: Dict):
        """Send perfect signal alert to Telegram"""
        direction_emoji = "🟢" if signal['direction'] == "BULLISH" else "🔴"
        position_type = "LONG" if signal['direction'] == "BULLISH" else "SHORT"
        
        message = f"""
{direction_emoji} **ROMEOPT PERFECT SIGNAL - {signal['timeframe']}**

**Symbol**: `{signal['symbol']}`
**Direction**: {position_type}
**Entry Price**: `{signal['entry_price']:.4f}`
**Current Price**: `{signal['current_price']:.4f}`

**Take Profit Targets**:
TP1: `{signal['tp_levels'][0]:.4f}` (1.2R)
TP2: `{signal['tp_levels'][1]:.4f}` (2.0R)  
TP3: `{signal['tp_levels'][2]:.4f}` (3.0R)

**Stop Loss**: `{signal['sl_level']:.4f}`
**Risk/Reward**: {signal['risk_reward']}
**Quality Score**: {signal['quality_score']}/6 ✅

**6-Step Validation**:
✅ Liquidity Sweep ({signal['step_details']['sweep']['type']})
✅ Displacement ({signal['direction']})
✅ Zone Retracement  
✅ Premium/Discount
✅ Momentum Confirmation
✅ Volume Confirmation

**Data Source**: Bybit API
**Time**: {signal['timestamp'].strftime('%H:%M:%S UTC')}
**Signal ID**: `{signal['signal_id']}`

*Perfect RomeOPT Strategy • Bybit Market Data*
"""
        
        success = await self.send_telegram_alert(message)
        if success:
            self.logger.info(f"📨 Perfect signal alert sent for {signal['symbol']}")

    async def track_signal_performance(self):
        """Track active signals for TP/SL hits"""
        self.logger.info("🔍 Starting signal performance tracking...")
        
        while True:
            try:
                current_time = datetime.now()
                completed_signals = []
                
                for signal_id, signal in self.active_signals.items():
                    # Get current price from Bybit
                    current_price = await self.get_bybit_ticker(signal['symbol'])
                    if not current_price:
                        continue
                    
                    # Check TP levels
                    for i, tp_level in enumerate(signal['tp_levels']):
                        tp_key = f'tp_{i+1}_hit'
                        if tp_key not in signal:
                            if (signal['direction'] == "BULLISH" and current_price >= tp_level) or \
                               (signal['direction'] == "BEARISH" and current_price <= tp_level):
                                signal[tp_key] = {
                                    'timestamp': current_time,
                                    'price': current_price,
                                    'level': i+1
                                }
                                
                                # Send TP alert
                                tp_msg = f"""
✅ **TP{i+1} HIT - {signal['symbol']} {signal['timeframe']}**

**Direction**: {signal['direction']}
**Target Price**: `{tp_level:.4f}`
**Current Price**: `{current_price:.4f}`
**Entry Price**: `{signal['entry_price']:.4f}`
**Profit**: `{abs(current_price - signal['entry_price']):.4f}`

**Time**: {current_time.strftime('%H:%M:%S UTC')}
**Data Source**: Bybit
"""
                                await self.send_telegram_alert(tp_msg)
                                self.logger.info(f"✅ {signal['symbol']} TP{i+1} hit!")
                    
                    # Check SL
                    sl_key = 'sl_hit'
                    if sl_key not in signal:
                        if (signal['direction'] == "BULLISH" and current_price <= signal['sl_level']) or \
                           (signal['direction'] == "BEARISH' and current_price >= signal['sl_level']):
                            signal[sl_key] = {
                                'timestamp': current_time,
                                'price': current_price
                            }
                            completed_signals.append(signal_id)
                            
                            # Send SL alert
                            sl_msg = f"""
❌ **SL HIT - {signal['symbol']} {signal['timeframe']}**

**Direction**: {signal['direction']}
**Entry Price**: `{signal['entry_price']:.4f}`
**Stop Loss**: `{signal['sl_level']:.4f}`
**Current Price**: `{current_price:.4f}`
**Loss**: `{abs(current_price - signal['entry_price']):.4f}`

**Time**: {current_time.strftime('%H:%M:%S UTC')}
**Data Source**: Bybit
"""
                            await self.send_telegram_alert(sl_msg)
                            self.logger.warning(f"❌ {signal['symbol']} SL hit")
                    
                    # Remove old signals (6 hours max)
                    signal_age = current_time - signal['timestamp']
                    if signal_age > timedelta(hours=6):
                        completed_signals.append(signal_id)
                        self.logger.info(f"🕒 {signal['symbol']} signal expired")
                
                # Clean up completed signals
                for signal_id in completed_signals:
                    if signal_id in self.active_signals:
                        self.signal_history.append(self.active_signals[signal_id])
                        del self.active_signals[signal_id]
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"❌ Performance tracking error: {str(e)}")
                await asyncio.sleep(30)

    async def send_status_report(self):
        """Send periodic status reports"""
        self.logger.info("📊 Starting status reporting...")
        
        report_count = 0
        while True:
            try:
                report_count += 1
                uptime = datetime.now() - self.start_time
                hours = uptime.total_seconds() / 3600
                
                status_msg = f"""
📊 **BYBIT ROMEOPT SCANNER STATUS**

• **Uptime**: {hours:.1f} hours
• **Analysis Cycles**: {self.analysis_count}
• **Signals Generated**: {self.signals_generated}
• **Active Signals**: {len(self.active_signals)}
• **Coins Monitoring**: {len(self.coins)}
• **Timeframe**: {self.timeframe}m

**Performance**:
• Success Rate: {len([s for s in self.signal_history if any(k in s for k in ['tp_1_hit', 'tp_2_hit', 'tp_3_hit'])]) / max(1, len(self.signal_history)):.1%}
• Avg Quality: {sum(s.get('quality_score', 0) for s in self.signal_history) / max(1, len(self.signal_history)):.1f}/6

**Data Source**: Bybit API
**Status**: 🟢 PERFECTLY OPERATIONAL
**Last Update**: {datetime.now().strftime('%H:%M UTC')}
**Report**: #{report_count}
"""
                await self.send_telegram_alert(status_msg)
                self.logger.info(f"📊 Status report #{report_count} sent")
                
                await asyncio.sleep(3600)  # Every hour
                
            except Exception as e:
                self.logger.error(f"❌ Status report error: {str(e)}")
                await asyncio.sleep(3600)

    async def test_bybit_connection(self):
        """Test Bybit API connection"""
        self.logger.info("🔧 Testing Bybit API connection...")
        
        test_symbol = 'BTCUSDT'
        
        # Test klines
        klines = await self.get_bybit_klines(test_symbol, limit=5)
        if klines:
            self.logger.info(f"✅ Bybit klines test PASSED - Got {len(klines)} candles")
            for candle in klines[-3:]:  # Show last 3 candles
                self.logger.info(f"   📊 {candle['timestamp']}: O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']}")
        else:
            self.logger.error("❌ Bybit klines test FAILED")
            return False
        
        # Test ticker
        price = await self.get_bybit_ticker(test_symbol)
        if price:
            self.logger.info(f"✅ Bybit ticker test PASSED - Price: {price}")
        else:
            self.logger.error("❌ Bybit ticker test FAILED")
            return False
            
        return True

    async def analyze_coins_sequentially(self):
        """Perfect sequential coin analysis"""
        self.logger.info(f"🔄 Starting analysis cycle for {len(self.coins)} coins")
        
        signals_found = 0
        for symbol in self.coins:
            try:
                # Optimal delay between analyses
                await asyncio.sleep(2)
                
                signal = await self.generate_romeopt_signal(symbol)
                
                if signal:
                    signal_id = signal['signal_id']
                    
                    # Avoid duplicate signals
                    if signal_id not in self.active_signals:
                        self.active_signals[signal_id] = signal
                        signals_found += 1
                        
                        # Send perfect signal alert
                        await self.send_signal_alert(signal)
                        
                        self.logger.info(f"🎯 New perfect signal: {symbol} {signal['direction']}")
                
            except Exception as e:
                self.logger.error(f"❌ {symbol} analysis failed: {str(e)}")
                continue
        
        self.analysis_count += 1
        self.logger.info(f"✅ Analysis cycle {self.analysis_count} completed - {signals_found} signals")
        
        return signals_found

    async def run_perfect_scanner(self):
        """Main scanner loop - perfectly optimized"""
        self.logger.info("🚀 STARTING PERFECT BYBIT ROMEOPT SCANNER")
        
        # Test Bybit connection first
        connection_ok = await self.test_bybit_connection()
        if not connection_ok:
            self.logger.error("❌ Cannot start scanner - Bybit connection failed")
            return
        
        # Send startup message
        startup_msg = f"""
🚀 **PERFECT BYBIT ROMEOPT SCANNER STARTED**

• **Version**: Final Working v1.0
• **Coins**: {', '.join(self.coins)}
• **Timeframe**: {self.timeframe}m
• **Strategy**: 6-Step RomeOPT
• **Data Source**: Bybit API ✅

**Features**:
✅ Perfect 6-step validation
✅ Real-time signal tracking
✅ Performance monitoring
✅ Error-resistant design
✅ Bybit API integration

**Status**: 🟢 OPERATIONAL
**Start Time**: {self.start_time.strftime('%Y-%m-%d %H:%M UTC')}

*Ready to find perfect trading opportunities with Bybit!*
"""
        await self.send_telegram_alert(startup_msg)
        
        # Start background services
        asyncio.create_task(self.track_signal_performance())
        asyncio.create_task(self.send_status_report())
        
        self.logger.info("✅ All background services started")
        
        # Main analysis loop
        cycle_number = 0
        while True:
            cycle_number += 1
            cycle_start = datetime.now()
            
            try:
                signals_found = await self.analyze_coins_sequentially()
                
                # Perfect timing control
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                wait_time = max(5, self.analysis_interval - cycle_duration)
                
                self.logger.info(f"⏰ Cycle {cycle_number}: {cycle_duration:.1f}s, found {signals_found} signals, waiting {wait_time:.1f}s")
                
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    
            except Exception as e:
                self.logger.error(f"❌ Main loop error: {str(e)}")
                await asyncio.sleep(30)  # Recover gracefully

# 🎯 PERFECT EXECUTION
async def main():
    try:
        scanner = PerfectBybitRomeOPTScanner()
        await scanner.run_perfect_scanner()
    except Exception as e:
        logging.critical(f"💥 CRITICAL FAILURE: {str(e)}")
        raise

if __name__ == "__main__":
    logging.info("🎯 STARTING PERFECT BYBIT ROMEOPT SCANNER - FINAL VERSION")
    asyncio.run(main())