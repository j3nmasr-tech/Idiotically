import os
import asyncio
import aiohttp
import logging
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bingx_fixed_scanner.log'),
        logging.StreamHandler()
    ]
)

class BingXFixedScanner:
    def __init__(self):
        logging.info("🚀 INITIALIZING BINGX FIXED SCANNER")
        
        # Load credentials
        self.api_key = os.getenv('BINGX_API_KEY')
        self.api_secret = os.getenv('BINGX_API_SECRET')
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Validate
        if not all([self.api_key, self.api_secret, self.telegram_token, self.telegram_chat_id]):
            raise ValueError("Missing environment variables")
        
        # Realistic configuration
        self.coins = ['BTC-USDT', 'ETH-USDT', 'BNB-USDT']  # Start with 3 coins
        self.timeframe = '5m'
        self.analysis_interval = 60  # 60 seconds between cycles
        
        # BingX API endpoints - CORRECTED
        self.base_url = "https://open-api.bingx.com"
        
        # State
        self.active_signals = {}
        
        logging.info("✅ BINGX FIXED SCANNER READY")

    async def bingx_public_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make public BingX API request"""
        try:
            url = f"{self.base_url}{endpoint}"
            
            if params:
                query_string = urllib.parse.urlencode(params)
                url = f"{url}?{query_string}"
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        logging.debug(f"📡 API Response: {data}")
                        return data
                    else:
                        logging.error(f"❌ API Error {response.status}: {await response.text()}")
                        return None
        except Exception as e:
            logging.error(f"❌ API Request failed: {e}")
            return None

    async def get_bingx_klines_fixed(self, symbol: str, limit: int = 20) -> Optional[List[Dict]]:
        """Get klines from BingX - FIXED ENDPOINT"""
        try:
            # Remove dash for BingX symbol format
            bingx_symbol = symbol.replace('-', '')
            
            params = {
                'symbol': bingx_symbol,
                'interval': self.timeframe.upper(),  # BingX uses uppercase
                'limit': limit
            }
            
            logging.info(f"📊 Fetching klines for {symbol} -> {bingx_symbol}")
            
            data = await self.bingx_public_request('/openApi/swap/v3/quote/klines', params)
            
            if data and data.get('code') == 0 and 'data' in data:
                candles = []
                for candle in data['data']:
                    candles.append({
                        'timestamp': datetime.fromtimestamp(candle['time'] / 1000),
                        'open': float(candle['open']),
                        'high': float(candle['high']),
                        'low': float(candle['low']),
                        'close': float(candle['close']),
                        'volume': float(candle['volume']),
                        'is_closed': True
                    })
                
                logging.info(f"✅ {symbol}: Got {len(candles)} REAL candles")
                return candles
            else:
                logging.warning(f"❌ {symbol}: No data or API error: {data}")
                return None
                
        except Exception as e:
            logging.error(f"❌ {symbol}: Klines failed: {e}")
            return None

    async def get_bingx_ticker_fixed(self, symbol: str) -> Optional[float]:
        """Get current price from BingX - FIXED ENDPOINT"""
        try:
            # Remove dash for BingX symbol format
            bingx_symbol = symbol.replace('-', '')
            
            params = {'symbol': bingx_symbol}
            
            data = await self.bingx_public_request('/openApi/swap/v2/quote/ticker', params)
            
            if data and data.get('code') == 0 and 'data' in data:
                for ticker in data['data']:
                    if ticker['symbol'] == bingx_symbol:
                        price = float(ticker['lastPrice'])
                        logging.info(f"💰 {symbol}: Current price = {price}")
                        return price
                
                logging.warning(f"❌ {symbol}: Symbol not found in ticker data")
                return None
            else:
                logging.warning(f"❌ {symbol}: No ticker data: {data}")
                return None
                
        except Exception as e:
            logging.error(f"❌ {symbol}: Ticker failed: {e}")
            return None

    async def test_bingx_connection(self):
        """Test if we can connect to BingX API"""
        logging.info("🔧 Testing BingX API connection...")
        
        # Test with BTC-USDT
        test_symbol = 'BTC-USDT'
        
        # Test klines
        klines = await self.get_bingx_klines_fixed(test_symbol, limit=5)
        if klines:
            logging.info(f"✅ Klines test PASSED - Got {len(klines)} candles")
            for candle in klines:
                logging.info(f"   📊 {candle['timestamp']}: O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']}")
        else:
            logging.error("❌ Klines test FAILED")
            return False
        
        # Test ticker
        price = await self.get_bingx_ticker_fixed(test_symbol)
        if price:
            logging.info(f"✅ Ticker test PASSED - Price: {price}")
        else:
            logging.error("❌ Ticker test FAILED")
            return False
            
        return True

    # Simplified 6-step analysis for testing
    async def generate_test_signal(self, symbol: str) -> Optional[Dict]:
        """Generate test signal with real data"""
        logging.info(f"🔍 {symbol}: Testing with real data...")
        
        # Get real data
        candles = await self.get_bingx_klines_fixed(symbol, limit=15)
        if not candles:
            return None
            
        current_price = await self.get_bingx_ticker_fixed(symbol)
        if not current_price:
            return None
        
        # Simple signal based on price action
        recent_closes = [c['close'] for c in candles[-5:]]
        price_trend = "BULLISH" if recent_closes[-1] > recent_closes[0] else "BEARISH"
        
        signal = {
            'symbol': symbol,
            'direction': price_trend,
            'entry_price': current_price,
            'timestamp': datetime.now(),
            'timeframe': self.timeframe,
            'current_price': current_price,
            'price_change': f"{((recent_closes[-1] - recent_closes[0]) / recent_closes[0] * 100):.2f}%"
        }
        
        return signal

    async def send_telegram_alert(self, message: str):
        """Send alert to Telegram"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logging.info("✅ Telegram alert sent")
                    else:
                        logging.error(f"❌ Telegram failed: {response.status}")
        except Exception as e:
            logging.error(f"❌ Telegram error: {e}")

    async def run_diagnostic(self):
        """Run complete diagnostic"""
        logging.info("🩺 RUNNING BINGX DIAGNOSTIC...")
        
        # Test API connection
        api_ok = await self.test_bingx_connection()
        if not api_ok:
            error_msg = """
❌ **BINGX DIAGNOSTIC FAILED**

**Issue**: Cannot connect to BingX API
**Possible Causes**:
• Incorrect API endpoints
• Symbol format issues  
• BingX API maintenance
• Network connectivity

**Action**: Check BingX API documentation for correct endpoints
"""
            await self.send_telegram_alert(error_msg)
            return False
        
        # Test all coins
        results = {}
        for symbol in self.coins:
            logging.info(f"🔍 Testing {symbol}...")
            candles = await self.get_bingx_klines_fixed(symbol, limit=3)
            price = await self.get_bingx_ticker_fixed(symbol)
            
            results[symbol] = {
                'klines': bool(candles),
                'price': bool(price),
                'candle_count': len(candles) if candles else 0,
                'current_price': price
            }
            
            await asyncio.sleep(1)  # Rate limiting
        
        # Report results
        success_count = sum(1 for r in results.values() if r['klines'] and r['price'])
        
        diagnostic_msg = f"""
🩺 **BINGX DIAGNOSTIC RESULTS**

**API Connection**: ✅ SUCCESS
**Coins Tested**: {len(self.coins)}
**Successful**: {success_count}/{len(self.coins)}

**Detailed Results**:
"""
        
        for symbol, result in results.items():
            status = "✅" if result['klines'] and result['price'] else "❌"
            diagnostic_msg += f"• {symbol}: {status} "
            diagnostic_msg += f"({result['candle_count']} candles, "
            diagnostic_msg += f"${result['current_price'] if result['current_price'] else 'N/A'})\n"
        
        if success_count > 0:
            diagnostic_msg += "\n**Status**: 🟢 READY FOR TRADING"
        else:
            diagnostic_msg += "\n**Status**: 🔴 CHECK API ENDPOINTS"
        
        await self.send_telegram_alert(diagnostic_msg)
        logging.info("📊 Diagnostic completed")
        
        return success_count > 0

    async def start_fixed_scanner(self):
        """Start the fixed scanner"""
        logging.info("🚀 STARTING FIXED BINGX SCANNER")
        
        # Send startup message
        await self.send_telegram_alert(f"""
🚀 **BINGX SCANNER STARTED**

• **Coins**: {', '.join(self.coins)}
• **Timeframe**: {self.timeframe}
• **Mode**: DIAGNOSTIC + REAL DATA
• **Start Time**: {datetime.now().strftime('%H:%M UTC')}

**Status**: Running diagnostics...
""")
        
        # Run diagnostic first
        diagnostic_ok = await self.run_diagnostic()
        
        if not diagnostic_ok:
            logging.error("❌ Scanner cannot start - diagnostic failed")
            return
        
        # If diagnostic passed, start real analysis
        await self.send_telegram_alert("✅ **DIAGNOSTIC PASSED** - Starting real analysis...")
        
        cycle_count = 0
        while True:
            cycle_count += 1
            logging.info(f"🔄 Analysis cycle {cycle_count}")
            
            signals_found = 0
            for symbol in self.coins:
                try:
                    await asyncio.sleep(2)  # Rate limiting
                    
                    signal = await self.generate_test_signal(symbol)
                    if signal:
                        signals_found += 1
                        logging.info(f"🎯 {symbol}: Test signal - {signal['direction']} - Price: {signal['current_price']}")
                        
                        # Send simple alert for testing
                        alert_msg = f"""
📊 **TEST SIGNAL - {symbol}**

**Direction**: {signal['direction']}
**Price**: {signal['current_price']}
**Change**: {signal['price_change']}
**Time**: {signal['timestamp'].strftime('%H:%M UTC')}

*This is a test signal with real data*
"""
                        await self.send_telegram_alert(alert_msg)
                        
                except Exception as e:
                    logging.error(f"❌ {symbol}: Analysis failed: {e}")
                    continue
            
            logging.info(f"✅ Cycle {cycle_count} completed - {signals_found} signals")
            
            # Wait for next cycle
            await asyncio.sleep(self.analysis_interval)

# Run the fixed scanner
async def main():
    scanner = BingXFixedScanner()
    await scanner.start_fixed_scanner()

if __name__ == "__main__":
    logging.info("🎯 STARTING BINGX FIXED SCANNER - DIAGNOSTIC MODE")
    asyncio.run(main())