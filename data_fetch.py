import os
import time
import requests
from datetime import datetime, timedelta

# ============================================
# CONFIG
# ============================================

API_KEY = os.getenv("TWELVEDATA_KEY")
BASE_URL = "https://api.twelvedata.com/time_series"

# cache expires after 12 minutes (720 seconds)
CACHE_TTL = 720

# global dict → { "XAUUSD_15m": candle_data }
LATEST_DATA = {}
LAST_UPDATE = {}


# ============================================
# INTERNAL UTILS
# ============================================

def now_ts():
    return int(time.time())


def cache_key(symbol: str, timeframe: str):
    return f"{symbol}_{timeframe}"


def cache_expired(symbol: str, timeframe: str):
    key = cache_key(symbol, timeframe)
    last = LAST_UPDATE.get(key, 0)
    return (now_ts() - last) > CACHE_TTL


# ============================================
# FETCH FROM API
# ============================================

def fetch_from_api(symbol: str, timeframe: str, limit: int = 200):
    """Fetch klines directly from TwelveData"""
    if not API_KEY:
        print("[TD] ERROR → TWELVEDATA_KEY missing in environment!")
        return None

    params = {
        "symbol": symbol,
        "interval": timeframe,
        "apikey": API_KEY,
        "outputsize": limit,
        "format": "JSON"
    }

    try:
        r = requests.get(BASE_URL, params=params, timeout=10)
        data = r.json()

        if "status" in data and data["status"] == "error":
            print(f"[TD] API ERROR {symbol} {timeframe}: {data.get('message')}")
            return None

        if "values" not in data:
            print(f"[TD] INVALID RESPONSE for {symbol} {timeframe}")
            return None

        return data["values"]

    except Exception as e:
        print(f"[TD] REQUEST ERROR {symbol} {timeframe}: {e}")
        return None


# ============================================
# CACHE HANDLING
# ============================================

def update_cache(symbol: str, timeframe: str):
    """Force refresh cache for one symbol/timeframe"""
    print(f"[TD] Fetching {symbol} {timeframe} (refresh)...")

    data = fetch_from_api(symbol, timeframe)
    if data:
        key = cache_key(symbol, timeframe)
        LATEST_DATA[key] = data
        LAST_UPDATE[key] = now_ts()
        return True

    return False



def get_cached_klines(symbol: str, timeframe: str):
    """
    Returns:
      - cached data (if fresh)
      - OR fetches and returns new data
      - OR None on failure
    """
    key = cache_key(symbol, timeframe)

    # If we HAVE cache and not expired → return it
    if key in LATEST_DATA and not cache_expired(symbol, timeframe):
        return LATEST_DATA[key]

    # Otherwise → try refreshing from API
    ok = update_cache(symbol, timeframe)
    if ok:
        return LATEST_DATA.get(key)

    return None



# ============================================
# MASS CACHE UPDATING
# ============================================

def update_cache_for_tf(symbols: list, timeframe: str):
    """Refresh cache for multiple symbols for ONE timeframe"""
    print(f"[TD] Updating timeframe: {timeframe}")

    for sym in symbols:
        ok = update_cache(sym, timeframe)
        if not ok:
            print(f"[TD] FAILED {sym} {timeframe}")

    return True



def update_cache_all_timeframes(symbols: list, timeframes: list):
    """
    PUBLIC FUNCTION  
    Refreshes entire cache for ALL symbols + ALL TFs  
    Called automatically at bot start
    """
    print("[TD] Updating ALL timeframes...")

    for tf in timeframes:
        update_cache_for_tf(symbols, tf)

    print("[TD] Cache update complete.")



# ============================================
# DEBUG
# ============================================

def debug_cache_summary():
    print("=== CACHE SUMMARY ===")
    for k in LATEST_DATA.keys():
        age = now_ts() - LAST_UPDATE.get(k, 0)
        print(f"{k} → {age}s old")
    print("=====================")