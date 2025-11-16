# data_fetch.py
import os
import time
import requests
import datetime
from typing import List, Dict, Optional

# Get API key from environment (Northflank secret)
API_KEY = os.getenv("TWELVEDATA_KEY")
BASE_URL = "https://api.twelvedata.com/time_series"

# map internal TF -> TwelveData interval
TF_MAP = {
    "15m": "15min",
    "30m": "30min",
    "1h":  "1h",
    "4h":  "4h",
    "1d":  "1day"
}

# module-level cache: { tf: { symbol_norm: [candles...] } }
# candle format: {"time": datetime, "open": float, "high": float, "low": float, "close": float, "volume": float}
LATEST_DATA: Dict[str, Dict[str, List[Dict]]] = {}

# helper: normalize symbol to TwelveData expected format
def normalize_symbol(sym: str) -> str:
    if not isinstance(sym, str):
        return sym
    s = sym.strip().upper()
    # Accept "EURUSD" or "EUR/USD" -> convert to "EUR/USD" (TwelveData accepts both, we use slash)
    if "/" in s:
        return s.replace("=X", "").replace("//", "/")
    if s.endswith("=X"):
        s = s[:-2]
    # insert slash between first 3 and last 3 (works for FX and XAUUSD)
    if len(s) in (6,5) and "/" not in s:
        # e.g. EURUSD -> EUR/USD or XAUUSD -> XAU/USD (XAU is 3)
        if len(s) == 6:
            return f"{s[:3]}/{s[3:]}"
        else:
            return s
    return s

# Build list string for API (comma separated)
def join_symbols_for_api(symbols: List[str]) -> str:
    # normalized symbols for the API (TwelveData accepts "EUR/USD" or "EURUSD", we'll use "EUR/USD")
    norm = [normalize_symbol(s) for s in symbols]
    return ",".join(norm)

# Perform a batch request for many symbols at once for a single timeframe
def fetch_batch(symbols: List[str], tf: str, outputsize: int = 200, timeout: int = 12) -> Optional[Dict[str, List[Dict]]]:
    """
    Returns dict: { normalized_symbol: [candles oldest->newest] } or None on error.
    """
    if API_KEY is None:
        print("[TD] TWELVEDATA_KEY missing in environment.")
        return None

    interval = TF_MAP.get(tf)
    if interval is None:
        print(f"[TD] Unsupported timeframe: {tf}")
        return None

    if not symbols:
        return {}

    # TwelveData accepts comma-separated symbols for batch requests
    symbol_param = join_symbols_for_api(symbols)

    params = {
        "symbol": symbol_param,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": API_KEY,
        "format": "JSON"
    }

    try:
        r = requests.get(BASE_URL, params=params, timeout=timeout)
    except Exception as e:
        print(f"[TD] Request error for {tf}: {e}")
        return None

    if r.status_code == 429:
        print(f"[TD] RATE LIMITED (429) for {tf} - sleeping 5s")
        time.sleep(5)
        return None

    if r.status_code != 200:
        print(f"[TD] HTTP {r.status_code} for {tf} - {r.text[:200]}")
        return None

    try:
        data = r.json()
    except Exception as e:
        print(f"[TD] JSON decode error for {tf}: {e}")
        return None

    # If error (single message) - e.g. invalid symbol
    if data.get("status") == "error":
        print(f"[TD] API ERROR: {data.get('message')}")
        return None

    # When batching, TwelveData returns keys for each symbol. Structure example:
    # { "EUR/USD": { "meta":{...}, "values":[{...}, ...] }, "GBP/USD": { ... } }
    result = {}
    # Two cases:
    # 1) single symbol request -> data has "values"
    # 2) batch -> keys are symbols, each has "values"
    if "values" in data:
        # single-symbol response (we still map it)
        symbol_norm = normalize_symbol(symbols[0])
        values = data["values"]
        result[symbol_norm] = parse_td_values(values)
        return result

    # otherwise parse keys
    for key, val in data.items():
        # skip meta/status fields
        if key in ("status", "meta", "message", "name"):
            continue
        try:
            # The batch returns for each key a dict with 'values'
            values = val.get("values")
            if not values:
                # sometimes val itself is the values list (edge cases), attempt to parse
                if isinstance(val, list):
                    values = val
                else:
                    values = []
            if values:
                symbol_norm = key
                result[symbol_norm] = parse_td_values(values)
        except Exception as e:
            print(f"[TD] parse error for {key} {e}")
            continue

    return result

def parse_td_values(values_list: List[Dict]) -> List[Dict]:
    """Convert TwelveData 'values' (descending order newest->oldest) into list oldest->newest of candles."""
    candles = []
    # TwelveData returns newest->oldest; we want oldest->newest
    for item in reversed(values_list):
        try:
            dt = item.get("datetime") or item.get("timestamp")
            # datetime is ISO string like '2025-11-16 09:00:00' or '2025-11-16T09:00:00'
            if isinstance(dt, (int, float)):
                ts = int(dt)
                ts_dt = datetime.datetime.utcfromtimestamp(ts)
            else:
                ts_dt = datetime.datetime.fromisoformat(dt.replace(" ", "T"))
            candles.append({
                "time": ts_dt,
                "open": float(item.get("open", 0.0)),
                "high": float(item.get("high", 0.0)),
                "low": float(item.get("low", 0.0)),
                "close": float(item.get("close", 0.0)),
                "volume": float(item.get("volume", 0.0)) if item.get("volume") is not None else 0.0
            })
        except Exception:
            continue
    return candles

# PUBLIC: update cache for a given timeframe and full symbol list
def update_cache_for_tf(symbols: List[str], tf: str, outputsize: int = 200) -> bool:
    """
    Fetches batch for given symbols and stores them in LATEST_DATA[tf].
    Returns True on success, False on failure.
    """
    print(f"[TD] Fetching {len(symbols)} symbols for {tf} ...")
    batch = fetch_batch(symbols, tf, outputsize=outputsize)
    if not batch:
        print(f"[TD] Batch fetch failed for {tf}")
        return False

    # initialize tf cache if needed
    if tf not in LATEST_DATA:
        LATEST_DATA[tf] = {}

    # store normalized keys & ensure normalized symbol names (strip exchanges)
    for key, candles in batch.items():
        # normalize key into form like "EUR/USD" -> also provide alias without slash for easy lookup
        k_norm = key.upper().replace(" ", "")
        LATEST_DATA[tf][k_norm] = candles

        # also store without slash for convenience (EURUSD)
        k_noslash = k_norm.replace("/", "")
        LATEST_DATA[tf][k_noslash] = candles

    print(f"[TD] Cache updated for {tf}: {len(LATEST_DATA[tf])} symbols cached")
    return True

# PUBLIC: get candles from cache; returns None if not found
def get_cached_klines(symbol: str, tf: str) -> Optional[List[Dict]]:
    if tf not in LATEST_DATA:
        return None
    s1 = normalize_symbol(symbol).upper()
    s2 = s1.replace("/", "")
    return LATEST_DATA[tf].get(s1) or LATEST_DATA[tf].get(s2)