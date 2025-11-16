import requests
import datetime
import time

# ============================================================
# Yahoo Timeframe Mapping
# ============================================================
TF_MAP = {
    "15m":  "15m",
    "30m":  "30m",
    "1h":   "60m",
    "4h":   "240m",
    "1d":   "1d"
}

# Yahoo headers required (very important)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Connection": "keep-alive"
}


def fetch_ohlc_yahoo(symbol: str, tf: str, limit=200, retries=3):
    """
    Stable Yahoo Finance OHLC fetcher for Forex.
    Returns list of {time, open, high, low, close, volume}
    """

    # ----------------------------------------------------------
    # Validate timeframe
    # ----------------------------------------------------------
    if tf not in TF_MAP:
        print(f"[Yahoo] Unsupported TF: {tf}")
        return None

    interval = TF_MAP[tf]

    # ----------------------------------------------------------
    # Clean symbol formatting: EURUSD=X (not EUR/USD)
    # ----------------------------------------------------------
    if symbol.endswith("=X"):
        yahoo_symbol = symbol
    else:
        yahoo_symbol = symbol.replace("/", "") + "=X"

    # ----------------------------------------------------------
    # Build Yahoo request URL
    # ----------------------------------------------------------
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{yahoo_symbol}"
        f"?interval={interval}&range=60d"
        f"&includePrePost=false&events=history"
    )

    # ----------------------------------------------------------
    # Retry logic (Yahoo sometimes fails randomly)
    # ----------------------------------------------------------
    for attempt in range(retries):

        try:
            r = requests.get(url, headers=HEADERS, timeout=12)

            if r.status_code != 200:
                print(f"[Yahoo] HTTP {r.status_code} {symbol} {tf}")
                time.sleep(0.5)
                continue

            # Yahoo sometimes returns HTML instead of JSON
            if not r.text.strip().startswith("{"):
                print(f"[Yahoo] Non-JSON response {symbol} {tf}")
                time.sleep(0.5)
                continue

            data = r.json()

        except Exception as e:
            print(f"[Yahoo] Error {symbol} {tf}: {e}")
            time.sleep(0.5)
            continue

        # ------------------------------------------------------
        # Validate JSON structure
        # ------------------------------------------------------
        try:
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            indicators = result["indicators"]["quote"][0]

        except Exception:
            print(f"[Yahoo] Invalid JSON format for {symbol} {tf}")
            continue

        if not timestamps:
            print(f"[Yahoo] Empty timestamps {symbol} {tf}")
            time.sleep(0.5)
            continue

        # ------------------------------------------------------
        # Build candles
        # ------------------------------------------------------
        candles = []
        for i in range(len(timestamps)):
            o = indicators["open"][i]
            h = indicators["high"][i]
            l = indicators["low"][i]
            c = indicators["close"][i]

            # Skip missing OHLC rows
            if None in (o, h, l, c):
                continue

            candles.append({
                "time": datetime.datetime.fromtimestamp(timestamps[i]),
                "open": o,
                "high": h,
                "low":  l,
                "close": c,
                "volume": indicators.get("volume", [0])[i] if "volume" in indicators else 0
            })

        if len(candles) < 50:
            print(f"[Yahoo] Not enough data {symbol} {tf} ({len(candles)} candles)")
            return None

        return candles[-limit:]  # success

    # ----------------------------------------------------------
    # All retries failed
    # ----------------------------------------------------------
    print(f"[Yahoo] TOTAL FAILURE {symbol} {tf}")
    return None