import requests
import datetime

# Yahoo interval mapping
TF_MAP = {
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "4h": "240m",
    "1d": "1d"
}

# Browser headers to bypass Yahoo blocking
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json,text/javascript,*/*;q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"
}


def fetch_ohlc_yahoo(symbol: str, tf: str, limit=200):
    """
    Fetches OHLC from Yahoo Finance for Forex pairs.
    """

    if tf not in TF_MAP:
        print(f"[Yahoo] Unsupported TF: {tf}")
        return None

    yahoo_symbol = symbol.replace("/", "") + "=X"   # EURUSD -> EURUSD=X
    interval = TF_MAP[tf]

    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{yahoo_symbol}?interval={interval}&range=60d"
    )

    # ----- Make request with browser headers -----
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()
    except Exception as e:
        print(f"[Yahoo] Failed {symbol} {tf}: {e}")
        return None

    # ----- Parse response -----
    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        indicators = result["indicators"]["quote"][0]
    except Exception:
        print(f"[Yahoo] No data for {symbol} {tf}")
        return None

    candles = []
    for i in range(len(timestamps)):
        o = indicators["open"][i]
        h = indicators["high"][i]
        l = indicators["low"][i]
        c = indicators["close"][i]

        if o is None or h is None or l is None or c is None:
            continue

        candles.append({
            "time": datetime.datetime.fromtimestamp(timestamps[i]),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": indicators.get("volume", [0])[i] if "volume" in indicators else 0
        })

    if len(candles) < 50:
        print(f"[Yahoo] Not enough data for {symbol} {tf}")
        return None

    return candles[-limit:]