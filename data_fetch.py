import requests
import datetime
import time

# Yahoo interval mapping
TF_MAP = {
    "15m":  "15m",
    "30m":  "30m",
    "1h":   "60m",
    "4h":   "240m",
    "1d":   "1d"
}


def fetch_ohlc_yahoo(symbol: str, tf: str, limit=200):
    """
    Fetches OHLC from Yahoo Finance for Forex.
    Yahoo format example: EURUSD=X
    """

    if tf not in TF_MAP:
        print(f"[Yahoo] Unsupported TF: {tf}")
        return None

    yahoo_symbol = symbol.replace("/", "") + "=X"   # EURUSD -> EURUSD=X

    interval = TF_MAP[tf]

    # build URL
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{yahoo_symbol}?interval={interval}&range=60d"
    )

    try:
        r = requests.get(url, timeout=10)
        data = r.json()
    except Exception as e:
        print(f"[Yahoo] Failed {symbol} {tf}: {e}")
        return None

    try:
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        indicators = result["indicators"]["quote"][0]
    except:
        print(f"[Yahoo] No data for {symbol} {tf}")
        return None

    candles = []
    for i in range(len(timestamps)):
        candles.append({
            "time": datetime.datetime.fromtimestamp(timestamps[i]),
            "open": indicators["open"][i],
            "high": indicators["high"][i],
            "low":  indicators["low"][i],
            "close": indicators["close"][i],
            "volume": indicators["volume"][i] if "volume" in indicators else 0
        })

    # Filter out None values
    candles = [c for c in candles if None not in (c["open"], c["high"], c["low"], c["close"])]

    if len(candles) < 50:
        print(f"[Yahoo] Not enough data for {symbol} {tf}")
        return None

    return candles[-limit:]