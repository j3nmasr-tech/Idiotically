import requests
import datetime


# Yahoo valid interval mapping
TF_MAP = {
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "4h": "240m"
}


def fetch_ohlc_yahoo(symbol: str, tf: str, limit=200):
    """Fetch OHLC candles from Yahoo Finance (forex-safe)."""

    if tf not in TF_MAP:
        print(f"[Yahoo] Unsupported TF: {tf}")
        return None

    interval = TF_MAP[tf]

    # Yahoo expects symbol exactly like EURUSD=X (already in config)
    yahoo_symbol = symbol

    # Build correct URL
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        f"?interval={interval}&range=30d"
    )

    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"[Yahoo] HTTP {r.status_code} {yahoo_symbol} {tf}")
            return None

        data = r.json()
    except Exception as e:
        print(f"[Yahoo] Fetch error {yahoo_symbol} {tf}: {e}")
        return None

    # Validate JSON
    try:
        result = data["chart"]["result"][0]
    except:
        print(f"[Yahoo] No result for {yahoo_symbol} {tf}")
        return None

    try:
        timestamps = result["timestamp"]
        indicators = result["indicators"]["quote"][0]
    except:
        print(f"[Yahoo] Missing fields for {yahoo_symbol} {tf}")
        return None

    candles = []
    for i in range(len(timestamps)):
        o = indicators["open"][i]
        h = indicators["high"][i]
        l = indicators["low"][i]
        c = indicators["close"][i]

        if None in (o, h, l, c):
            continue

        candles.append({
            "time": datetime.datetime.fromtimestamp(timestamps[i]),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": indicators.get("volume", [0])[i]
        })

    if len(candles) < 20:
        print(f"[Yahoo] Not enough data for {yahoo_symbol} {tf}")
        return None

    return candles[-limit:]