import requests
import csv
from datetime import datetime

def get_klines(symbol, tf):
    tf_map = {
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "4h": "240",
        "1d": "d"
    }

    interval = tf_map[tf]

    # Stooq uses lowercase symbols without slash
    stooq_symbol = symbol.lower().replace("/", "")

    url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i={interval}"

    r = requests.get(url)
    if r.status_code != 200:
        print(f"Error fetching {symbol} {tf}: HTTP {r.status_code}")
        return None

    lines = r.text.strip().split("\n")
    reader = csv.DictReader(lines)

    candles = []
    for row in reader:
        try:
            candles.append({
                "time": datetime.strptime(row["Date"], "%Y-%m-%d"),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row.get("Volume", 0) or 0)
            })
        except:
            continue

    if len(candles) < 20:
        print(f"Not enough data for {symbol} {tf}")
        return None

    return candles
