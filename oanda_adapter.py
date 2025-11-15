import requests
import pandas as pd
import os
from utils import log

OANDA_URL = os.getenv("OANDA_URL", "https://api-fxpractice.oanda.com")
API_KEY = os.getenv("OANDA_API_KEY")
ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID")

class OandaAPI:

    def get_candles(self, symbol, timeframe, count=200):
        url = f"{OANDA_URL}/v3/instruments/{symbol}/candles"

        params = {
            "granularity": timeframe,
            "count": count,
            "price": "M"
        }

        headers = {
            "Authorization": f"Bearer {API_KEY}"
        }

        r = requests.get(url, params=params, headers=headers)

        if r.status_code != 200:
            log(f"OANDA error {symbol} {timeframe}: {r.text}")
            return None

        data = r.json()["candles"]

        rows = []
        for c in data:
            if not c["complete"]:
                continue
            rows.append([
                c["time"],
                float(c["mid"]["o"]),
                float(c["mid"]["h"]),
                float(c["mid"]["l"]),
                float(c["mid"]["c"])
            ])

        df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close"])
        return df
