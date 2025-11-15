import pandas as pd

def atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs()
    ], axis=1).max(axis=1)

    return tr.rolling(period).mean().iloc[-1]


def turtle_breakout(df, period=20):
    high = df["high"].rolling(period).max().iloc[-2]
    low = df["low"].rolling(period).min().iloc[-2]
    close = df["close"].iloc[-1]

    return close > high, close < low


def detect_crt(df):
    last = df.iloc[-1]
    prev = df.iloc[-2]
    wick_top = last["high"] - max(last["open"], last["close"])
    wick_bottom = min(last["open"], last["close"]) - last["low"]
    body = abs(last["close"] - last["open"])
    return wick_bottom > body and wick_bottom > wick_top, wick_top > body and wick_top > wick_bottom


def smc_bias(df):
    closes = df["close"].values
    if closes[-1] > closes[-5]:
        return "bull"
    elif closes[-1] < closes[-5]:
        return "bear"
    return "neutral"
