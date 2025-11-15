from indicators import (
    turtle_breakout,
    detect_crt,
    smc_bias,
    atr,
)
from utils import log
from config import TIMEFRAMES, INSTRUMENTS

MIN_TF_SCORE = 60

def analyze_instruments(oanda):
    signals = []
    for symbol in INSTRUMENTS:
        log(f"Checking {symbol}")

        tf_scores = {}
        tf_directions = {}

        # fetch candles for each TF
        data = {}
        for tf in TIMEFRAMES:
            candles = oanda.get_candles(symbol, tf, count=200)
            if candles is None:
                log(f"Failed to fetch {symbol} {tf}")
                continue
            data[tf] = candles

        # calculate per-TF scores
        for tf, df in data.items():
            crt_b, crt_s = detect_crt(df)
            tb_b, tb_s = turtle_breakout(df)
            bias = smc_bias(df)
            vol = True  # Forex has good tick volume

            bull_score = (
                0.40 * (1 if bias == "bull" else 0) +
                0.25 * (1 if tb_b else 0) +
                0.20 * (1 if crt_b else 0) +
                0.15 * (1 if vol else 0)
            ) * 100

            bear_score = (
                0.40 * (1 if bias == "bear" else 0) +
                0.25 * (1 if tb_s else 0) +
                0.20 * (1 if crt_s else 0) +
                0.15 * (1 if vol else 0)
            ) * 100

            if bull_score >= MIN_TF_SCORE:
                tf_directions[tf] = "BUY"
            elif bear_score >= MIN_TF_SCORE:
                tf_directions[tf] = "SELL"
            else:
                tf_directions[tf] = None

            tf_scores[tf] = max(bull_score, bear_score)

        # balanced strict = 2 of 3 agree
        valid = [d for d in tf_directions.values() if d is not None]
        if len(valid) < 2:
            continue

        buy_count = valid.count("BUY")
        sell_count = valid.count("SELL")

        if buy_count >= 2:
            direction = "BUY"
        elif sell_count >= 2:
            direction = "SELL"
        else:
            continue

        # entry price = latest close on highest TF
        high_tf = TIMEFRAMES[-1]
        entry = float(data[high_tf]["close"].iloc[-1])

        # ATR stops
        the_atr = atr(data["1H"])
        if direction == "BUY":
            sl = entry - the_atr * 1.5
            tp1 = entry + the_atr * 1.0
            tp2 = entry + the_atr * 2.0
            tp3 = entry + the_atr * 3.0
        else:
            sl = entry + the_atr * 1.5
            tp1 = entry - the_atr * 1.0
            tp2 = entry - the_atr * 2.0
            tp3 = entry - the_atr * 3.0

        signals.append({
            "symbol": symbol,
            "direction": direction,
            "entry": round(entry, 5),
            "sl": round(sl, 5),
            "tp1": round(tp1, 5),
            "tp2": round(tp2, 5),
            "tp3": round(tp3, 5),
            "confidence": int(sum(tf_scores.values()) / len(tf_scores)),
            "tf_reason": str(tf_directions)
        })

    return signals
