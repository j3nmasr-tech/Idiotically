# signal_engine.py
import pandas as pd
import numpy as np
from utils import log, load_config
from indicators import (
    turtle_breakout,
    detect_crt,
    smc_bias,
    atr as compute_atr,   # keep name distinct if your indicator is named atr
)
# Use the TwelveData cache accessor (from data_fetch.py provided earlier)
from data_fetch import get_cached_klines, normalize_symbol
# Load config
CONFIG = load_config()
TIMEFRAMES = CONFIG.get("TIMEFRAMES", ["15m", "30m", "1h", "4h"])
INSTRUMENTS = CONFIG.get("INSTRUMENTS", [])
MIN_TF_SCORE = CONFIG.get("MIN_TF_SCORE", 60)   # keep your default
CONFIDENCE_MIN = CONFIG.get("MIN_CONFIDENCE", 60.0)


def to_df(candles):
    """Convert list of candle dicts into pandas DataFrame (oldest->newest)."""
    if candles is None:
        return None
    df = pd.DataFrame(candles)
    if "time" in df.columns:
        # Ensure datetime dtype
        df = df.sort_values("time").reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)
    # Ensure numeric types
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def analyze_instruments(_unused_api=None):
    """
    Main entry similar to your original function, but reads candles from data_fetch cache.
    Returns list of signals (same structure as your original).
    """
    signals = []

    for raw_symbol in INSTRUMENTS:
        # Normalize symbol so it matches cache keys (e.g. "EURUSD" -> "EURUSD" or "EUR/USD")
        symbol_norm = raw_symbol.replace("=X", "").replace("/", "").upper()
        log(f"Checking {raw_symbol}")

        tf_scores = {}
        tf_directions = {}
        data = {}

        # FETCH CACHED DATA PER TIMEFRAME (no HTTP calls here)
        for tf in TIMEFRAMES:
            candles = get_cached_klines(symbol_norm, tf)
            if candles is None:
                log(f"Failed to fetch {raw_symbol} {tf} (cached)")
                continue

            df = to_df(candles)
            if df is None or len(df) < 60:
                log(f"Not enough data {raw_symbol} {tf} (have {0 if df is None else len(df)})")
                continue

            data[tf] = df

        # If any timeframe missing → skip symbol (same behaviour as original)
        if len(data) < len(TIMEFRAMES):
            continue

        # PER-TF ANALYSIS (same scoring logic you used)
        for tf, df in data.items():
            try:
                crt_b, crt_s = detect_crt(df)
            except Exception as e:
                log(f"detect_crt error {raw_symbol} {tf}: {e}")
                crt_b, crt_s = False, False

            try:
                tb_b, tb_s = turtle_breakout(df)
            except Exception as e:
                log(f"turtle_breakout error {raw_symbol} {tf}: {e}")
                tb_b, tb_s = False, False

            try:
                bias = smc_bias(df)
            except Exception as e:
                log(f"smc_bias error {raw_symbol} {tf}: {e}")
                bias = "bear"

            vol = True  # Forex tick volume OK (keep your prior assumption)

            # Bull score
            bull_score = (
                0.40 * (1 if bias == "bull" else 0) +
                0.25 * (1 if tb_b else 0) +
                0.20 * (1 if crt_b else 0) +
                0.15 * (1 if vol else 0)
            ) * 100

            # Bear score
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

        # STRICT-BALANCED: 2 of 3 TF must agree (same as before)
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

        # ENTRY PRICE (highest TF)
        high_tf = TIMEFRAMES[-1]                 # e.g. "4h"
        entry = float(data[high_tf]["close"].iloc[-1])

        # ATR from 1h timeframe (fallback to first TF if not present)
        atr_tf = "1h" if "1h" in data else TIMEFRAMES[0]
        try:
            the_atr = compute_atr(data[atr_tf])
            if the_atr is None or the_atr <= 0:
                # fallback: simple average true range
                tr = (data[atr_tf]["high"] - data[atr_tf]["low"]).rolling(14).mean().iloc[-1]
                the_atr = float(tr) if not np.isnan(tr) else (entry * 0.001)
        except Exception as e:
            log(f"ATR compute error {raw_symbol} {atr_tf}: {e}")
            the_atr = entry * 0.001

        # TP / SL (same multipliers you used)
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

        # Confidence (average of per-tf scores)
        confidence = int(sum(tf_scores.values()) / len(tf_scores)) if tf_scores else 0

        # Safety: require min confidence
        if confidence < CONFIDENCE_MIN:
            log(f"Skipping {raw_symbol}: confidence {confidence}% < {CONFIDENCE_MIN}%")
            continue

        # BUILD SIGNAL
        signals.append({
            "symbol": raw_symbol,
            "direction": direction,
            "entry": round(entry, 5),
            "sl": round(sl, 5),
            "tp1": round(tp1, 5),
            "tp2": round(tp2, 5),
            "tp3": round(tp3, 5),
            "confidence": confidence,
            "tf_reason": str(tf_directions)
        })

    return signals