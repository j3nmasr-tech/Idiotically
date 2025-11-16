# ============================================
# signal_engine.py  (Full Fixed Version)
# ============================================

import pandas as pd
import numpy as np

from utils import log, load_config
from indicators import (
    turtle_breakout,
    detect_crt,
    smc_bias,
    atr as compute_atr,
)

# Cached market data loader
from data_fetch import get_cached_klines


# ---------- CONFIG ----------
CONFIG = load_config()
TIMEFRAMES = CONFIG.get("TIMEFRAMES", ["15m", "30m", "1h", "4h"])
INSTRUMENTS = CONFIG.get("INSTRUMENTS", [])
MIN_TF_SCORE = CONFIG.get("MIN_TF_SCORE", 60)
CONFIDENCE_MIN = CONFIG.get("MIN_CONFIDENCE", 60)


# -------------------------------------------------
# Convert raw candle list → pandas DataFrame
# -------------------------------------------------
def to_df(candles):
    if not candles:
        return None

    df = pd.DataFrame(candles)

    # Ensure sorted
    if "time" in df.columns:
        df = df.sort_values("time")

    df = df.reset_index(drop=True)

    # Convert numeric fields
    for col in ("open", "high", "low", "close", "volume"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# -------------------------------------------------
# MAIN ENGINE — Reads ONLY from cache (no HTTP)
# -------------------------------------------------
def analyze_instruments(_unused_api=None):

    signals = []

    for raw_symbol in INSTRUMENTS:

        # Normalize to match your cache keys (e.g. "EURUSD")
        symbol_norm = raw_symbol.replace("=X", "").replace("/", "").upper()

        log(f"Checking {raw_symbol}...")

        data = {}
        tf_scores = {}
        tf_directions = {}

        # ------------------------------------------
        # LOAD CACHED CANDLES FOR EACH TIMEFRAME
        # ------------------------------------------
        for tf in TIMEFRAMES:

            candles = get_cached_klines(symbol_norm, tf)

            if candles is None:
                log(f"{raw_symbol} {tf} missing in cache")
                continue

            df = to_df(candles)

            if df is None or len(df) < 60:
                log(f"{raw_symbol} {tf} insufficient data ({len(df)})")
                continue

            data[tf] = df

        # If ANY TF is missing → skip (strict mode)
        if len(data) < len(TIMEFRAMES):
            continue

        # ------------------------------------------
        # PER TIMEFRAME ANALYSIS
        # ------------------------------------------
        for tf, df in data.items():

            # CRT
            try:
                crt_b, crt_s = detect_crt(df)
            except:
                crt_b, crt_s = False, False

            # Turtle
            try:
                tb_b, tb_s = turtle_breakout(df)
            except:
                tb_b, tb_s = False, False

            # SMC bias
            try:
                bias = smc_bias(df)
            except:
                bias = "bear"

            # Assume volume always OK (forex)
            vol_ok = True

            # Score calculations
            bull_score = (
                0.40 * (1 if bias == "bull" else 0) +
                0.25 * (1 if tb_b else 0) +
                0.20 * (1 if crt_b else 0) +
                0.15 * (1 if vol_ok else 0)
            ) * 100

            bear_score = (
                0.40 * (1 if bias == "bear" else 0) +
                0.25 * (1 if tb_s else 0) +
                0.20 * (1 if crt_s else 0) +
                0.15 * (1 if vol_ok else 0)
            ) * 100

            # Direction per TF
            if bull_score >= MIN_TF_SCORE:
                tf_directions[tf] = "BUY"
            elif bear_score >= MIN_TF_SCORE:
                tf_directions[tf] = "SELL"
            else:
                tf_directions[tf] = None

            tf_scores[tf] = max(bull_score, bear_score)

        # ------------------------------------------
        # STRICT-BALANCED: At least 2 of 3 TF agree
        # ------------------------------------------
        valid_dirs = [d for d in tf_directions.values() if d is not None]

        if len(valid_dirs) < 2:
            continue

        buy_count = valid_dirs.count("BUY")
        sell_count = valid_dirs.count("SELL")

        if buy_count >= 2:
            direction = "BUY"
        elif sell_count >= 2:
            direction = "SELL"
        else:
            continue

        # ------------------------------------------
        # ENTRY = highest timeframe close
        # ------------------------------------------
        high_tf = TIMEFRAMES[-1]
        entry = float(data[high_tf]["close"].iloc[-1])

        # ------------------------------------------
        # ATR = from 1H if possible
        # ------------------------------------------
        atr_tf = "1h" if "1h" in data else TIMEFRAMES[0]

        try:
            atr_val = compute_atr(data[atr_tf])
            if atr_val is None or atr_val <= 0 or np.isnan(atr_val):
                raise ValueError("Invalid ATR")
        except:
            # Fallback ATR
            tr = (data[atr_tf]["high"] - data[atr_tf]["low"]).rolling(14).mean().iloc[-1]
            atr_val = float(tr if not np.isnan(tr) else entry * 0.001)

        # ------------------------------------------
        # TP / SL generation
        # ------------------------------------------
        if direction == "BUY":
            sl = entry - atr_val * 1.5
            tp1 = entry + atr_val * 1
            tp2 = entry + atr_val * 2
            tp3 = entry + atr_val * 3
        else:
            sl = entry + atr_val * 1.5
            tp1 = entry - atr_val * 1
            tp2 = entry - atr_val * 2
            tp3 = entry - atr_val * 3

        # ------------------------------------------
        # CONFIDENCE = average TF score
        # ------------------------------------------
        confidence = int(sum(tf_scores.values()) / len(tf_scores)) if tf_scores else 0

        if confidence < CONFIDENCE_MIN:
            log(f"{raw_symbol} skipped: confidence {confidence}% < {CONFIDENCE_MIN}%")
            continue

        # ------------------------------------------
        # FINAL SIGNAL
        # ------------------------------------------
        signals.append({
            "symbol": raw_symbol,
            "direction": direction,
            "entry": round(entry, 5),
            "sl": round(sl, 5),
            "tp1": round(tp1, 5),
            "tp2": round(tp2, 5),
            "tp3": round(tp3, 5),
            "confidence": confidence,
            "tf_reason": str(tf_directions),
        })

    return signals