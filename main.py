#!/usr/bin/env python3
# SIRTS Forex/Gold Signal Bot — MT5
# Requirements: pandas, numpy, MetaTrader5, requests
# Environment variables (Northflank secrets): BOT_TOKEN, CHAT_ID, MT5_LOGIN, MT5_PASSWORD, MT5_SERVER

import os
import time
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import csv
import MetaTrader5 as mt5

# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")

CAPITAL = 80.0
LEVERAGE = 30

CHECK_INTERVAL = 60
API_CALL_DELAY = 0.2

TIMEFRAMES = ["M15", "M30", "H1", "H4"]
WEIGHT_BIAS   = 0.40
WEIGHT_TURTLE = 0.25
WEIGHT_CRT    = 0.20
WEIGHT_VOLUME = 0.15

MIN_TF_SCORE  = 55
CONF_MIN_TFS  = 2
CONFIDENCE_MIN = 60.0

TOP_SYMBOLS = list(dict.fromkeys([
    "EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","USDCAD","NZDUSD",
    "EURGBP","EURJPY","GBPJPY","AUDJPY","CADJPY","CHFJPY","EURCHF",
    "EURAUD","XAUUSD","GBPCHF","AUDNZD","AUDCAD","NZDJPY",
    "EURCAD","USDNOK","USDSEK","USDTRY","USDSGD","EURSEK",
    "EURNOK","GBPAUD","GBPCAD","NZDCHF","XAGUSD","AUDCHF",
    "AUDSGD","CHFSGD","EURSGD","GBPNZD","EURNZD","CADCHF"
]))

LOG_CSV = "./sirts_forex_signals.csv"

# ===== STATE =====
open_trades = []
recent_signals = {}
last_heartbeat = time.time()
last_summary = time.time()

# ===== HELPERS =====
def send_message(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured:", text)
        return False
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": text}, timeout=10
        )
        return True
    except Exception as e:
        print("Telegram send error:", e)
        return False

# ===== MT5 INITIALIZATION =====
if not MT5_LOGIN or not MT5_PASSWORD or not MT5_SERVER:
    print("❌ MT5 credentials missing, exiting")
    exit(1)
try:
    login_int = int(MT5_LOGIN)
except ValueError:
    print("❌ MT5_LOGIN must be an integer")
    exit(1)

if not mt5.initialize(login=login_int, password=MT5_PASSWORD, server=MT5_SERVER):
    print("❌ MT5 initialization failed")
    exit(1)
else:
    print("✅ MT5 initialized")

# ===== DATA FUNCTIONS =====
def get_klines(symbol, timeframe="H1", n=200):
    tf_map = {"M15": mt5.TIMEFRAME_M15, "M30": mt5.TIMEFRAME_M30,
              "H1": mt5.TIMEFRAME_H1, "H4": mt5.TIMEFRAME_H4}
    rates = mt5.copy_rates_from_pos(symbol, tf_map[timeframe], 0, n)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df.rename(columns={"open":"open","high":"high","low":"low","close":"close","tick_volume":"volume"}, inplace=True)
    return df[["open","high","low","close","volume"]]

def get_price(symbol):
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
    return float(tick.ask)

# ===== INDICATORS =====
def detect_crt(df):
    if len(df) < 12:
        return False, False
    last = df.iloc[-1]
    o = float(last["open"]); h = float(last["high"]); l = float(last["low"]); c = float(last["close"]); v = float(last["volume"])
    body_series = (df["close"] - df["open"]).abs()
    avg_body = body_series.rolling(8, min_periods=6).mean().iloc[-1]
    avg_vol  = df["volume"].rolling(8, min_periods=6).mean().iloc[-1]
    if np.isnan(avg_body) or np.isnan(avg_vol):
        return False, False
    body = abs(c - o)
    wick_up   = h - max(o, c)
    wick_down = min(o, c) - l
    bull = (body < avg_body * 0.8) and (wick_down > avg_body * 0.5) and (v < avg_vol * 1.5) and (c > o)
    bear = (body < avg_body * 0.8) and (wick_up   > avg_body * 0.5) and (v < avg_vol * 1.5) and (c < o)
    return bull, bear

def detect_turtle(df, look=20):
    if len(df) < look+2:
        return False, False
    ph = df["high"].iloc[-look-1:-1].max()
    pl = df["low"].iloc[-look-1:-1].min()
    last = df.iloc[-1]
    bull = (last["low"] < pl) and (last["close"] > pl*1.002)
    bear = (last["high"] > ph) and (last["close"] < ph*0.998)
    return bull, bear

def smc_bias(df):
    e20 = df["close"].ewm(span=20).mean().iloc[-1]
    e50 = df["close"].ewm(span=50).mean().iloc[-1]
    return "bull" if e20 > e50 else "bear"

def volume_ok(df):
    ma = df["volume"].rolling(20, min_periods=8).mean().iloc[-1]
    if np.isnan(ma):
        return True
    current = df["volume"].iloc[-1]
    return current > ma * 1.3

def get_direction_from_ma(df, span=20):
    try:
        ma = df["close"].ewm(span=span).mean().iloc[-1]
        return "BUY" if df["close"].iloc[-1] > ma else "SELL"
    except Exception:
        return None

# ===== POSITION SIZING =====
def get_atr(df, period=14):
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,len(df))]
    return float(np.mean(trs)) if trs else 0.0

def trade_params(entry, side, atr, atr_multiplier_sl=1.7, tp_mults=(1.8,2.8,3.8)):
    adj_sl = atr * atr_multiplier_sl
    if side=="BUY":
        sl = entry - adj_sl
        tp1,tp2,tp3 = entry + atr*tp_mults[0], entry + atr*tp_mults[1], entry + atr*tp_mults[2]
    else:
        sl = entry + adj_sl
        tp1,tp2,tp3 = entry - atr*tp_mults[0], entry - atr*tp_mults[1], entry - atr*tp_mults[2]
    return sl,tp1,tp2,tp3

def pos_size_units(entry, sl):
    risk_usd = CAPITAL * 0.05
    sl_dist = abs(entry - sl)
    if sl_dist == 0:
        return 0
    units = risk_usd / sl_dist
    return round(units,2)

# ===== LOGGING =====
def init_csv():
    if not os.path.exists(LOG_CSV):
        os.makedirs(os.path.dirname(LOG_CSV), exist_ok=True)
        with open(LOG_CSV,"w",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp","symbol","side","entry","tp1","tp2","tp3","sl","tf","units"])

def log_signal(row):
    try:
        with open(LOG_CSV,"a",newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception as e:
        print("CSV log error:", e)

# ===== ANALYSIS =====
def analyze_symbol(symbol):
    tf_confirmations = 0
    chosen_dir = None
    chosen_tf = None
    per_tf_scores = []

    for tf in TIMEFRAMES:
        df = get_klines(symbol, tf)
        if df is None or len(df)<30:
            continue
        crt_b, crt_s = detect_crt(df)
        ts_b, ts_s = detect_turtle(df)
        bias = smc_bias(df)
        volok = volume_ok(df)

        bull_score = (WEIGHT_CRT*(1 if crt_b else 0)+
                      WEIGHT_TURTLE*(1 if ts_b else 0)+
                      WEIGHT_VOLUME*(1 if volok else 0)+
                      WEIGHT_BIAS*(1 if bias=="bull" else 0))*100
        bear_score = (WEIGHT_CRT*(1 if crt_s else 0)+
                      WEIGHT_TURTLE*(1 if ts_s else 0)+
                      WEIGHT_VOLUME*(1 if volok else 0)+
                      WEIGHT_BIAS*(1 if bias=="bear" else 0))*100

        per_tf_scores.append(max(bull_score,bear_score))

        if bull_score >= MIN_TF_SCORE:
            tf_confirmations += 1
            chosen_dir = "BUY"
            chosen_tf = tf
        elif bear_score >= MIN_TF_SCORE:
            tf_confirmations += 1
            chosen_dir = "SELL"
            chosen_tf = tf

    if tf_confirmations < CONF_MIN_TFS or chosen_dir is None:
        return False

    confidence_pct = np.mean(per_tf_scores)
    if confidence_pct < CONFIDENCE_MIN:
        return False

    entry = get_price(symbol)
    if entry is None:
        return False

    df_h1 = get_klines(symbol, "H1")
    atr = get_atr(df_h1)
    sl,tp1,tp2,tp3 = trade_params(entry, chosen_dir, atr)
    units = pos_size_units(entry, sl)

    if units<=0:
        return False

    msg = (f"✅ {chosen_dir} {symbol}\n"
           f"💵 Entry: {entry}\n"
           f"🎯 TP1:{tp1} TP2:{tp2} TP3:{tp3}\n"
           f"🛑 SL:{sl}\n"
           f"💰 Units:{units} | Confidence:{confidence_pct:.1f}% | TF:{chosen_tf}")
    send_message(msg)

    log_signal([datetime.utcnow().isoformat(), symbol, chosen_dir, entry, tp1,tp2,tp3,sl,chosen_tf,units])
    return True

# ===== STARTUP =====
init_csv()
send_message("✅ SIRTS Forex/Gold Bot Deployed")

# ===== MAIN LOOP =====
while True:
    for i, sym in enumerate(TOP_SYMBOLS, start=1):
        print(f"[{i}/{len(TOP_SYMBOLS)}] Scanning {sym}")
        try:
            analyze_symbol(sym)
        except Exception as e:
            print(f"Error scanning {sym}: {e}")
        time.sleep(API_CALL_DELAY)
    now = time.time()
    if now - last_heartbeat > 43200:
        send_message(f"💓 Heartbeat {datetime.utcnow().strftime('%H:%M UTC')}")
        last_heartbeat = now
    if now - last_summary > 86400:
        send_message("📊 Daily summary sent.")
        last_summary = now
    time.sleep(CHECK_INTERVAL)