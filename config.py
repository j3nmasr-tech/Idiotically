# ===========================
# FOREX BOT CONFIGURATION
# ===========================

# Check every 5 minutes (safe for 800 requests/day limit)
CHECK_INTERVAL = 300   # seconds

# ===========================
# TIMEFRAMES TO SCAN (TwelveData format)
# ===========================
TIMEFRAMES = [
    "15min",
    "30min",
    "1h",
    "4h"
]

# ===========================
# FOREX INSTRUMENTS TO SCAN
# (Top 30 Forex Pairs + Gold + Silver)
# ===========================
INSTRUMENTS = [

    # ===== Majors =====
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "USD/JPY",
    "USD/CHF",
    "USD/CAD",

    # ===== Euro Crosses =====
    "EUR/GBP",
    "EUR/JPY",
    "EUR/CHF",
    "EUR/AUD",
    "EUR/NZD",
    "EUR/CAD",

    # ===== GBP Crosses =====
    "GBP/JPY",
    "GBP/CHF",
    "GBP/AUD",
    "GBP/NZD",
    "GBP/CAD",

    # ===== AUD Crosses =====
    "AUD/JPY",
    "AUD/CHF",
    "AUD/CAD",
    "AUD/NZD",

    # ===== NZD Crosses =====
    "NZD/JPY",
    "NZD/CHF",
    "NZD/CAD",

    # ===== CAD Crosses =====
    "CAD/JPY",
    "CAD/CHF",

    # ===== Metals =====
    "XAU/USD",    # Gold
    "XAG/USD"     # Silver
]

# ===========================
# STRATEGY SETTINGS
# ===========================
MIN_CONFIDENCE = 60.0
MIN_TF_SCORE = 55.0
USE_STRICT_TF = True

# Risk Settings
BASE_RISK = 0.01
MAX_RISK = 0.03