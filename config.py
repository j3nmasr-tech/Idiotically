# ===========================
# FOREX BOT CONFIGURATION
# ===========================

# How often the bot checks new candles (seconds)
CHECK_INTERVAL = 60     # 1 minute

# ===========================
# TIMEFRAMES TO SCAN
# ===========================
TIMEFRAMES = [
    "15m",
    "30m",
    "1h",
    "4h"
]

# ===========================
# FOREX INSTRUMENTS TO SCAN
# (Clean Yahoo-Safe List)
# ===========================
INSTRUMENTS = [

    # ===== Major Pairs (100% supported) =====
    "EURUSD=X",
    "GBPUSD=X",
    "AUDUSD=X",
    "NZDUSD=X",
    "USDJPY=X",
    "USDCHF=X",
    "USDCAD=X",

    # ===== Crosses (safe & reliable) =====
    "EURGBP=X",
    "EURJPY=X",
    "GBPJPY=X",
    "AUDJPY=X",
    "NZDJPY=X",
    "CHFJPY=X",
    "CADJPY=X",

    # ===== Metals (use 1H+ data only) =====
    "XAUUSD=X",
    "XAGUSD=X",
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