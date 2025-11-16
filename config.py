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
# ===========================
INSTRUMENTS = [

    # Major Pairs
    "EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X",
    "USDJPY=X", "USDCHF=X", "USDCAD=X",

    # Euro Crosses
    "EURGBP=X", "EURJPY=X", "EURCHF=X",
    "EURAUD=X", "EURNZD=X", "EURCAD=X",

    # GBP Crosses
    "GBPJPY=X", "GBPCHF=X",
    "GBPAUD=X", "GBPNZD=X", "GBPCAD=X",

    # AUD Crosses
    "AUDJPY=X", "AUDCHF=X", "AUDCAD=X", "AUDNZD=X",

    # NZD Crosses
    "NZDJPY=X", "NZDCHF=X", "NZDCAD=X",

    # CAD Crosses
    "CADJPY=X", "CADCHF=X",

    # CHF Crosses
    "CHFJPY=X",

    # Metals
    "XAUUSD=X",  # Gold
    "XAGUSD=X",  # Silver

    # USD Exotic Pairs
    "USDSEK=X", "USDNOK=X", "USDZAR=X", "USDTRY=X",
    "USDHKD=X", "USDSGD=X",

    # Euro Exotic Crosses
    "EURSEK=X", "EURNOK=X",
    "EURHUF=X", "EURPLN=X",

    # GBP Exotic Crosses
    "GBPSEK=X", "GBPNOK=X",

    # Others
    "AUDSGD=X", "JPYSGD=X",

    "ZARJPY=X", "TRYJPY=X",
]

# ===========================
# MORE SETTINGS (OPTIONAL)
# I can fill these if needed
# ===========================

# Minimum trend confidence %
MIN_CONFIDENCE = 60.0

# Minimum TF confirmation
MIN_TF_SCORE = 55.0

# Example risk settings (tell me if you want them ON)
BASE_RISK = 0.01
MAX_RISK = 0.03

# Add more filters if needed
USE_STRICT_TF = True