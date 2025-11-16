# ===========================
# FOREX BOT CONFIGURATION
# ===========================

CONFIG = {
    # how often to update the TwelveData cache (seconds)
    "CHECK_INTERVAL": 60,

    # timeframes used in analysis
    "TIMEFRAMES": [
        "15m",
        "30m",
        "1h",
        "4h",
        "1d"
    ],

    # full top 30 FX + gold
    "INSTRUMENTS": [
        "EURUSD","GBPUSD","USDJPY","AUDUSD","NZDUSD","USDCHF","USDCAD",
        "EURJPY","EURGBP","GBPJPY","CHFJPY","AUDJPY","NZDJPY","CADJPY",
        "EURAUD","EURNZD","GBPAUD","GBPCAD","GBPCHF",
        "AUDNZD","AUDCHF","NZDCHF","CADCHF","AUDCAD","NZDCAD","EURCHF",
        "USDSEK","USDNOK","USDZAR",
        "XAUUSD"
    ],

    # strategy settings
    "BASE_RISK": 1.0,
    "LEVERAGE": 30,
    "USE_CACHE": True
}