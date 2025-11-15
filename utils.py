import os
import logging

def log(msg):
    print(msg)

def load_config():
    import config
    return {
        "CHECK_INTERVAL": config.CHECK_INTERVAL,
        "TIMEFRAMES": config.TIMEFRAMES,
        "INSTRUMENTS": config.INSTRUMENTS
    }