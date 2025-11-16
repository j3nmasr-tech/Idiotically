import json
import datetime

def load_config():
    from config import CONFIG
    return CONFIG

def log(msg: str):
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)