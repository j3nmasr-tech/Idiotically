import yaml
import datetime

def log(msg):
    print(f"[{datetime.datetime.now()}] {msg}")

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)
