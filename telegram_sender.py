# telegram_sender.py
import os
import requests

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(text, parse_mode="Markdown"):
    if not BOT_TOKEN or not CHAT_ID:
        print("[telegram] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print("[telegram] send failed:", r.status_code, r.text)
            return False
        return True
    except Exception as e:
        print("[telegram] exception:", e)
        return False