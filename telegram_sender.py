# telegram_sender.py
import os
import requests

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TELEGRAM_MAX_LEN = 4000  # leave room for formatting overhead


def send_telegram_message(text, parse_mode="Markdown"):
    # Safety checks
    if not BOT_TOKEN or not CHAT_ID:
        print("[telegram] ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")
        return False

    # Prevent Telegram message overflow
    if len(text) > TELEGRAM_MAX_LEN:
        text = text[:TELEGRAM_MAX_LEN] + "\n\n✂️ Message trimmed due to length limit."

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)

        if response.status_code != 200:
            # Print Telegram errors clearly
            print(f"[telegram] FAILED ({response.status_code}): {response.text}")
            return False

        return True

    except Exception as e:
        print("[telegram] EXCEPTION:", str(e))
        return False