import time
import traceback
from signal_engine import analyze_instruments
from telegram_sender import send_telegram_message
from utils import load_config, log

CONFIG = load_config()
CHECK_INTERVAL = int(CONFIG.get("CHECK_INTERVAL", 60))

def main():
    log("Starting Scalp Forex Alert Bot (Stooq Free Data)...")

    # No API object needed anymore
    data_api = None

    while True:
        try:
            signals = analyze_instruments(data_api)

            for sig in signals:
                msg = (
                    f"🔥 SCALP SIGNAL — {sig['symbol']}\n"
                    f"Direction: {sig['direction']}\n"
                    f"Entry: {sig['entry']}\n"
                    f"TP1: {sig['tp1']}\n"
                    f"TP2: {sig['tp2']}\n"
                    f"TP3: {sig['tp3']}\n"
                    f"SL: {sig['sl']}\n\n"
                    f"Confidence: {sig['confidence']}%\n"
                    f"Timeframe: {sig['tf_reason']}"
                )
                send_telegram_message(msg)
                log(f"Sent signal for {sig['symbol']}")

        except Exception as e:
            log("Error during cycle:")
            log(str(e))
            traceback.print_exc()

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()