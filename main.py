import time
import traceback

from utils import load_config, log
from telegram_sender import send_telegram_message
from signal_engine import analyze_instruments

# TwelveData cache updater
from data_fetch import update_cache_all_timeframes

# Load main config
CONFIG = load_config()
CHECK_INTERVAL = int(CONFIG.get("CHECK_INTERVAL", 60))

TIMEFRAMES = CONFIG.get("TIMEFRAMES", ["15m", "30m", "1h", "4h"])
INSTRUMENTS = CONFIG.get("INSTRUMENTS", [])


def main():
    log("Starting Forex Bot (TwelveData Cached)...")

    # --- Startup Telegram Notification ---
    try:
        send_telegram_message("🚀 Forex Bot Started Successfully (TwelveData + Cached Mode).")
        log("Startup message sent.")
    except Exception as e:
        log(f"Failed sending startup message: {e}")

    # Main loop
    last_update = 0

    while True:
        try:
            now = time.time()

            # ==========================================================
            # 1) UPDATE TWELVEDATA CACHE ONCE PER CHECK (VERY LIGHT)
            # ==========================================================
            if now - last_update >= CHECK_INTERVAL:
                log("Updating TwelveData cache...")
                update_cache_all_timeframes(INSTRUMENTS, TIMEFRAMES)
                last_update = now

            # ==========================================================
            # 2) RUN ANALYSIS USING CACHED DATA (ZERO API CALLS)
            # ==========================================================
            signals = analyze_instruments()

            for sig in signals:
                msg = (
                    f"🔥 **FOREX SIGNAL — {sig['symbol']}**\n"
                    f"📉 Direction: *{sig['direction']}*\n"
                    f"💰 Entry: `{sig['entry']}`\n"
                    f"🎯 TP1: `{sig['tp1']}`\n"
                    f"🎯 TP2: `{sig['tp2']}`\n"
                    f"🎯 TP3: `{sig['tp3']}`\n"
                    f"🛑 SL: `{sig['sl']}`\n\n"
                    f"📊 Confidence: *{sig['confidence']}%*\n"
                    f"⏱ Reason: `{sig['tf_reason']}`"
                )

                send_telegram_message(msg)
                log(f"Sent signal for {sig['symbol']}")

        except Exception as e:
            log("❗ Error during cycle:")
            log(str(e))
            traceback.print_exc()

        time.sleep(1)  # Keep loop fast, cache updates handle API frequency


if __name__ == "__main__":
    main()