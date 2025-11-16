import time
import traceback

from utils import load_config, log
from telegram_sender import send_telegram_message
from signal_engine import analyze_instruments

# Correct TwelveData updater (this exists in your data_fetch.py)
from data_fetch import update_cache_for_tf

# Load config
CONFIG = load_config()
CHECK_INTERVAL = int(CONFIG.get("CHECK_INTERVAL", 60))

TIMEFRAMES = CONFIG.get("TIMEFRAMES", ["15m", "30m", "1h", "4h"])
INSTRUMENTS = CONFIG.get("INSTRUMENTS", [])


def update_all_timeframes(symbols, timeframes):
    """Update cache for all TFs using batch calls."""
    log("Updating TwelveData cache for all timeframes...")
    for tf in timeframes:
        try:
            update_cache_for_tf(symbols, tf)
        except Exception as e:
            log(f"Cache update error for {tf}: {e}")
    log("Cache update completed.")


def main():
    log("Starting Forex Bot (TwelveData Cached Mode)...")

    # --- Startup Telegram Notification ---
    try:
        send_telegram_message("🚀 Forex Bot Started Successfully (TwelveData + Cache Mode).")
        log("Startup message sent.")
    except Exception as e:
        log(f"Failed sending startup message: {e}")

    last_cache = 0

    # =============================
    #   MAIN LOOP
    # =============================
    while True:
        try:
            now = time.time()

            # Update cache every CHECK_INTERVAL seconds
            if now - last_cache >= CHECK_INTERVAL:
                update_all_timeframes(INSTRUMENTS, TIMEFRAMES)
                last_cache = now

            # Run analysis using ONLY cached data
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
                log(f"Signal sent for {sig['symbol']}")

        except Exception as e:
            log("❗ Error during cycle:")
            log(str(e))
            traceback.print_exc()

        # Keep loop fast — we only slow API via cache interval
        time.sleep(1)


if __name__ == "__main__":
    main()