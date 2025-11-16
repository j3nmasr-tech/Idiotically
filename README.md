# SIRTS Forex/Gold Signal Bot

✅ Real-time Forex and Gold signal bot based on multiple timeframe analysis, ATR-based SL/TP, and trend detection.  
💵 Supports major Forex pairs and XAUUSD.  
📡 Sends signals via Telegram.  

---

## Features

- Multiple timeframe confirmations (M15, M30, H1, H4)
- ATR-based stop-loss and take-profits
- Position sizing with risk management
- CSV logging for backtesting
- Heartbeat messages every 12h
- Daily summary messages
- Fully compatible with Northflank deployment

---

## Requirements

- Python 3.11+
- MetaTrader5 terminal installed (for Windows/Linux deployment)
- Telegram bot token and chat ID
- Northflank secrets configured for credentials

---

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/sirts-forex-bot.git
cd sirts-forex-bot
