# Scalp Forex Alert Bot (Telegram + OANDA + Northflank)

This bot scans multiple Forex pairs, metals, and indices using:
- SMC Bias
- Turtle Breakout
- CRT pattern detection
- 2/3 balanced strict TF agreement

It sends clean BUY/SELL alerts to Telegram.

## Deployment (Northflank)
1. Create Service (worker)
2. Upload repo
3. Add env vars:
   - OANDA_API_KEY
   - OANDA_ACCOUNT_ID
   - OANDA_URL=https://api-fxpractice.oanda.com
   - TELEGRAM_TOKEN
   - TELEGRAM_CHAT_ID
4. Run command:
