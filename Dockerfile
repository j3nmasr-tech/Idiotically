FROM python:3.11-slim

WORKDIR /app

# Install dependencies required by MetaTrader5
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libc6 \
    libstdc++6 \
    libx11-6 \
    libxss1 \
    libasound2 \
    libglib2.0-0 \
    libxext6 \
    libsm6 \
    libxrender1 \
    libfontconfig1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code
COPY main.py .

# Environment variables
ENV BOT_TOKEN=""
ENV CHAT_ID=""
ENV MT5_LOGIN=""
ENV MT5_PASSWORD=""
ENV MT5_SERVER=""

CMD ["python", "main.py"]