# Use official Python 3.11 slim (MetaTrader5 works on 3.8–3.11 only)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for MetaTrader5
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libc6 \
    libstdc++6 \
    libgcc1 \
    libx11-6 \
    libxss1 \
    libasound2 \
    libatlas-base-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY main.py .

# Environment variables (Northflank secrets override these)
ENV BOT_TOKEN=""
ENV CHAT_ID=""
ENV MT5_LOGIN=""
ENV MT5_PASSWORD=""
ENV MT5_SERVER=""

# Start bot
CMD ["python", "main.py"]