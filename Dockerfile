# Use official Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for MetaTrader5
RUN apt-get update && apt-get install -y \
    libatlas-base-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code
COPY main.py .

# Use environment variables for secrets
ENV BOT_TOKEN=""
ENV CHAT_ID=""
ENV MT5_LOGIN=""
ENV MT5_PASSWORD=""
ENV MT5_SERVER=""

# Run the bot
CMD ["python", "main.py"]