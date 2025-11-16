# Use official Python 3.12 image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements if you have one, otherwise install directly
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Set environment variables to be read at runtime
ENV PYTHONUNBUFFERED=1

# Run the bot
CMD ["python", "./CMD ["python", "./main.py"]
