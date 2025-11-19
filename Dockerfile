FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

CMD ["python3", "main.py"]