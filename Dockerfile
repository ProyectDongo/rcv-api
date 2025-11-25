FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

COPY . .

CMD ["sh", "-c", "celery -A tasks worker -l info & uvicorn main:app --host 0.0.0.0 --port $PORT"]