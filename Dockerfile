FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ api/
COPY migrations/ migrations/

EXPOSE 8000
CMD ["sh", "-c", "python migrations/migrate.py && uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4"]
