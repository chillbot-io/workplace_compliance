FROM python:3.11-slim

RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ api/
COPY migrations/ migrations/

RUN chown -R appuser:appuser /app

EXPOSE 8000
USER appuser

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/v1/health')" || exit 1

CMD ["sh", "-c", "python migrations/migrate.py && uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4"]
