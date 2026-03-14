FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# system deps for manylinux wheels if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /app
RUN chmod +x /app/entrypoint.sh || true

# Expected runtime env vars:
# - GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON
# - INPUT_SHEET_ID
# - OUTPUT_SHEET_ID (optional)
# - RUN_ONCE (true|false)
# - SCHEDULE_DAYS (default 7)

ENTRYPOINT ["/app/entrypoint.sh"]