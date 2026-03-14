#!/usr/bin/env bash
set -euo pipefail

cd /app

# If user provided raw JSON creds in env, write to file and set GOOGLE_SERVICE_ACCOUNT_FILE
if [ -n "${GOOGLE_SERVICE_ACCOUNT_JSON:-}" ] && [ -z "${GOOGLE_SERVICE_ACCOUNT_FILE:-}" ]; then
  echo "$GOOGLE_SERVICE_ACCOUNT_JSON" > /tmp/sa.json
  export GOOGLE_SERVICE_ACCOUNT_FILE=/tmp/sa.json
fi

exec python current.py