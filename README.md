# Customer Monthly Orders & AOV job

This service reads transactional data from a Google Sheets spreadsheet, computes per-customer monthly order counts and AOV, and writes results back to Google Sheets. It is designed to run weekly (default) and is Docker-ready.

Required environment variables
- `INPUT_SHEET_ID` — spreadsheet ID (not URL) containing input sheet (sheet named `main` or first sheet)
- `OUTPUT_SHEET_ID` — spreadsheet ID to write outputs (defaults to input sheet)
- `GOOGLE_SERVICE_ACCOUNT_FILE` — path inside container to service account JSON (recommended)
  OR
- `GOOGLE_SERVICE_ACCOUNT_JSON` — raw service account JSON content (the entrypoint will write to `/tmp/sa.json`)
- `RUN_ONCE` — if `true` the container runs the job once then exits (useful for cron on host)
- `SCHEDULE_DAYS` — number of days between runs when running as a long-lived process (default `7`)

Build and run with Docker

Build:
```bash
docker build -t customer-monthly:latest .
```

Run (mount service account file):
```bash
docker run --rm \
  -e INPUT_SHEET_ID=your_input_sheet_id \
  -e OUTPUT_SHEET_ID=your_output_sheet_id \
  -e RUN_ONCE=true \
  -v /path/to/sa.json:/secrets/sa.json:ro \
  -e GOOGLE_SERVICE_ACCOUNT_FILE=/secrets/sa.json \
  customer-monthly:latest
```

Or run as a long-lived container that runs every 7 days (uses APScheduler inside container):
```bash
docker run -d \
  --name customer-monthly \
  -e INPUT_SHEET_ID=your_input_sheet_id \
  -e OUTPUT_SHEET_ID=your_output_sheet_id \
  -v /path/to/sa.json:/secrets/sa.json:ro \
  -e GOOGLE_SERVICE_ACCOUNT_FILE=/secrets/sa.json \
  customer-monthly:latest
```

Run with Docker Compose (recommended for deployments)

1. Create `secrets/sa.json` and put your service account JSON there (keep it secure).
2. Edit `docker-compose.yml` and set `INPUT_SHEET_ID` and `OUTPUT_SHEET_ID` environment values (or provide an env file).

Start the service (it runs APScheduler inside the container and executes every `SCHEDULE_DAYS` days — default 7):

```bash
docker compose up -d --build
```

To run once via compose (useful for testing):

```bash
docker compose run --rm -e RUN_ONCE=true customer-monthly
```

Notes:
- The container mounts `./secrets/sa.json` into `/secrets/sa.json`; ensure the file exists and is readable by Docker.
- APScheduler runs inside the container. For production-grade scheduling, you can instead run the container with `RUN_ONCE=true` from an external cron or CI scheduler.

Scheduling via host cron (recommended for predictable scheduling)
Use `RUN_ONCE=true` and run the container from host cron weekly:

```cron
# run weekly at 00:00 Sunday
0 0 * * 0 docker run --rm -e INPUT_SHEET_ID=... -e OUTPUT_SHEET_ID=... -v /path/to/sa.json:/secrets/sa.json:ro -e GOOGLE_SERVICE_ACCOUNT_FILE=/secrets/sa.json your-registry/customer-monthly:latest
```

Notes
- The included `service.json` is an example descriptor; adapt to your platform (systemd, k8s, or cloud service).
- Keep service account JSON secure and mount it read-only into the container.
- Test with `RUN_ONCE=true` before enabling scheduled runs.