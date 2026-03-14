import os
import io
import json
import logging
import tempfile
from typing import Dict

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from dotenv import load_dotenv
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

LOGGER = logging.getLogger("customer_monthly_job")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# load .env if present (convenience for local testing)
load_dotenv()


def _auth_client() -> gspread.Client:
    """Authenticate to Google Sheets using a service account.

    Expect either `GOOGLE_SERVICE_ACCOUNT_FILE` (path mounted in container)
    or `GOOGLE_SERVICE_ACCOUNT_JSON` (raw JSON content) env var.
    """
    sa_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

    if sa_file and os.path.exists(sa_file):
        LOGGER.info("Using service account file from %s", sa_file)
        return gspread.service_account(filename=sa_file)

    if sa_json:
        LOGGER.info("Using service account JSON from env var")
        try:
            # ensure it's valid JSON
            json.loads(sa_json)
            fd, path = tempfile.mkstemp(suffix=".json")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(sa_json)
            return gspread.service_account(filename=path)
        except Exception:
            LOGGER.warning("GOOGLE_SERVICE_ACCOUNT_JSON is present but invalid JSON; will try .env fallback")

    # Dotenv may fail to parse multiline JSON — attempt to extract a JSON blob from .env as a fallback
    env_path = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_path):
        try:
            raw = open(env_path, "r", encoding="utf-8").read()
            start = raw.find("{")
            end = raw.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = raw[start:end+1]
                try:
                    json.loads(candidate)
                    LOGGER.info("Found JSON blob in .env, using it for service account credentials")
                    fd, path = tempfile.mkstemp(suffix=".json")
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        f.write(candidate)
                    return gspread.service_account(filename=path)
                except Exception:
                    LOGGER.debug("Found braces in .env but content is not valid JSON")
        except Exception:
            LOGGER.debug("Failed to read .env for fallback JSON extraction", exc_info=True)

    raise RuntimeError("No Google service account credentials provided. Set GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON")


def load_sheet(spreadsheet_id: str, worksheet_name: str = "main") -> pd.DataFrame:
    client = _auth_client()
    sh = client.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        # fallback to first worksheet
        ws = sh.get_worksheet(0)

    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    # drop empty columns created by gspread
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]  # type: ignore
    return df


def process_dataframe(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = df.copy()
    initial_rows = len(df)
    df["date"] = pd.to_datetime(df.get("date", df.get("Date")), errors="coerce")
    if "number" in df.columns:
        df = df.dropna(subset=["date", "number"])
    else:
        df = df.dropna(subset=["date"])
    LOGGER.info("Rows: initial=%s after dropping missing date/number=%s", initial_rows, len(df))
    df["year_month"] = df["date"].dt.to_period("M")

    cust_mobile = df.get("customerMobile", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": ""})
    cust_name = df.get("customerName", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": ""})
    customer_key = cust_mobile.mask(cust_mobile.eq(""), cust_name)
    df["customer_key"] = customer_key
    before_cust = len(df)
    df = df[df["customer_key"].ne("")]
    LOGGER.info("Rows after customer_key filter: %s (dropped %s)", len(df), before_cust - len(df))

    tx = (
        df.sort_values("date")
          .groupby("number", as_index=False)
          .agg({
              "customer_key": "first",
              "date": "first",
              "orderAmount": "first"
          })
    )

    tx["year_month"] = tx["date"].dt.to_period("M")
    LOGGER.info("Transactions collapsed to invoices (tx rows)=%s", len(tx))

    customer_monthly = (
        tx.groupby(["customer_key", "year_month"])  # type: ignore
          .agg(
              order_count=("number", "size"),
              total_revenue=("orderAmount", "sum")
          )
          .reset_index()
    )

    customer_monthly["aov"] = (customer_monthly["total_revenue"] / customer_monthly["order_count"]).round(2)

    LOGGER.info("Customer-month groups: %s rows", len(customer_monthly))

    # Build pivot tables
    pivot_orders = customer_monthly.pivot(index="customer_key", columns="year_month", values="order_count").fillna(0)
    pivot_aov = customer_monthly.pivot(index="customer_key", columns="year_month", values="aov").fillna(0)

    # Ensure we include the full continuous month range between min and max month
    try:
        min_month = customer_monthly["year_month"].min()
        max_month = customer_monthly["year_month"].max()
        LOGGER.info("Detected customer_monthly month range: %s to %s", min_month, max_month)
        full_months = pd.period_range(min_month, max_month, freq="M").astype(str)
        LOGGER.info("Full months expanded: %s", list(full_months)[:24])
    except Exception:
        # Fallback to discovered months if something goes wrong
        full_months = sorted(pivot_orders.columns.astype(str))

    pivot_orders.columns = pivot_orders.columns.astype(str)
    pivot_aov.columns = pivot_aov.columns.astype(str)

    # Reindex columns to the full continuous months (missing months become zeros)
    pivot_orders = pivot_orders.reindex(full_months, axis=1, fill_value=0)
    pivot_aov = pivot_aov.reindex(full_months, axis=1, fill_value=0)

    pivot_orders = pivot_orders.reset_index()
    pivot_aov = pivot_aov.reset_index()

    months = sorted(customer_monthly["year_month"].astype(str).unique())
    combined = pd.DataFrame({"customer_key": pivot_orders.get("customer_key", pd.Series(dtype=str))})

    for month in months:
        combined[f"{month}_Orders"] = pivot_orders.get(month, 0)
        combined[f"{month}_AOV"] = pivot_aov.get(month, 0)

    return {
        "Customer Orders & AOV": combined,
        "Orders Only": pivot_orders,
        "AOV Only": pivot_aov,
    }


def write_to_sheets(spreadsheet_id: str, dfs: Dict[str, pd.DataFrame]):
    client = _auth_client()
    sh = client.open_by_key(spreadsheet_id)

    for name, df in dfs.items():
        try:
            try:
                ws = sh.worksheet(name)
                ws.clear()
            except Exception:
                ws = sh.add_worksheet(title=name, rows=str(max(100, len(df) + 10)), cols=str(max(10, len(df.columns))))

            set_with_dataframe(ws, df, include_index=False, resize=True)
            LOGGER.info("Wrote sheet '%s' (%s rows)", name, len(df))
        except Exception as e:
            LOGGER.exception("Failed to write sheet %s: %s", name, e)


def run_job():
    input_sheet = os.environ.get("INPUT_SHEET_ID")
    output_sheet = os.environ.get("OUTPUT_SHEET_ID", input_sheet)

    if not input_sheet:
        raise RuntimeError(
            "Missing INPUT_SHEET_ID. Set the env var (e.g. in PowerShell: $env:INPUT_SHEET_ID='your_id'; python current.py) "
            "or add INPUT_SHEET_ID=your_id to a .env file in the working directory."
        )

    LOGGER.info("Loading input sheet %s", input_sheet)
    df = load_sheet(input_sheet)
    LOGGER.info("Read %s rows from input", len(df))

    # Diagnostics: inspect raw date coverage
    raw_date_col = df.get("date", df.get("Date"))
    parsed_dates = pd.to_datetime(raw_date_col, errors="coerce")
    n_missing_dates = int(parsed_dates.isna().sum())
    LOGGER.info("Raw date column: %s non-null, %s null after parsing", int(parsed_dates.count()), n_missing_dates)
    if parsed_dates.notna().any():
        LOGGER.info("Raw date range: %s to %s", parsed_dates.min(), parsed_dates.max())
        months_in_raw = parsed_dates.dt.to_period("M").astype(str).unique()
        LOGGER.info("Months present in raw data (sample up to 12): %s", list(months_in_raw)[:12])

    results = process_dataframe(df)
    LOGGER.info("Processing complete, writing %s output sheets", len(results))
    write_to_sheets(output_sheet, results)
    LOGGER.info("Job finished")


def main():
    run_once = os.environ.get("RUN_ONCE", "false").lower() in ("1", "true", "yes")
    schedule_days = int(os.environ.get("SCHEDULE_DAYS", "7"))

    if run_once:
        run_job()
        return

    scheduler = BlockingScheduler()
    scheduler.add_job(run_job, "interval", days=schedule_days, next_run_time=datetime.now())
    LOGGER.info("Scheduled job every %s days. Starting scheduler...", schedule_days)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("Shutting down scheduler")


if __name__ == "__main__":
    main()