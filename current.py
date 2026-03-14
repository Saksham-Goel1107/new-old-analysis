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
import argparse
import sys
import requests
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
            # Normalize Series -> DataFrame
            if isinstance(df, pd.Series):
                df = df.to_frame()

            # If the DataFrame has a meaningful index name or non-default index,
            # preserve it by resetting the index into a column so the sheet shows row labels.
            if getattr(df.index, "name", None) is not None and df.index.name != "index":
                df = df.reset_index()

            # Ensure column names are strings (handles Period objects)
            df.columns = df.columns.astype(str)

            try:
                ws = sh.worksheet(name)
                ws.clear()
            except Exception:
                ws = sh.add_worksheet(title=name, rows=str(max(100, len(df) + 10)), cols=str(max(10, len(df.columns))))

            set_with_dataframe(ws, df, include_index=False, resize=True)
            LOGGER.info("Wrote sheet '%s' (%s rows)", name, len(df))
        except Exception as e:
            LOGGER.exception("Failed to write sheet %s: %s", name, e)


def compute_new_old_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly metrics splitting new vs returning customers.

    Returns a DataFrame with columns:
    year_month, unique_customers, new_customers, old_customers,
    total_orders, new_orders, old_orders, total_revenue, new_revenue, old_revenue
    """
    df = df.copy()
    # parse date and require invoice number
    df["date"] = pd.to_datetime(df.get("date", df.get("Date")), errors="coerce")
    if "number" in df.columns:
        df = df.dropna(subset=["date", "number"])  # need date + invoice
    else:
        df = df.dropna(subset=["date"])

    # robust customer id
    cust_mobile = df.get("customerMobile", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": ""})
    cust_name = df.get("customerName", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": ""})
    city = df.get("city", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": ""})
    fallback_id = cust_name + "|" + city
    customer_id = cust_mobile.mask(cust_mobile.eq(""), fallback_id)
    df["customer_id"] = customer_id
    df = df[df["customer_id"].ne("")]

    # collapse to one row per invoice
    tx = (
        df.sort_values("date")
          .groupby("number", as_index=False)
          .agg({
              "customer_id": "first",
              "date": "first",
              "orderAmount": "first"
          })
    )

    tx["year_month"] = tx["date"].dt.to_period("M")

    # first purchase month per customer
    first_month = tx.groupby("customer_id")["date"].min().dt.to_period("M").rename("first_purchase_month")
    tx = tx.merge(first_month, on="customer_id", how="left")

    # customer-month level
    cust_month = (
        tx.groupby(["customer_id", "year_month"], observed=True)
          .agg(orders=("number", "nunique"), revenue=("orderAmount", "sum"))
          .reset_index()
    )

    cust_month = cust_month.merge(first_month.reset_index(), on="customer_id", how="left")
    cust_month["is_new"] = cust_month["year_month"] == cust_month["first_purchase_month"]

    # aggregate totals
    total = cust_month.groupby("year_month").agg(
        unique_customers=("customer_id", "nunique"),
        total_orders=("orders", "sum"),
        total_revenue=("revenue", "sum"),
    )

    new = cust_month[cust_month["is_new"]].groupby("year_month").agg(
        new_customers=("customer_id", "nunique"),
        new_orders=("orders", "sum"),
        new_revenue=("revenue", "sum"),
    )

    old = cust_month[~cust_month["is_new"]].groupby("year_month").agg(
        old_customers=("customer_id", "nunique"),
        old_orders=("orders", "sum"),
        old_revenue=("revenue", "sum"),
    )

    monthly = total.join(new, how="left").join(old, how="left").fillna(0)
    monthly = monthly.reset_index()
    monthly["year_month"] = monthly["year_month"].astype(str)

    # ensure integer columns where appropriate
    for c in ["unique_customers", "new_customers", "old_customers", "total_orders", "new_orders", "old_orders"]:
        if c in monthly:
            monthly[c] = monthly[c].astype(int)

    monthly = monthly.sort_values("year_month")
    return monthly


def compute_cohort_metrics(df: pd.DataFrame):
    """Compute cohort matrices: monthly summary, cohort counts, retention rates, cohort sizes.

    Returns a dict of DataFrames:
      - Monthly New vs Old (summary)
      - CohortCounts
      - RetentionRate
      - CohortSize
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df.get("date", df.get("Date")), errors="coerce")
    df = df.dropna(subset=["date", "number"]) if "number" in df.columns else df.dropna(subset=["date"])

    cust_mobile = df.get("customerMobile", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": "", "None": ""})
    cust_name = df.get("customerName", pd.Series(dtype=str)).astype(str).fillna("").replace({"nan": "", "NaN": "", "None": ""})
    customer_key = cust_mobile.mask(cust_mobile.eq(""), cust_name)
    df["customer_key"] = customer_key
    df = df[df["customer_key"].ne("")]

    tx = (
        df.sort_values("date")
          .groupby("number", as_index=False)
          .agg({
              "customer_key": "first",
              "date": "first",
              "orderAmount": "first",
          })
    )

    tx["year_month"] = tx["date"].dt.to_period("M")

    # first purchase month per customer (cohort)
    first_month = tx.groupby("customer_key")["year_month"].min().rename("first_purchase_month")
    tx = tx.join(first_month, on="customer_key")

    # customer-month level
    cust_month = (
        tx.groupby(["customer_key", "year_month"])  # type: ignore
          .agg(orders=("number", "nunique"), revenue=("orderAmount", "sum"))
          .reset_index()
    )

    cust_month = cust_month.join(first_month, on="customer_key")
    cust_month["is_new"] = cust_month["year_month"] == cust_month["first_purchase_month"]

    # monthly summary
    monthly_summary = (
        cust_month.groupby("year_month")
                  .agg(
                      total_customers=("customer_key", "nunique"),
                      new_customers=("is_new", "sum"),
                      old_customers=("is_new", lambda s: (~s).sum()),
                      total_orders=("orders", "sum"),
                      total_revenue=("revenue", "sum"),
                  )
                  .reset_index()
    )

    monthly_summary["year_month"] = monthly_summary["year_month"].astype(str)
    monthly_summary = monthly_summary.sort_values("year_month")

    # cohort counts matrix
    cohort_counts = (
        cust_month.groupby(["first_purchase_month", "year_month"])["customer_key"]
                  .nunique()
                  .reset_index()
                  .pivot(index="first_purchase_month", columns="year_month", values="customer_key")
    )

    cohort_counts = cohort_counts.fillna(0).astype(int)
    cohort_counts.index.name = "cohort_month"
    cohort_counts_display = cohort_counts.copy()
    cohort_counts_display.columns = cohort_counts_display.columns.astype(str)
    cohort_counts_display.index = cohort_counts_display.index.astype(str)

    # cohort size and retention
    cohort_size = (
        first_month.value_counts().sort_index().rename("cohort_size")
    )
    cohort_size.index.name = "cohort_month"
    cohort_size = cohort_size.reindex(cohort_counts.index, fill_value=0)
    cohort_size_display = cohort_size.copy().to_frame()
    cohort_size_display.index = cohort_size_display.index.astype(str)

    cohort_size_nonzero = cohort_size.replace(0, pd.NA)
    retention_rate = cohort_counts.div(cohort_size_nonzero, axis=0).round(3)
    retention_display = retention_rate.copy()
    retention_display.columns = retention_display.columns.astype(str)
    retention_display.index = retention_display.index.astype(str)

    return {
        "Monthly New vs Old": monthly_summary,
        "CohortCounts": cohort_counts_display,
        "RetentionRate": retention_display,
        "CohortSize": cohort_size_display,
    }


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
    # compute new vs returning customer monthly metrics and add to outputs
    try:
        new_old = compute_new_old_metrics(df)
        results["New vs Old Monthly"] = new_old
        LOGGER.info("Computed New vs Old Monthly metrics (%s rows)", len(new_old))
    except Exception:
        LOGGER.exception("Failed to compute New vs Old metrics")
    # compute cohort metrics and add sheets (do not remove existing sheets)
    try:
        cohort_results = compute_cohort_metrics(df)
        for k, v in cohort_results.items():
            # avoid overwriting existing keys; if conflict, append suffix
            sheet_name = k
            if sheet_name in results:
                i = 1
                while f"{sheet_name}_{i}" in results:
                    i += 1
                sheet_name = f"{sheet_name}_{i}"
            results[sheet_name] = v
        LOGGER.info("Computed cohort metrics and added %s sheets", len(cohort_results))
    except Exception:
        LOGGER.exception("Failed to compute cohort metrics")
    LOGGER.info("Processing complete, writing %s output sheets", len(results))
    write_to_sheets(output_sheet, results)
    LOGGER.info("Job finished")


def send_heartbeat(success: bool = True, exit_code: int = 0):
    """Send heartbeat to BetterStack uptime monitor. Uses env var `HEARTBEAT_URL` if set,
    otherwise uses provided default.
    On failure, appends `/fail` or `/<exit_code>` to the URL as recommended.
    """
    url = os.environ.get("HEARTBEAT_URL", "https://uptime.betterstack.com/api/v1/heartbeat/D73zG99KsRjfUwogYsmoBJtQ")
    try:
        if success:
            # simple GET is accepted as heartbeat
            requests.get(url, timeout=10)
            LOGGER.info("Sent success heartbeat to %s", url)
        else:
            fail_url = f"{url}/{exit_code if exit_code and exit_code != 0 else 'fail'}"
            requests.get(fail_url, timeout=10)
            LOGGER.info("Sent failure heartbeat to %s", fail_url)
    except Exception:
        LOGGER.exception("Failed to send heartbeat to %s", url)


def execute_and_heartbeat():
    try:
        run_job()
        send_heartbeat(success=True)
    except SystemExit as e:
        code = int(getattr(e, "code", 1) or 1)
        send_heartbeat(success=False, exit_code=code)
        raise
    except Exception as e:
        LOGGER.exception("Job failed: %s", e)
        # send failure heartbeat with generic non-zero code
        send_heartbeat(success=False, exit_code=1)
        raise


def main():
    run_once = os.environ.get("RUN_ONCE", "false").lower() in ("1", "true", "yes")
    schedule_days = int(os.environ.get("SCHEDULE_DAYS", "7"))

    if run_once:
        execute_and_heartbeat()
        return

    scheduler = BlockingScheduler()
    scheduler.add_job(execute_and_heartbeat, "interval", days=schedule_days, next_run_time=datetime.now())
    LOGGER.info("Scheduled job every %s days. Starting scheduler...", schedule_days)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("Shutting down scheduler")


def healthcheck() -> int:
    """Lightweight healthcheck: verify env and service account file exist.

    Returns 0 if healthy, non-zero otherwise. This should be fast and must NOT start the scheduler.
    """
    input_sheet = os.environ.get("INPUT_SHEET_ID")
    sa_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")

    if not input_sheet:
        LOGGER.error("Healthcheck failed: INPUT_SHEET_ID not set")
        return 1

    if not sa_file or not os.path.exists(sa_file):
        LOGGER.error("Healthcheck failed: GOOGLE_SERVICE_ACCOUNT_FILE missing or not found (%s)", sa_file)
        return 2

    LOGGER.info("Healthcheck OK")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--healthcheck", action="store_true", help="Run a fast healthcheck and exit")
    args = parser.parse_args()

    if args.healthcheck:
        sys.exit(healthcheck())

    main()