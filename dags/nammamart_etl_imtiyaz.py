"""NammaMart pandas ETL; 09:00 Asia/Kolkata, Monday through Saturday."""

import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/usr/local/airflow")

import pendulum
from airflow.sdk import dag, task, get_current_context

from include.nammamart_etl import DATA, WAREHOUSE, record_run, run_etl, warehouse_connection
from include.nammamart_quality import read_source


DEFAULT_ARGS = {
    "owner": "mohammed-imtiyaz",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}


def _inspect(name):
    frame = read_source(DATA / f"{name}.csv", name)
    print(f"Extracted {name}: {len(frame)} rows, {len(frame.columns)} expected columns")
    return len(frame)


@dag(
    dag_id="nammamart_etl_imtiyaz",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["nammamart", "etl", "fde-module-2"],
)
def nammamart_etl_imtiyaz():
    @task
    def extract_customers():
        return _inspect("customers")

    @task
    def extract_products():
        return _inspect("products")

    @task
    def extract_stores():
        return _inspect("stores")

    @task
    def extract_orders():
        return _inspect("orders")

    @task
    def extract_payments():
        return _inspect("payments")

    @task
    def validate_sources(counts):
        if any(count == 0 for count in counts):
            raise ValueError("An input file is empty")
        print(f"Schema and non-empty checks passed for five files: {counts}")
        return sum(counts)

    @task
    def transform_and_load(_validated):
        context = get_current_context()
        incremental = bool((context["dag_run"].conf or {}).get("incremental", False))
        return run_etl(run_id=context["run_id"], incremental=incremental)

    @task
    def report(_result):
        with warehouse_connection(WAREHOUSE) as con:
            print("Daily store revenue:", con.execute(
                "SELECT * FROM etl_daily_zone_revenue ORDER BY order_date, store_id LIMIT 15").fetchall())
            print("Category margin:", con.execute(
                "SELECT * FROM etl_category_margin ORDER BY category").fetchall())
            print("Payment mismatches:", con.execute(
                "SELECT reason, COUNT(*) FROM etl_payment_reconciliation GROUP BY 1").fetchall())
            con.execute("UPDATE pipeline_runs SET details=details || '; report_complete=1' "
                        "WHERE run_id=? AND dag_id=? AND status='SUCCESS'",
                        [get_current_context()["run_id"], "nammamart_etl_imtiyaz"])

    @task(trigger_rule="all_done", retries=0)
    def audit():
        context = get_current_context()
        run_id = context["run_id"]
        with warehouse_connection(WAREHOUSE) as con:
            row = con.execute("SELECT status,details FROM pipeline_runs WHERE run_id=? AND dag_id=? "
                              "ORDER BY end_time DESC LIMIT 1",
                              [run_id, "nammamart_etl_imtiyaz"]).fetchone()
            succeeded = bool(row and row[0] == "SUCCESS" and "report_complete=1" in (row[1] or ""))
            if row:
                con.execute("UPDATE pipeline_runs SET end_time=?,status=? WHERE run_id=? AND dag_id=?",
                            [datetime.now(), "SUCCESS" if succeeded else "FAILED", run_id, "nammamart_etl_imtiyaz"])
            else:
                record_run(con, run_id, "nammamart_etl_imtiyaz", datetime.now(),
                           0, 0, 0, "FAILED", "Upstream task failed before processing")
        if not succeeded:
            raise RuntimeError("Upstream extraction, processing, or report failed; see task logs")

    counts = [extract_customers(), extract_products(), extract_stores(),
              extract_orders(), extract_payments()]
    checked = validate_sources(counts)
    result = transform_and_load(checked)
    printed = report(result)
    printed >> audit()


nammamart_etl_imtiyaz()
