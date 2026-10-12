"""Pandas-first ETL implementation for the NammaMart assignment."""

from __future__ import annotations

import fcntl
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import duckdb
import pandas as pd

from include.nammamart_quality import read_source, validate_frame, mask_email, mask_phone


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "include" / "data" / "nammamart"
WAREHOUSE = ROOT / "include" / "warehouse_imtiyaz.duckdb"
FILES = ("customers", "products", "stores", "orders", "payments")
DAG_ID = "nammamart_etl_imtiyaz"


@contextmanager
def warehouse_connection(path: Path = WAREHOUSE):
    """Serialize access by the two Airflow DAGs to one DuckDB warehouse."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path) + ".lock", "a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        connection = duckdb.connect(str(path))
        try:
            yield connection
        finally:
            connection.close()
            fcntl.flock(lock, fcntl.LOCK_UN)


def ensure_audit_tables(con):
    con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
        severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
        status VARCHAR, checked_at TIMESTAMP)""")
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
        rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
        status VARCHAR, details VARCHAR)""")


def record_run(con, run_id, dag_id, start, extracted, loaded, quarantined, status, details=""):
    ensure_audit_tables(con)
    # Airflow Clear reuses the same run_id. Keep one current audit row per DAG run;
    # Airflow task logs retain the earlier attempts and failure history.
    con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, dag_id])
    con.execute("""INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [run_id, dag_id, start, datetime.now(), extracted, loaded,
                 quarantined, status, details])


def _load_frame(con, table: str, frame: pd.DataFrame):
    con.register("incoming_frame", frame)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM incoming_frame")
    con.unregister("incoming_frame")


def _append_quality(con, checks: list):
    if checks:
        frame = pd.DataFrame(checks)
        con.register("quality_frame", frame)
        con.execute("""INSERT INTO dq_results
            SELECT run_id, dag_id, check_name, dimension, severity,
                   rows_checked, rows_failed, status, CAST(checked_at AS TIMESTAMP)
            FROM quality_frame""")
        con.unregister("quality_frame")


def _quarantine(name: str, failed: pd.DataFrame, run_id: str, root: Path):
    if failed.empty:
        return
    folder = root / "include" / "quarantine" / "imtiyaz"
    folder.mkdir(parents=True, exist_ok=True)
    safe_run = re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
    failed.to_csv(folder / f"{name}_failed_{safe_run}.csv", index=False)


def run_etl(data_dir: Path = DATA, warehouse: Path = WAREHOUSE,
            run_id: Optional[str] = None, incremental: bool = False) -> Dict[str, int]:
    """Execute ETL once, with real quality/audit records and idempotent marts."""
    run_id = run_id or "manual_" + datetime.now().strftime("%Y%m%dT%H%M%S")
    start = datetime.now()
    extracted = loaded = quarantined = 0
    all_checks, all_issues = [], []
    try:
        with warehouse_connection(warehouse) as con:
            prior_increment = con.execute("""SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name='etl_run_state'""").fetchone()[0]
            already_applied = bool(prior_increment and con.execute(
                "SELECT COALESCE(MAX(incremental_applied), false) FROM etl_run_state").fetchone()[0])
        effective_incremental = incremental or already_applied
        raw = {name: read_source(data_dir / f"{name}.csv", name) for name in FILES}
        extracted = sum(len(frame) for frame in raw.values())
        latest_order = pd.to_datetime(raw["orders"]["order_ts"], errors="coerce").max()
        freshness_failed = int(pd.isna(latest_order) or
                               latest_order < pd.Timestamp.now() - pd.Timedelta(days=1))
        for check_name, dimension, checked, failed in (
            ("orders_volume", "Completeness", len(raw["orders"]), int(len(raw["orders"]) < 1000)),
            ("orders_freshness", "Timeliness", len(raw["orders"]), freshness_failed),
        ):
            all_checks.append({"run_id": run_id, "dag_id": DAG_ID, "check_name": check_name,
                               "dimension": dimension, "severity": "Warning",
                               "rows_checked": checked, "rows_failed": failed,
                               "status": "FAIL" if failed else "PASS",
                               "checked_at": datetime.now().isoformat(timespec="seconds")})
        accepted, failures = {}, {}
        for name in ("customers", "products", "stores"):
            accepted[name], failures[name], issues, checks = validate_frame(
                name, raw[name], run_id, DAG_ID)
            all_issues.extend(issues)
            all_checks.extend(checks)
        product_prices = {str(row.product_id): float(row.mrp)
                          for row in accepted["products"].itertuples()}
        refs = {"customers": accepted["customers"]["customer_id"],
                "products": accepted["products"]["product_id"],
                "stores": accepted["stores"]["store_id"],
                "product_prices": product_prices}
        accepted["orders"], failures["orders"], issues, checks = validate_frame(
            "orders", raw["orders"], run_id, DAG_ID, refs)
        all_issues.extend(issues)
        all_checks.extend(checks)
        updates = inserts = 0
        if effective_incremental:
            increment = read_source(data_dir / "orders_2026-10-01_increment.csv", "increment")
            extracted += len(increment)
            inc_ok, inc_bad, issues, checks = validate_frame(
                "orders", increment, run_id, DAG_ID, refs)
            failures["increment"] = inc_bad
            all_issues.extend({**issue, "file": "orders_2026-10-01_increment.csv"}
                              for issue in issues)
            all_checks.extend({**check, "check_name": "increment_" + check["check_name"]}
                              for check in checks)
            existing = set(accepted["orders"]["order_id"])
            updates = int(inc_ok["order_id"].isin(existing).sum())
            inserts = len(inc_ok) - updates
            accepted["orders"] = pd.concat([accepted["orders"], inc_ok], ignore_index=True)
            accepted["orders"] = accepted["orders"].drop_duplicates("order_id", keep="last")
        accepted["payments"], failures["payments"], issues, checks = validate_frame(
            "payments", raw["payments"], run_id, DAG_ID,
            {"orders": accepted["orders"]["order_id"]})
        all_issues.extend(issues)
        all_checks.extend(checks)

        root = data_dir.parents[2]
        for name, failed in failures.items():
            quarantined += len(failed)
            _quarantine(name, failed, run_id, root)

        customer = accepted["customers"].copy()
        customer["masked_email"] = customer["email"].map(mask_email)
        customer["masked_phone"] = customer["phone"].map(mask_phone)
        customer = customer[["customer_id", "masked_email", "masked_phone", "city", "loyalty_tier"]]
        product = accepted["products"].copy()
        for col in ("mrp", "cost_price"):
            product[col] = pd.to_numeric(product[col])
        product = product[["product_id", "product_name", "category", "brand", "mrp", "cost_price"]]
        store = accepted["stores"].copy()
        store["capacity_orders_per_day"] = pd.to_numeric(store["capacity_orders_per_day"])
        store = store[["store_id", "store_name", "zone", "capacity_orders_per_day"]]
        order = accepted["orders"].copy()
        for col in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
            order[col] = pd.to_numeric(order[col], errors="coerce")
        order["order_date"] = pd.to_datetime(order["order_ts"]).dt.date
        fact = order.merge(customer[["customer_id", "loyalty_tier"]], on="customer_id")
        fact = fact.merge(product[["product_id", "category", "cost_price"]], on="product_id")
        fact = fact.merge(store[["store_id", "store_name", "zone"]], on="store_id")
        fact["net_revenue"] = fact["order_amount"].where(fact["order_status"] == "DELIVERED", 0.0)
        fact["gross_margin"] = (fact["net_revenue"] - fact["quantity"] * fact["cost_price"]).where(
            fact["order_status"] == "DELIVERED", 0.0)
        fact["within_15_min"] = ((fact["order_status"] == "DELIVERED")
                                 & (fact["delivery_minutes"] <= 15))
        fact = fact[["order_id", "order_ts", "order_date", "customer_id", "store_id",
                     "product_id", "quantity", "unit_price", "discount_pct", "order_amount",
                     "payment_mode", "order_status", "delivery_minutes", "category",
                     "store_name", "zone", "loyalty_tier", "cost_price", "net_revenue",
                     "gross_margin", "within_15_min"]]
        loaded = len(fact)

        with warehouse_connection(warehouse) as con:
            con.begin()
            try:
                ensure_audit_tables(con)
                con.execute("CREATE TABLE IF NOT EXISTS etl_run_state (incremental_applied BOOLEAN)")
                if effective_incremental:
                    con.execute("DELETE FROM etl_run_state")
                    con.execute("INSERT INTO etl_run_state VALUES (true)")
                for table, frame in (("etl_dim_customer", customer), ("etl_dim_product", product),
                                     ("etl_dim_store", store), ("etl_fact_orders", fact)):
                    _load_frame(con, table, frame)
                payments = accepted["payments"].copy()
                payments["amount_paid"] = pd.to_numeric(payments["amount_paid"])
                _load_frame(con, "etl_clean_payments", payments)
                con.execute("""CREATE OR REPLACE TABLE etl_payment_reconciliation AS
                    WITH successful AS (
                      SELECT order_id, SUM(amount_paid) amount_paid
                      FROM etl_clean_payments WHERE payment_status = 'SUCCESS' GROUP BY 1)
                    SELECT f.order_id, f.order_amount, s.amount_paid,
                      CASE WHEN s.order_id IS NULL THEN 'missing_success_payment'
                           WHEN ABS(f.order_amount-s.amount_paid)>0.01 THEN 'amount_mismatch'
                      END AS reason
                    FROM etl_fact_orders f LEFT JOIN successful s USING(order_id)
                    WHERE f.order_status='DELIVERED' AND
                          (s.order_id IS NULL OR ABS(f.order_amount-s.amount_paid)>0.01)
                    UNION ALL
                    SELECT p.order_id, NULL, p.amount_paid, 'orphan_payment'
                    FROM etl_clean_payments p LEFT JOIN etl_fact_orders f USING(order_id)
                    WHERE f.order_id IS NULL""")
                con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                    SELECT order_date, zone, store_id, store_name,
                           ROUND(SUM(net_revenue),2) net_revenue,
                           COUNT(*) FILTER (WHERE order_status='DELIVERED') delivered_orders
                    FROM etl_fact_orders GROUP BY 1,2,3,4""")
                con.execute("""CREATE OR REPLACE TABLE etl_category_margin AS
                    SELECT category, ROUND(SUM(net_revenue),2) net_revenue,
                           ROUND(SUM(gross_margin),2) gross_margin
                    FROM etl_fact_orders GROUP BY 1""")
                con.execute("""CREATE OR REPLACE TABLE etl_store_capacity AS
                    SELECT f.order_date, f.store_id, COUNT(*) order_count, s.capacity_orders_per_day,
                           COUNT(*) > s.capacity_orders_per_day AS over_capacity
                    FROM etl_fact_orders f JOIN etl_dim_store s USING(store_id)
                    GROUP BY 1,2,4""")
                _append_quality(con, all_checks)
                if all_issues:
                    _load_frame(con, "etl_data_issues_latest", pd.DataFrame(all_issues))
                record_run(con, run_id, DAG_ID, start, extracted, loaded, quarantined, "SUCCESS",
                           f"incremental={effective_incremental}; inserted={inserts}; updated={updates}")
                con.commit()
            except Exception:
                con.rollback()
                raise
        print(f"ETL run {run_id}: extracted={extracted}, loaded={loaded}, quarantined={quarantined}, "
              f"increment inserted={inserts}, updated={updates}")
        return {"extracted": extracted, "loaded": loaded, "quarantined": quarantined,
                "inserted": inserts, "updated": updates}
    except Exception as exc:
        with warehouse_connection(warehouse) as con:
            record_run(con, run_id, DAG_ID, start, extracted, loaded, quarantined,
                       "FAILED", f"{type(exc).__name__}: {exc}"[:500])
        raise
