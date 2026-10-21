"""Repeatable, isolated experiments against both real pipeline implementations."""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from include.nammamart_etl import DATA, run_etl
from include.nammamart_elt import run_elt


RUNNERS = {"etl": run_etl, "elt": run_elt}
FACT = {"etl": "etl_fact_orders", "elt": "mart_fact_orders"}
DAG = {"etl": "nammamart_etl_imtiyaz", "elt": "nammamart_elt_imtiyaz"}
TEST_AS_OF = datetime(2026, 10, 12, 12, 0, 0)


@pytest.fixture
def setup(tmp_path):
    data = tmp_path / "include" / "data" / "nammamart"
    data.mkdir(parents=True)
    for source in DATA.glob("*.csv"):
        shutil.copy2(source, data / source.name)
    return data, tmp_path / "include" / "warehouse_imtiyaz.duckdb"


def execute(kind, setup, run_id, incremental=False):
    data, warehouse = setup
    return RUNNERS[kind](data_dir=data, warehouse=warehouse,
                         run_id=f"{kind}_{run_id}", incremental=incremental,
                         as_of=TEST_AS_OF)


def query(setup, sql, parameters=None):
    with duckdb.connect(str(setup[1]), read_only=True) as con:
        return con.execute(sql, parameters or []).fetchall()


@pytest.mark.parametrize("kind", RUNNERS)
def test_01_happy_path(kind, setup):
    outcome = execute(kind, setup, "happy")
    assert outcome["extracted"] == 5212
    assert outcome["loaded"] == 1896
    assert query(setup, f"SELECT ROUND(SUM(net_revenue),2) FROM {FACT[kind]}") == [(930779.95,)]
    assert query(setup, "SELECT status FROM pipeline_runs WHERE run_id=?", [f"{kind}_happy"]) == [("SUCCESS",)]


@pytest.mark.parametrize("kind", RUNNERS)
def test_02_rerun_idempotency(kind, setup):
    execute(kind, setup, "first")
    before = query(setup, f"SELECT COUNT(*),ROUND(SUM(net_revenue),2) FROM {FACT[kind]}")
    execute(kind, setup, "second")
    assert query(setup, f"SELECT COUNT(*),ROUND(SUM(net_revenue),2) FROM {FACT[kind]}") == before
    if kind == "elt":
        assert query(setup, "SELECT COUNT(*) FROM raw_orders") == [(4824,)]
    execute(kind, setup, "first")  # Airflow Clear reuses the original run_id.
    assert query(setup, "SELECT COUNT(*) FROM pipeline_runs WHERE run_id=?",
                 [f"{kind}_first"]) == [(1,)]
    assert query(setup, f"SELECT COUNT(*),ROUND(SUM(net_revenue),2) FROM {FACT[kind]}") == before


@pytest.mark.parametrize("kind", RUNNERS)
def test_03_incremental_new_and_corrected(kind, setup):
    execute(kind, setup, "baseline")
    outcome = execute(kind, setup, "increment", incremental=True)
    assert outcome["loaded"] == 2021
    assert query(setup, f"SELECT ROUND(SUM(net_revenue),2) FROM {FACT[kind]}") == [(991180.8,)]
    assert execute(kind, setup, "scheduled_after_increment")["loaded"] == 2021
    if kind == "etl":
        assert outcome["inserted"] == 125 and outcome["updated"] == 7
    else:
        assert query(setup, "SELECT COUNT(*) FROM raw_orders WHERE _source_file LIKE '%increment%'") == [(160,)]
        assert query(setup, "SELECT COUNT(*) FROM stg_orders WHERE _source_file LIKE '%increment%'") == [(160,)]


@pytest.mark.parametrize("kind", RUNNERS)
def test_04_missing_source_fails_and_is_audited(kind, setup):
    (setup[0] / "products.csv").unlink()
    with pytest.raises(FileNotFoundError, match="products.csv"):
        execute(kind, setup, "missing")
    assert query(setup, "SELECT status FROM pipeline_runs WHERE run_id=?", [f"{kind}_missing"]) == [("FAILED",)]


@pytest.mark.parametrize("kind", RUNNERS)
def test_05_new_bad_rows_are_rejected(kind, setup):
    path = setup[0] / "orders.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    template = frame.iloc[0].copy()
    additions = []
    for i, change in enumerate((
        {"customer_id": ""}, {"quantity": "-1"}, {"store_id": "UNKNOWN"},
        {"order_status": "SHIPPED"}, {"order_ts": "2027-01-01 09:00:00"}), start=1):
        row = template.copy()
        row["order_id"] = f"BAD{i:03d}"
        for column, value in change.items():
            row[column] = value
        additions.append(row)
    pd.concat([frame, pd.DataFrame(additions)], ignore_index=True).to_csv(path, index=False)
    execute(kind, setup, "bad_rows")
    assert query(setup, f"SELECT COUNT(*) FROM {FACT[kind]} WHERE order_id LIKE 'BAD%'") == [(0,)]
    table = "etl_data_issues_latest" if kind == "etl" else "elt_quarantine"
    column = "row_key"
    assert query(setup, f"SELECT COUNT(DISTINCT {column}) FROM {table} WHERE {column} LIKE 'BAD%'") == [(5,)]
    for row_key, reason in (("BAD001", "required_customer"),
                            ("BAD002", "positive_quantity_price"),
                            ("BAD003", "known_store_id"),
                            ("BAD004", "valid_order_status"),
                            ("BAD005", "not_future_order")):
        reason_column = "problem" if kind == "etl" else "failure_reason"
        assert query(setup, f"SELECT COUNT(*) FROM {table} WHERE row_key=? AND {reason_column} LIKE ?",
                     [row_key, f"%{reason}%"])[0][0] > 0


@pytest.mark.parametrize("kind", RUNNERS)
def test_06_schema_change_fails_loudly(kind, setup):
    path = setup[0] / "orders.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False).rename(columns={"order_amount": "total"})
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="Schema mismatch"):
        execute(kind, setup, "schema")
    assert query(setup, "SELECT status FROM pipeline_runs WHERE run_id=?", [f"{kind}_schema"]) == [("FAILED",)]


@pytest.mark.parametrize("kind", RUNNERS)
def test_07_volume_warning(kind, setup):
    path = setup[0] / "orders.csv"
    pd.read_csv(path, dtype=str, keep_default_na=False).head(50).to_csv(path, index=False)
    execute(kind, setup, "volume")
    assert query(setup, "SELECT rows_failed FROM dq_results WHERE run_id=? AND check_name='orders_volume'",
                 [f"{kind}_volume"]) == [(1,)]


@pytest.mark.parametrize("kind", RUNNERS)
def test_08_price_distribution_detected(kind, setup):
    execute(kind, setup, "drift_baseline")
    before_s01 = query(setup, f"SELECT ROUND(COALESCE(SUM(net_revenue),0),2) FROM {FACT[kind]} "
                              "WHERE store_id='S01'")[0][0]
    path = setup[0] / "orders.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    frame.loc[frame["store_id"] == "S01", "unit_price"] = (
        pd.to_numeric(frame.loc[frame["store_id"] == "S01", "unit_price"]) * 100).astype(str)
    frame.to_csv(path, index=False)
    execute(kind, setup, "drift")
    check = "price_distribution" if kind == "etl" else "orders_price_distribution"
    assert query(setup, "SELECT rows_failed>0 FROM dq_results WHERE run_id=? AND check_name=?",
                 [f"{kind}_drift", check]) == [(True,)]
    after_s01 = query(setup, f"SELECT ROUND(COALESCE(SUM(net_revenue),0),2) FROM {FACT[kind]} "
                             "WHERE store_id='S01'")[0][0]
    print(f"{kind} S01 revenue before={before_s01}, after={after_s01}, change={after_s01-before_s01}")
    assert after_s01 < before_s01


@pytest.mark.parametrize("kind", RUNNERS)
def test_09_stale_orders_warn(kind, setup):
    path = setup[0] / "orders.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    frame["order_ts"] = "2026-08-01 09:00:00"
    frame.to_csv(path, index=False)
    execute(kind, setup, "stale")
    assert query(setup, "SELECT rows_failed FROM dq_results WHERE run_id=? AND check_name='orders_freshness'",
                 [f"{kind}_stale"]) == [(1,)]


@pytest.mark.parametrize("kind", RUNNERS)
def test_10_recovery_after_restoring_source(kind, setup):
    path = setup[0] / "products.csv"
    path.unlink()
    with pytest.raises(FileNotFoundError):
        execute(kind, setup, "recovery_failed")
    shutil.copy2(DATA / "products.csv", path)
    outcome = execute(kind, setup, "recovery_success")
    assert outcome["loaded"] == 1896
    assert query(setup, "SELECT status FROM pipeline_runs WHERE run_id IN (?,?) ORDER BY run_id",
                 [f"{kind}_recovery_failed", f"{kind}_recovery_success"]) == [("FAILED",), ("SUCCESS",)]
