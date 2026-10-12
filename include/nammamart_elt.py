"""Append-only raw ingestion and SQL-only ELT transformations."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from include.nammamart_etl import DATA, FILES, ROOT, WAREHOUSE, ensure_audit_tables, record_run, warehouse_connection
from include.nammamart_quality import COLUMNS


DAG_ID = "nammamart_elt_imtiyaz"
SQL = ROOT / "include" / "sql"


def _append_raw(con, file_path: Path, name: str, run_id: str) -> int:
    if not file_path.is_file():
        raise FileNotFoundError(f"Required source file is missing: {file_path.name}")
    expected = COLUMNS["orders" if name == "increment" else name]
    table_name = "orders" if name == "increment" else name
    raw_table = f"raw_{table_name}"
    columns_sql = ", ".join(f'"{column}" VARCHAR' for column in expected)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {raw_table} (
        {columns_sql}, _source_file VARCHAR, _source_row BIGINT,
        _run_id VARCHAR, _loaded_at TIMESTAMP)""")
    with file_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != expected:
            raise ValueError(f"Schema mismatch in {file_path.name}: expected {expected}; got {reader.fieldnames}")
        rows = []
        loaded_at = datetime.now()
        for source_row, row in enumerate(reader, start=2):
            rows.append([row[column] for column in expected]
                        + [file_path.name, source_row, run_id, loaded_at])
    placeholders = ",".join("?" for _ in range(len(expected) + 4))
    con.executemany(f"INSERT INTO {raw_table} VALUES ({placeholders})", rows)
    return len(rows)


def run_elt(data_dir: Path = DATA, warehouse: Path = WAREHOUSE,
            run_id: Optional[str] = None, incremental: bool = False) -> Dict[str, int]:
    """Ingest raw records, run SQL layers, and record actual outcome."""
    run_id = run_id or "manual_" + datetime.now().strftime("%Y%m%dT%H%M%S")
    start = datetime.now()
    extracted = loaded = quarantined = 0
    try:
        with warehouse_connection(warehouse) as con:
            con.begin()
            try:
                ensure_audit_tables(con)
                con.execute("CREATE OR REPLACE TEMP TABLE run_context AS SELECT ?::VARCHAR run_id, ?::VARCHAR dag_id",
                            [run_id, DAG_ID])
                for name in FILES:
                    extracted += _append_raw(con, data_dir / f"{name}.csv", name, run_id)
                if incremental:
                    extracted += _append_raw(con, data_dir / "orders_2026-10-01_increment.csv", "increment", run_id)
                for filename in ("01_staging.sql", "02_marts.sql", "03_quality.sql"):
                    con.execute((SQL / filename).read_text(encoding="utf-8"))
                loaded = con.execute("SELECT COUNT(*) FROM mart_fact_orders").fetchone()[0]
                quarantined = con.execute("SELECT COUNT(*) FROM elt_quarantine").fetchone()[0]
                record_run(con, run_id, DAG_ID, start, extracted, loaded, quarantined,
                           "SUCCESS", f"incremental={incremental}")
                con.commit()
            except Exception:
                con.rollback()
                raise
        print(f"ELT run {run_id}: extracted={extracted}, loaded={loaded}, quarantined={quarantined}")
        return {"extracted": extracted, "loaded": loaded, "quarantined": quarantined}
    except Exception as exc:
        with warehouse_connection(warehouse) as con:
            record_run(con, run_id, DAG_ID, start, extracted, loaded, quarantined,
                       "FAILED", f"{type(exc).__name__}: {exc}"[:500])
        raise
