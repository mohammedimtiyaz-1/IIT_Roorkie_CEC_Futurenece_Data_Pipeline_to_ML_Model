"""Export reproducible, PII-free business answers and a quality trend chart."""

from __future__ import annotations

from pathlib import Path

import duckdb

from include.nammamart_etl import ROOT, WAREHOUSE


RESULTS = ROOT / "evidence" / "results"
QUERIES = {
    "q1_daily_revenue": {
        "etl": "SELECT order_date,zone,store_id,store_name,net_revenue FROM etl_daily_zone_revenue ORDER BY 1,3",
        "elt": "SELECT order_date,zone,store_id,store_name,net_revenue FROM mart_daily_store_kpis ORDER BY 1,3",
    },
    "q2_category_margin": {
        "etl": "SELECT category,net_revenue,gross_margin FROM etl_category_margin ORDER BY 1",
        "elt": "SELECT category,net_revenue,gross_margin FROM mart_category_margin ORDER BY 1",
    },
    "q3_delivery_sla": {
        "etl": """SELECT store_id,store_name,
          ROUND(AVG(delivery_minutes) FILTER (WHERE order_status='DELIVERED'),2) avg_delivery_minutes,
          ROUND(100.0*COUNT(*) FILTER (WHERE within_15_min)/NULLIF(COUNT(*) FILTER
            (WHERE order_status='DELIVERED'),0),2) within_15_pct,
          COUNT(*) FILTER (WHERE order_status='DELIVERED') delivered_orders
          FROM etl_fact_orders GROUP BY 1,2 ORDER BY 1""",
        "elt": "SELECT * FROM mart_delivery_sla ORDER BY store_id",
    },
    "q4_payment_mode_and_gap": {
        "etl": """SELECT payment_mode,ROUND(SUM(net_revenue),2) revenue,
          (SELECT ROUND(SUM(ABS(COALESCE(order_amount,0)-COALESCE(amount_paid,0))),2)
           FROM etl_payment_reconciliation) unreconciled_amount
          FROM etl_fact_orders GROUP BY 1 ORDER BY 1""",
        "elt": """SELECT payment_mode,revenue,
          (SELECT ROUND(SUM(ABS(COALESCE(order_amount,0)-COALESCE(amount_paid,0))),2)
           FROM mart_payment_reconciliation) unreconciled_amount
          FROM mart_payment_mode_revenue ORDER BY 1""",
    },
    "q5_top_customers": {
        "etl": """SELECT customer_id,loyalty_tier,ROUND(SUM(net_revenue),2) spend,
          COUNT(*) FILTER (WHERE order_status='DELIVERED') delivered_orders
          FROM etl_fact_orders GROUP BY 1,2 ORDER BY spend DESC LIMIT 10""",
        "elt": "SELECT * FROM mart_top_customers ORDER BY spend DESC",
    },
    "q6_rejections": {
        "etl": """SELECT file,problem AS failure_reason,COUNT(DISTINCT row_key) rejected_rows
          FROM etl_data_issues_latest WHERE severity='Critical'
          GROUP BY 1,2 ORDER BY 1,2""",
        "elt": "SELECT file,failure_reason,rejected_rows FROM mart_rejections ORDER BY 1,2",
    },
}


def export(warehouse: Path = WAREHOUSE, results: Path = RESULTS):
    results.mkdir(parents=True, exist_ok=True)
    lines = ["# NammaMart query evidence", "",
             "Generated from the local DuckDB warehouse. The CSVs contain no names, email addresses or phone numbers.", ""]
    with duckdb.connect(str(warehouse), read_only=True) as con:
        etl = con.execute("SELECT COUNT(*),ROUND(SUM(net_revenue),2) FROM etl_fact_orders").fetchone()
        elt = con.execute("SELECT COUNT(*),ROUND(SUM(net_revenue),2) FROM mart_fact_orders").fetchone()
        if etl != elt:
            raise AssertionError(f"ETL/ELT totals differ: {etl} vs {elt}")
        lines.extend([f"Current accepted facts: {etl[0]}; net revenue: ₹{etl[1]:,.2f} in each pipeline.", ""])
        for name, variants in QUERIES.items():
            lines.extend([f"## {name}", ""])
            for pipeline, sql in variants.items():
                frame = con.execute(sql).df()
                destination = results / f"{name}_{pipeline}.csv"
                frame.to_csv(destination, index=False)
                lines.extend([f"### {pipeline.upper()}", "", "```sql", sql.strip(), "```", "",
                              f"Result: {len(frame)} rows in [`{destination.name}`]({destination.name}).", ""])
                if len(frame):
                    lines.extend(["First three rows:", "", "```text", frame.head(3).to_string(index=False), "```", ""])
        audit = con.execute("""SELECT run_id,dag_id,start_time,end_time,rows_extracted,
          rows_loaded,rows_quarantined,status,details FROM pipeline_runs ORDER BY end_time""").df()
        audit.to_csv(results / "pipeline_runs.csv", index=False)
        quality = con.execute("SELECT * FROM dq_results ORDER BY checked_at,dag_id,check_name").df()
        quality.to_csv(results / "dq_results.csv", index=False)
        reconciliation = con.execute("SELECT * FROM mart_payment_reconciliation ORDER BY order_id").df()
        reconciliation.to_csv(results / "payment_reconciliation.csv", index=False)
        trend = con.execute("""SELECT run_id,dag_id,SUM(rows_failed) failed_checks
          FROM dq_results GROUP BY 1,2 ORDER BY run_id""").fetchall()
    (results / "QUERY_RESULTS.md").write_text("\n".join(lines), encoding="utf-8")
    _write_trend_svg(results.parent / "dq_trend.svg", trend)
    print(f"Exported 12 query CSVs, audit/quality/reconciliation CSVs, and trend chart to {results.parent}")


def _write_trend_svg(path: Path, trend):
    # PII-free generated chart, using only numeric check totals and run labels.
    points = trend[-12:]
    width, height = 900, 350
    maximum = max((row[2] for row in points), default=1) or 1
    x = lambda i: 70 + i * (width - 110) / max(len(points) - 1, 1)
    y = lambda value: height - 65 - value * (height - 115) / maximum
    polyline = " ".join(f"{x(i):.1f},{y(row[2]):.1f}" for i, row in enumerate(points))
    labels = "".join(
        f'<text x="{x(i):.1f}" y="{height-35}" text-anchor="middle" font-size="10">'
        f'{row[1].replace("nammamart_", "").replace("_imtiyaz", "")} {i+1}</text>'
        for i, row in enumerate(points))
    dots = "".join(f'<circle cx="{x(i):.1f}" cy="{y(row[2]):.1f}" r="5" fill="#5b3cc4"/>'
                   for i, row in enumerate(points))
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
           f'viewBox="0 0 {width} {height}"><rect width="100%" height="100%" fill="white"/>'
           f'<text x="40" y="28" font-size="18">NammaMart data-quality failures by run</text>'
           f'<line x1="60" y1="{height-65}" x2="{width-25}" y2="{height-65}" stroke="#666"/>'
           f'<line x1="60" y1="45" x2="60" y2="{height-65}" stroke="#666"/>'
           f'<polyline points="{polyline}" fill="none" stroke="#5b3cc4" stroke-width="3"/>'
           f'{dots}{labels}<text x="16" y="50" font-size="12">{maximum}</text></svg>')
    path.write_text(svg, encoding="utf-8")


if __name__ == "__main__":
    export()
