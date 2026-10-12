# Airflow screenshots

The following authentic Airflow graph exports were downloaded on 12 October 2026 and visually checked. Both show every task in the selected run as successful:

- [ETL green task graph](etl_graph_success.png)
- [ELT green task graph](elt_graph_success.png)
- [ETL extraction retry in progress](etl_graph_retry.png) — `extract_products` is `up_for_retry`; downstream tasks are not yet complete. This is a failure-in-progress view, not a failed-run or recovered-run screenshot.

The following full Airflow page captures show each enabled DAG's schedule (`0 9 * * 1-6`), successful latest run, next run (13 October 2026, 09:00 IST), and the UI timezone offset `+05:30`:

- [ETL schedule and next run](etl_schedule_next_run.png)
- [ELT schedule and next run](elt_schedule_next_run.png)

The remaining captures show **local previews of actual exported warehouse CSVs**, rendered by [local_preview.html](../results/local_preview.html). They are not Airflow or DuckDB-console screenshots:

- [Product quarantine preview](quarantine_products_preview.png) — all ten rows from the [PII-free product quarantine export](../results/quarantine_products.csv), including `failure_reason`.
- [Quality scorecard preview](dq_results_preview.png) — first 12 rows of [dq_results.csv](../results/dq_results.csv).
- [Run-audit preview](pipeline_runs_preview.png) — first 12 rows of [pipeline_runs.csv](../results/pipeline_runs.csv).
- [Payment reconciliation preview](payment_reconciliation_preview.png) — first 12 rows of [payment_reconciliation.csv](../results/payment_reconciliation.csv).

The exported CSVs, not the preview images, are the complete query results. The local hostname and Airflow run IDs are not public links. The screenshots substantiate local execution and exported evidence; they do not establish a production 9 AM SLA or an LMS grading result.
