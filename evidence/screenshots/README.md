# Airflow screenshots

The following authentic Airflow graph exports were downloaded on 12 October 2026 and visually checked. Both show every task in the selected run as successful:

- [ETL green task graph](etl_graph_success.png)
- [ELT green task graph](elt_graph_success.png)
- [ETL extraction retry in progress](etl_graph_retry.png) — `extract_products` is `up_for_retry`; downstream tasks are not yet complete. This is a failure-in-progress view, not a failed-run or recovered-run screenshot.

These graph exports omit the surrounding Airflow page, so they do **not** prove the schedule or next-run time. The schedule (`0 9 * * 1-6`) and next run (13 October 2026, 09:00 IST) were observed in Airflow and recorded in `SUBMISSION.md`, but a page screenshot was not saved. The exported CSVs for the quality scorecard, run audit, and payment reconciliation are real query results, not UI screenshots. A screenshot of a row-level quarantine view has not been saved. Do not describe the full screenshot checklist as complete.

Still to capture if possible: schedule and next-run time; one local quarantine view (with customer details obscured); query results from `dq_results`, `pipeline_runs`, and payment reconciliation. The local hostname and Airflow run IDs are not public links.
