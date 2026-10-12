# NammaMart query evidence

Generated from the local DuckDB warehouse. The CSVs contain no names, email addresses or phone numbers.

Current accepted facts: 2021; net revenue: ₹991,180.80 in each pipeline.

## q1_daily_revenue

### ETL

```sql
SELECT order_date,zone,store_id,store_name,net_revenue FROM etl_daily_zone_revenue ORDER BY 1,3
```

Result: 400 rows in [`q1_daily_revenue_etl.csv`](q1_daily_revenue_etl.csv).

First three rows:

```text
order_date  zone store_id          store_name  net_revenue
2026-09-01 North      S01    NammaMart Hebbal      1410.80
2026-09-01 North      S02 NammaMart Yelahanka      2385.55
2026-09-01 North      S03  NammaMart RT Nagar      4583.80
```

### ELT

```sql
SELECT order_date,zone,store_id,store_name,net_revenue FROM mart_daily_store_kpis ORDER BY 1,3
```

Result: 400 rows in [`q1_daily_revenue_elt.csv`](q1_daily_revenue_elt.csv).

First three rows:

```text
order_date  zone store_id          store_name  net_revenue
2026-09-01 North      S01    NammaMart Hebbal      1410.80
2026-09-01 North      S02 NammaMart Yelahanka      2385.55
2026-09-01 North      S03  NammaMart RT Nagar      4583.80
```

## q2_category_margin

### ETL

```sql
SELECT category,net_revenue,gross_margin FROM etl_category_margin ORDER BY 1
```

Result: 6 rows in [`q2_category_margin_etl.csv`](q2_category_margin_etl.csv).

First three rows:

```text
           category  net_revenue  gross_margin
     Dairy & Bakery    158331.05      42097.19
Fruits & Vegetables     81109.20      19116.58
          Household    178978.35      45340.69
```

### ELT

```sql
SELECT category,net_revenue,gross_margin FROM mart_category_margin ORDER BY 1
```

Result: 6 rows in [`q2_category_margin_elt.csv`](q2_category_margin_elt.csv).

First three rows:

```text
           category  net_revenue  gross_margin
     Dairy & Bakery    158331.05      42097.19
Fruits & Vegetables     81109.20      19116.58
          Household    178978.35      45340.69
```

## q3_delivery_sla

### ETL

```sql
SELECT store_id,store_name,
          ROUND(AVG(delivery_minutes) FILTER (WHERE order_status='DELIVERED'),2) avg_delivery_minutes,
          ROUND(100.0*COUNT(*) FILTER (WHERE within_15_min)/NULLIF(COUNT(*) FILTER
            (WHERE order_status='DELIVERED'),0),2) within_15_pct,
          COUNT(*) FILTER (WHERE order_status='DELIVERED') delivered_orders
          FROM etl_fact_orders GROUP BY 1,2 ORDER BY 1
```

Result: 13 rows in [`q3_delivery_sla_etl.csv`](q3_delivery_sla_etl.csv).

First three rows:

```text
store_id          store_name  avg_delivery_minutes  within_15_pct  delivered_orders
     S01    NammaMart Hebbal                 20.62          31.21               141
     S02 NammaMart Yelahanka                 20.99          30.94               139
     S03  NammaMart RT Nagar                 20.71          29.20               137
```

### ELT

```sql
SELECT * FROM mart_delivery_sla ORDER BY store_id
```

Result: 13 rows in [`q3_delivery_sla_elt.csv`](q3_delivery_sla_elt.csv).

First three rows:

```text
store_id          store_name  avg_delivery_minutes  within_15_pct  delivered_orders
     S01    NammaMart Hebbal                 20.62          31.21               141
     S02 NammaMart Yelahanka                 20.99          30.94               139
     S03  NammaMart RT Nagar                 20.71          29.20               137
```

## q4_payment_mode_and_gap

### ETL

```sql
SELECT payment_mode,ROUND(SUM(net_revenue),2) revenue,
          (SELECT ROUND(SUM(ABS(COALESCE(order_amount,0)-COALESCE(amount_paid,0))),2)
           FROM etl_payment_reconciliation) unreconciled_amount
          FROM etl_fact_orders GROUP BY 1 ORDER BY 1
```

Result: 4 rows in [`q4_payment_mode_and_gap_etl.csv`](q4_payment_mode_and_gap_etl.csv).

First three rows:

```text
    payment_mode   revenue  unreconciled_amount
            Card 203773.15             103372.7
Cash on Delivery 139249.40             103372.7
             UPI 534319.35             103372.7
```

### ELT

```sql
SELECT payment_mode,revenue,
          (SELECT ROUND(SUM(ABS(COALESCE(order_amount,0)-COALESCE(amount_paid,0))),2)
           FROM mart_payment_reconciliation) unreconciled_amount
          FROM mart_payment_mode_revenue ORDER BY 1
```

Result: 4 rows in [`q4_payment_mode_and_gap_elt.csv`](q4_payment_mode_and_gap_elt.csv).

First three rows:

```text
    payment_mode   revenue  unreconciled_amount
            Card 203773.15             103372.7
Cash on Delivery 139249.40             103372.7
             UPI 534319.35             103372.7
```

## q5_top_customers

### ETL

```sql
SELECT customer_id,loyalty_tier,ROUND(SUM(net_revenue),2) spend,
          COUNT(*) FILTER (WHERE order_status='DELIVERED') delivered_orders
          FROM etl_fact_orders GROUP BY 1,2 ORDER BY spend DESC LIMIT 10
```

Result: 10 rows in [`q5_top_customers_etl.csv`](q5_top_customers_etl.csv).

First three rows:

```text
customer_id loyalty_tier   spend  delivered_orders
     C00115       BRONZE 7337.70                 7
     C00310       BRONZE 7258.95                 8
     C00265         GOLD 6940.30                 7
```

### ELT

```sql
SELECT * FROM mart_top_customers ORDER BY spend DESC
```

Result: 10 rows in [`q5_top_customers_elt.csv`](q5_top_customers_elt.csv).

First three rows:

```text
customer_id loyalty_tier   spend  delivered_orders
     C00115       BRONZE 7337.70                 7
     C00310       BRONZE 7258.95                 8
     C00265         GOLD 6940.30                 7
```

## q6_rejections

### ETL

```sql
SELECT file,problem AS failure_reason,COUNT(DISTINCT row_key) rejected_rows
          FROM etl_data_issues_latest WHERE severity='Critical'
          GROUP BY 1,2 ORDER BY 1,2
```

Result: 30 rows in [`q6_rejections_etl.csv`](q6_rejections_etl.csv).

First three rows:

```text
         file        failure_reason  rejected_rows
customers.csv     not_future_signup              3
customers.csv            unique_key              5
   orders.csv delivered_has_minutes              8
```

### ELT

```sql
SELECT file,failure_reason,rejected_rows FROM mart_rejections ORDER BY 1,2
```

Result: 30 rows in [`q6_rejections_elt.csv`](q6_rejections_elt.csv).

First three rows:

```text
         file        failure_reason  rejected_rows
customers.csv     not_future_signup              3
customers.csv            unique_key              5
   orders.csv delivered_has_minutes              8
```
