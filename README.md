# Databricks BI Assignment ETL Repo

This repo contains the final Databricks/PySpark implementation aligned to the technical write-up.

## Runtime configuration

`config/config.yml`

```yaml
raw_base_path: /Volumes/workspace/bi_assignment/etl_assignment/data/raw
target_base_path: /Volumes/workspace/bi_assignment/etl_assignment/output
target_catalog: workspace
target_schema: bi_assignment
```

## Main notebook/script

`notebooks/01_end_to_end_etl_source.py`

## Layers and table naming

### Bronze
- bronze_customer
- bronze_orders
- bronze_product
- bronze_category
- bronze_currency_rate

### Silver
- silver_customer
- silver_category
- silver_product
- silver_currency_rate
- silver_orders
- silver_product_category
- silver_customer_current
- silver_order_line_enriched
- silver_faulty_transactions
- silver_duplicate_order_lines

### Gold / BI
- dim_customer
- dim_product
- dim_currency_rate
- dim_date
- fact_order_line
- audit_duplicate_order_lines
- audit_faulty_transactions

## Analytics CSV output

The script writes each analytics result to:

`/Volumes/workspace/bi_assignment/etl_assignment/output/<metric_name>`
