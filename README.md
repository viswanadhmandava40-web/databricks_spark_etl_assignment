# Databricks Spark BI ETL Assignment

This version is implemented for **Databricks using PySpark DataFrames, Spark SQL, and Delta tables**. It does not use pandas for the ETL.

## Files

- `src/databricks_spark_etl.py` - Databricks notebook/script containing bronze, silver, gold ETL and analytics execution.
- `notebooks/01_end_to_end_etl.py` - same notebook-style code, ready to import into Databricks.
- `sql/analytics.sql` - required BI SQL queries against target Delta tables.
- `docs/technical_writeup.md` - model/design, mapping, data quality, and scale trade-offs.
- `conf/databricks_workflow.yml` - Databricks Asset Bundle workflow example.
- `data/raw/` - provided source files.
- `outputs/` - sample expected metric outputs from the provided source data.

## Databricks run steps

1. Upload raw files to DBFS, for example:
   `/Volumes/workspace/bi_assignment/etl_assignment/data/raw/customer.csv`
   `/Volumes/workspace/bi_assignment/etl_assignment/data/raw/orders.csv`
   `/Volumes/workspace/bi_assignment/etl_assignment/data/raw/product.csv`
   `/Volumes/workspace/bi_assignment/etl_assignment/data/raw/prod_cat_tree.csv`
   `/Volumes/workspace/bi_assignment/etl_assignment/data/raw/currency_conversion.json`

2. Import `notebooks/01_end_to_end_etl.py` or `src/databricks_spark_etl.py` into Databricks.

3. Set notebook parameters:
   - `raw_base_path=/Volumes/workspace/bi_assignment/etl_assignment/data/raw`
   - `target_catalog=workspace`
   - `target_schema=bi_assignment`
   - `target_base_path=/Volumes/workspace/bi_assignment/etl_assignment/tmp`

4. Run the notebook. It creates Delta tables under:
   `workspace.bi_assignment`

5. Run `sql/analytics.sql` in Databricks SQL for BI metrics.

## AI usage statement

AI assistance was used to structure the ETL solution, create the Spark/Databricks implementation, prepare SQL metrics, and draft the technical documentation. The source data, transformation logic, and quality checks were validated from the provided assignment files.


## Updated Databricks Unity Catalog settings

This repo is configured with the following defaults:

```text
raw_base_path    = /Volumes/workspace/bi_assignment/etl_assignment/data/raw
target_base_path = /Volumes/workspace/bi_assignment/etl_assignment/tmp
target_catalog   = workspace
target_schema    = bi_assignment
```

Target tables are created under:

```text
workspace.bi_assignment
```

Upload the five source files into:

```text
/Volumes/workspace/bi_assignment/etl_assignment/data/raw
```

Run `sql/00_setup_catalog_schema_volume.sql` first only if the catalog/schema/volume are not already created and your workspace permits those commands.
