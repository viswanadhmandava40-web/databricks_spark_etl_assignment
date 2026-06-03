# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
"""
End-to-end Databricks / PySpark ETL pipeline for BI assignment.

This notebook follows the technical write-up conventions:
1. Bronze Layer  - raw ingestion with minimal changes and audit columns
2. Silver Layer  - cleaned, standardized, conformed data quality-ready tables
3. Gold Layer    - dimensional model for BI analytics
4. Analytics     - SQL metrics exported as CSV files

Expected config/config.yml values:
raw_base_path: /Volumes/workspace/bi_assignment/etl_assignment/data/raw
target_base_path: /Volumes/workspace/bi_assignment/etl_assignment/output
target_catalog: workspace
target_schema: bi_assignment

Source files expected under raw_base_path:
- customer.csv
- orders.csv
- product.csv
- prod_cat_tree.csv
- currency_conversion.json
"""

from typing import Dict
import re

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# -----------------------------------------------------------------------------
# 0. Runtime configuration
# -----------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "raw_base_path": "/Volumes/workspace/bi_assignment/etl_assignment/data/raw",
    "target_base_path": "/Volumes/workspace/bi_assignment/etl_assignment/output",
    "target_catalog": "workspace",
    "target_schema": "bi_assignment",
}


def _read_simple_yaml_config(path: str) -> Dict[str, str]:
    """Read simple key: value YAML without requiring PyYAML on Databricks."""
    cfg = {}
    try:
        # Works in Databricks for workspace/repo file paths.
        with open(path, "r", encoding="utf-8") as file_obj:
            for line in file_obj:
                clean_line = line.strip()
                if not clean_line or clean_line.startswith("#") or ":" not in clean_line:
                    continue
                key, value = clean_line.split(":", 1)
                cfg[key.strip()] = value.strip().strip('"').strip("'")
    except Exception:
        pass
    return cfg


config = DEFAULT_CONFIG.copy()
config.update(_read_simple_yaml_config("../config/config.yml"))
config.update(_read_simple_yaml_config("config/config.yml"))

# Widgets override config values when notebook is run as a Databricks job.
try:
    dbutils.widgets.text("raw_base_path", config["raw_base_path"])
    dbutils.widgets.text("target_base_path", config["target_base_path"])
    dbutils.widgets.text("target_catalog", config["target_catalog"])
    dbutils.widgets.text("target_schema", config["target_schema"])

    raw_base_path = dbutils.widgets.get("raw_base_path")
    target_base_path = dbutils.widgets.get("target_base_path")
    target_catalog = dbutils.widgets.get("target_catalog")
    target_schema = dbutils.widgets.get("target_schema")
except Exception:
    raw_base_path = config["raw_base_path"]
    target_base_path = config["target_base_path"]
    target_catalog = config["target_catalog"]
    target_schema = config["target_schema"]

full_schema = f"{target_catalog}.{target_schema}"

spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE SCHEMA {target_schema}")

print("Runtime configuration")
print(f"raw_base_path    = {raw_base_path}")
print(f"target_base_path = {target_base_path}")
print(f"target_catalog   = {target_catalog}")
print(f"target_schema    = {target_schema}")
print(f"full_schema      = {full_schema}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# 1. Explicit source schemas
# -----------------------------------------------------------------------------

customer_schema = T.StructType([
    T.StructField("cust_id", T.StringType(), True),
    T.StructField("created_at", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("type", T.StringType(), True),
    T.StructField("zip", T.StringType(), True),
    T.StructField("active", T.StringType(), True),
])

orders_schema = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("cust_id", T.StringType(), True),
    T.StructField("order date", T.StringType(), True),
    T.StructField("prod_id", T.StringType(), True),
    T.StructField("quantity", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
])

product_schema = T.StructType([
    T.StructField("prod_id", T.StringType(), True),
    T.StructField("cat_id", T.StringType(), True),
    T.StructField("prod_name", T.StringType(), True),
    T.StructField("price", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
])

category_schema = T.StructType([
    T.StructField("cat_id", T.StringType(), True),
    T.StructField("child", T.StringType(), True),
    T.StructField("parent", T.StringType(), True),
])

currency_schema = T.StructType([
    T.StructField("date", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("conversion", T.DoubleType(), True),
])

# COMMAND ----------

# -----------------------------------------------------------------------------
# 2. Helper functions
# -----------------------------------------------------------------------------


def normalize_column_name(column_name: str) -> str:
    """Convert source column names into Delta/Unity Catalog friendly snake_case."""
    clean_name = column_name.strip().lower().replace(" ", "_").replace("-", "_")
    clean_name = re.sub(r"[^a-zA-Z0-9_]", "", clean_name)
    clean_name = re.sub(r"_+", "_", clean_name).strip("_")
    return clean_name


def standardize_columns(df: DataFrame) -> DataFrame:
    """Standardize all DataFrame column names to avoid Delta invalid character issues."""
    return df.toDF(*[normalize_column_name(col_name) for col_name in df.columns])


def normalize_currency(col):
    """Standardize transaction currencies to match the reference rate file."""
    normalized = F.upper(F.trim(col))
    return (
        F.when(normalized.isin("YEN", "JPY"), F.lit("YEN"))
         .when(normalized.isin("POUND", "GBP"), F.lit("POUND"))
         .when(normalized == "USD", F.lit("USD"))
         .otherwise(normalized)
    )


def save_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Persist a managed Delta table in Unity Catalog."""
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"{full_schema}.{table_name}")
    )
    print(f"Created table: {full_schema}.{table_name}")


def read_csv_with_metadata(file_name: str, schema: T.StructType) -> DataFrame:
    """Read CSV from a UC Volume and add UC-safe file metadata."""
    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(f"{raw_base_path}/{file_name}")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    return standardize_columns(df)


def read_json_with_metadata(file_name: str, schema: T.StructType) -> DataFrame:
    """Read multi-line JSON from a UC Volume and add UC-safe file metadata."""
    df = (
        spark.read
        .option("multiLine", True)
        .schema(schema)
        .json(f"{raw_base_path}/{file_name}")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    return standardize_columns(df)


def write_query_result_to_csv(view_name: str, query: str) -> DataFrame:
    """Run analytics SQL, create temp view, and export result to configured CSV path."""
    result_df = spark.sql(query)
    result_df.createOrReplaceTempView(view_name)

    output_path = f"{target_base_path}/{view_name}"
    (
        result_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    print(f"\n===== {view_name} =====")
    result_df.show(50, truncate=False)
    print(f"CSV written to: {output_path}")
    return result_df

# COMMAND ----------

# -----------------------------------------------------------------------------
# 3. Bronze Layer - raw ingestion
# -----------------------------------------------------------------------------

bronze_customer = read_csv_with_metadata("customer.csv", customer_schema)
bronze_orders = read_csv_with_metadata("orders.csv", orders_schema)
bronze_product = read_csv_with_metadata("product.csv", product_schema)
bronze_category = read_csv_with_metadata("prod_cat_tree.csv", category_schema)
bronze_currency_rate = read_json_with_metadata("currency_conversion.json", currency_schema)

bronze_tables = {
    "bronze_customer": bronze_customer,
    "bronze_orders": bronze_orders,
    "bronze_product": bronze_product,
    "bronze_category": bronze_category,
    "bronze_currency_rate": bronze_currency_rate,
}

for table_name, table_df in bronze_tables.items():
    save_delta_table(table_df, table_name)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 4. Silver Layer - cleansing, standardization and conformance
# -----------------------------------------------------------------------------

# 4.1 Independent cleaned Silver tables
silver_customer = (
    bronze_customer
    .select(
        F.col("cust_id").cast("long").alias("customer_id"),
        F.to_date(F.trim("created_at"), "M/d/yyyy").alias("created_date"),
        F.upper(F.trim("country")).alias("country"),
        F.initcap(F.trim("region")).alias("region"),
        F.initcap(F.trim("type")).alias("customer_type"),
        F.trim("zip").alias("zip_code"),
        F.when(F.lower(F.trim("active")).isin("y", "yes", "true", "1"), F.lit(True))
         .when(F.lower(F.trim("active")).isin("n", "no", "false", "0"), F.lit(False))
         .otherwise(F.lit(False)).alias("is_active"),
        F.col("ingest_ts"),
        F.col("source_file")
    )
)

silver_category = (
    bronze_category
    .select(
        F.col("cat_id").cast("int").alias("category_id"),
        F.initcap(F.trim("child")).alias("category_name"),
        F.initcap(F.trim("parent")).alias("parent_category"),
        F.col("ingest_ts"),
        F.col("source_file")
    )
    .withColumn(
        "category_group",
        F.when(F.lower(F.col("parent_category")) == "all", F.col("category_name"))
         .otherwise(F.col("parent_category"))
    )
)

silver_product = (
    bronze_product
    .select(
        F.col("prod_id").cast("int").alias("product_id"),
        F.col("cat_id").cast("int").alias("category_id"),
        F.initcap(F.trim("prod_name")).alias("product_name"),
        F.col("price").cast("decimal(18,4)").alias("unit_price_local"),
        normalize_currency(F.col("currency")).alias("currency"),
        F.col("ingest_ts"),
        F.col("source_file")
    )
)

silver_currency_rate_raw = (
    bronze_currency_rate
    .select(
        F.to_date(F.trim("date"), "yyyy-MM-dd").alias("rate_date"),
        normalize_currency(F.col("currency")).alias("currency"),
        F.col("conversion").cast("decimal(18,6)").alias("usd_conversion_rate"),
        F.col("ingest_ts"),
        F.col("source_file")
    )
)

# Duplicate date/currency rates exist in the source. Keep the highest conversion
# rate per date/currency as a deterministic rule for this assignment.
w_rate = Window.partitionBy("rate_date", "currency").orderBy(F.col("usd_conversion_rate").desc())
silver_currency_rate = (
    silver_currency_rate_raw
    .withColumn("rn", F.row_number().over(w_rate))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

silver_orders = (
    bronze_orders
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("cust_id").cast("long").alias("customer_id"),
        F.to_timestamp(F.trim("order_date"), "M/d/yyyy H:mm").alias("order_ts"),
        F.to_date(F.to_timestamp(F.trim("order_date"), "M/d/yyyy H:mm")).alias("order_date"),
        F.col("prod_id").cast("int").alias("product_id"),
        F.col("quantity").cast("decimal(18,4)").alias("quantity"),
        F.lower(F.trim("status")).alias("order_status"),
        F.col("ingest_ts"),
        F.col("source_file")
    )
)

# Save base Silver tables first. This materializes the independent cleaned tables
# before any conformed/dependent Silver transformations are built.
base_silver_tables = {
    "silver_customer": silver_customer,
    "silver_category": silver_category,
    "silver_product": silver_product,
    "silver_currency_rate": silver_currency_rate,
    "silver_orders": silver_orders,
}

for table_name, table_df in base_silver_tables.items():
    save_delta_table(table_df, table_name)

# Re-read base Silver tables from Unity Catalog. This removes long lazy lineage and
# prevents one dependent transformation failure from silently skipping later tables.
silver_customer = spark.table(f"{full_schema}.silver_customer")
silver_category = spark.table(f"{full_schema}.silver_category")
silver_product = spark.table(f"{full_schema}.silver_product")
silver_currency_rate = spark.table(f"{full_schema}.silver_currency_rate")
silver_orders = spark.table(f"{full_schema}.silver_orders")

# 4.2 Conformed Silver tables
silver_product_category = (
    silver_product.alias("p")
    .join(silver_category.alias("c"), "category_id", "left")
    .select(
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.category_id"),
        F.col("c.category_name").alias("category"),
        F.col("c.category_group"),
        F.col("p.unit_price_local"),
        F.col("p.currency"),
        F.col("p.ingest_ts"),
        F.col("p.source_file")
    )
)
save_delta_table(silver_product_category, "silver_product_category")

w_customer = Window.partitionBy("customer_id").orderBy(F.col("created_date").desc_nulls_last())
silver_customer_current = (
    silver_customer
    .withColumn("rn", F.row_number().over(w_customer))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
save_delta_table(silver_customer_current, "silver_customer_current")

# Re-read conformed tables before building enriched order lines.
silver_product_category = spark.table(f"{full_schema}.silver_product_category")
silver_customer_current = spark.table(f"{full_schema}.silver_customer_current")

silver_order_line_enriched = (
    silver_orders.alias("o")
    .join(silver_customer_current.alias("c"), "customer_id", "left")
    .join(silver_product_category.alias("p"), "product_id", "left")
    .join(
        silver_currency_rate.alias("r"),
        (F.col("o.order_date") == F.col("r.rate_date")) &
        (F.col("p.currency") == F.col("r.currency")),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.order_ts"),
        F.col("o.order_date"),
        F.col("o.product_id"),
        F.col("o.quantity"),
        F.col("o.order_status"),
        F.col("c.customer_id").alias("matched_customer_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.category_group"),
        F.col("p.unit_price_local"),
        F.col("p.currency"),
        F.col("r.usd_conversion_rate"),
        F.col("o.ingest_ts"),
        F.col("o.source_file")
    )
)
save_delta_table(silver_order_line_enriched, "silver_order_line_enriched")

# 4.3 Silver data quality tables
silver_order_line_enriched = spark.table(f"{full_schema}.silver_order_line_enriched")

revenue_statuses = ["paid", "shipped"]

silver_faulty_transactions = (
    silver_order_line_enriched
    .withColumn(
        "fault_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("order_id").isNull() | (F.length(F.trim("order_id")) == 0), F.lit("missing_order_id")),
            F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
            F.when(F.col("matched_customer_id").isNull() & F.col("customer_id").isNotNull(), F.lit("unknown_customer")),
            F.when(F.col("product_id").isNull(), F.lit("missing_product_id")),
            F.when(F.col("product_name").isNull() & F.col("product_id").isNotNull(), F.lit("unknown_product")),
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
            F.when(~F.col("order_status").isin(revenue_statuses), F.lit("non_revenue_status")),
            F.when(F.col("unit_price_local").isNull() | (F.col("unit_price_local") <= 0), F.lit("invalid_price")),
            F.when(F.col("currency").isNull(), F.lit("missing_currency")),
            F.when(F.col("usd_conversion_rate").isNull(), F.lit("missing_currency_rate"))
        )
    )
    .filter(F.length(F.col("fault_reason")) > 0)
    .select(
        "order_id", "customer_id", "order_ts", "order_date", "product_id",
        "quantity", "order_status", "fault_reason", "ingest_ts", "source_file"
    )
)
save_delta_table(silver_faulty_transactions, "silver_faulty_transactions")

duplicate_key = ["order_id", "customer_id", "order_ts", "product_id", "quantity", "order_status"]
silver_duplicate_order_lines = (
    silver_orders
    .groupBy(*duplicate_key)
    .agg(F.count("*").alias("duplicate_count"))
    .filter(F.col("duplicate_count") > 1)
)
save_delta_table(silver_duplicate_order_lines, "silver_duplicate_order_lines")

# Final Silver registry used by validation and downstream Gold logic.
silver_tables = {
    "silver_customer": spark.table(f"{full_schema}.silver_customer"),
    "silver_category": spark.table(f"{full_schema}.silver_category"),
    "silver_product": spark.table(f"{full_schema}.silver_product"),
    "silver_currency_rate": spark.table(f"{full_schema}.silver_currency_rate"),
    "silver_orders": spark.table(f"{full_schema}.silver_orders"),
    "silver_product_category": spark.table(f"{full_schema}.silver_product_category"),
    "silver_customer_current": spark.table(f"{full_schema}.silver_customer_current"),
    "silver_order_line_enriched": spark.table(f"{full_schema}.silver_order_line_enriched"),
    "silver_faulty_transactions": spark.table(f"{full_schema}.silver_faulty_transactions"),
    "silver_duplicate_order_lines": spark.table(f"{full_schema}.silver_duplicate_order_lines"),
}

print("\n===== Silver tables created =====")
spark.sql(f"SHOW TABLES IN {full_schema} LIKE 'silver_*'").show(50, truncate=False)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 5. Gold Layer - dimensional model for BI
# -----------------------------------------------------------------------------

# Customer dimension: one current row per customer.
dim_customer = (
    silver_customer_current
    .select(
        F.col("customer_id"),
        F.col("customer_id").alias("customer_key"),
        F.col("created_date"),
        F.col("country"),
        F.col("region"),
        F.col("customer_type"),
        F.col("zip_code"),
        F.col("is_active")
    )
)

# Product dimension: product master enriched with analytics category fields.
dim_product = (
    silver_product_category
    .select(
        "product_id",
        "product_name",
        "category_id",
        "category",
        "category_group",
        "unit_price_local",
        "currency"
    )
)

# Currency rate dimension: one rate per currency/date.
dim_currency_rate = (
    silver_currency_rate
    .select("rate_date", "currency", "usd_conversion_rate")
)

# Fact table: revenue-valid order lines only.
fact_order_line = (
    silver_order_line_enriched
    .filter(
        F.col("order_status").isin(revenue_statuses)
        & F.col("order_id").isNotNull()
        & F.col("matched_customer_id").isNotNull()
        & F.col("product_name").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("unit_price_local").isNotNull()
        & (F.col("unit_price_local") > 0)
        & F.col("usd_conversion_rate").isNotNull()
    )
    .withColumn(
        "order_line_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("order_id"),
                F.col("customer_id"),
                F.col("order_ts").cast("string"),
                F.col("product_id"),
                F.col("quantity").cast("string"),
                F.col("order_status")
            ),
            256
        )
    )
    .withColumn(
        "revenue_usd",
        (F.col("quantity") * F.col("unit_price_local") * F.col("usd_conversion_rate")).cast("decimal(18,4)")
    )
    .select(
        "order_line_id",
        "order_id",
        "customer_id",
        "product_id",
        "order_ts",
        "order_date",
        "quantity",
        "order_status",
        "unit_price_local",
        "currency",
        "usd_conversion_rate",
        "revenue_usd"
    )
)

# Date dimension driven from valid fact dates.
dim_date = (
    fact_order_line
    .select("order_date")
    .distinct()
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("day", F.dayofmonth("order_date"))
    .withColumn("week_of_year", F.weekofyear("order_date"))
    .select("date_key", "order_date", "year", "quarter", "month", "day", "week_of_year")
)

# Audit tables retained for BI/DQ reporting.
audit_duplicate_order_lines = silver_duplicate_order_lines
audit_faulty_transactions = silver_faulty_transactions

gold_tables = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_currency_rate": dim_currency_rate,
    "dim_date": dim_date,
    "fact_order_line": fact_order_line,
    "audit_duplicate_order_lines": audit_duplicate_order_lines,
    "audit_faulty_transactions": audit_faulty_transactions,
}

for table_name, table_df in gold_tables.items():
    save_delta_table(table_df, table_name)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 6. Analytics SQL - core BI metrics
# -----------------------------------------------------------------------------

analytics_sql = {
    "daily_active_users_by_region": f"""
        SELECT
            f.order_date,
            c.region,
            COUNT(DISTINCT f.customer_id) AS daily_active_users
        FROM {full_schema}.fact_order_line f
        INNER JOIN {full_schema}.dim_customer c
            ON f.customer_id = c.customer_id
        GROUP BY f.order_date, c.region
        ORDER BY f.order_date, c.region
    """,

    "sweet_revenue_conversion_orders_quantity": f"""
        SELECT
            p.category_group,
            ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
            COUNT(DISTINCT f.order_id) AS order_count,
            SUM(f.quantity) AS total_quantity,
            ROUND(
                COUNT(DISTINCT f.order_id) / NULLIF(COUNT(DISTINCT f.customer_id), 0),
                4
            ) AS orders_per_customer_conversion_proxy
        FROM {full_schema}.fact_order_line f
        INNER JOIN {full_schema}.dim_product p
            ON f.product_id = p.product_id
        WHERE LOWER(p.category_group) = 'sweet'
        GROUP BY p.category_group
    """,

    "top3_products_by_revenue_region": f"""
        WITH product_region AS (
            SELECT
                c.region,
                p.product_id,
                p.product_name,
                ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
                DENSE_RANK() OVER (
                    PARTITION BY c.region
                    ORDER BY SUM(f.revenue_usd) DESC
                ) AS revenue_rank
            FROM {full_schema}.fact_order_line f
            INNER JOIN {full_schema}.dim_customer c
                ON f.customer_id = c.customer_id
            INNER JOIN {full_schema}.dim_product p
                ON f.product_id = p.product_id
            GROUP BY c.region, p.product_id, p.product_name
        )
        SELECT
            region,
            revenue_rank,
            product_id,
            product_name,
            revenue_usd
        FROM product_region
        WHERE revenue_rank <= 3
        ORDER BY region, revenue_rank, product_name
    """,

    "customer_lifetime_value_proxy": f"""
        SELECT
            c.customer_id,
            c.region,
            c.customer_type,
            c.is_active,
            ROUND(SUM(f.revenue_usd), 2) AS lifetime_revenue_usd,
            COUNT(DISTINCT f.order_id) AS lifetime_order_count,
            SUM(f.quantity) AS lifetime_quantity
        FROM {full_schema}.fact_order_line f
        INNER JOIN {full_schema}.dim_customer c
            ON f.customer_id = c.customer_id
        GROUP BY c.customer_id, c.region, c.customer_type, c.is_active
        ORDER BY lifetime_revenue_usd DESC
    """,

    "duplicate_orders_faulty_transactions": f"""
        SELECT
            'duplicate_order_line' AS issue_type,
            order_id,
            customer_id,
            CAST(order_ts AS STRING) AS order_ts,
            product_id,
            CAST(quantity AS STRING) AS quantity,
            order_status,
            CONCAT('duplicate_count=', duplicate_count) AS issue_detail
        FROM {full_schema}.audit_duplicate_order_lines

        UNION ALL

        SELECT
            'faulty_transaction' AS issue_type,
            order_id,
            customer_id,
            CAST(order_ts AS STRING) AS order_ts,
            product_id,
            CAST(quantity AS STRING) AS quantity,
            order_status,
            fault_reason AS issue_detail
        FROM {full_schema}.audit_faulty_transactions
        ORDER BY issue_type, order_id, product_id
    """,
}

for metric_name, metric_query in analytics_sql.items():
    write_query_result_to_csv(metric_name, metric_query)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 7. Final validation summary
# -----------------------------------------------------------------------------

print("\n===== Tables created in Unity Catalog schema =====")
spark.sql(f"SHOW TABLES IN {full_schema}").show(200, truncate=False)

print("\n===== Row count summary =====")
row_count_queries = []
for table_name in list(bronze_tables.keys()) + list(silver_tables.keys()) + list(gold_tables.keys()):
    row_count_queries.append(f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count FROM {full_schema}.{table_name}")

spark.sql(" UNION ALL ".join(row_count_queries)).show(200, truncate=False)
