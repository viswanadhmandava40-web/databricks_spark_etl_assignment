# Databricks notebook source
"""
Databricks / PySpark ETL pipeline for BI assignment.

Run options:
1. Import this file as a Databricks notebook and run all cells.
2. Run as a Databricks job with parameters:
   raw_base_path=/Volumes/workspace/bi_assignment/etl_assignment/data/raw
   target_catalog=workspace
   target_schema=bi_assignment
   target_base_path=/Volumes/workspace/bi_assignment/etl_assignment/tmp

This implementation avoids pandas and uses Spark DataFrames + SQL only.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Databricks widgets / local fallbacks
try:
    dbutils.widgets.text("raw_base_path", "/Volumes/workspace/bi_assignment/etl_assignment/data/raw")
    dbutils.widgets.text("target_catalog", "workspace")
    dbutils.widgets.text("target_schema", "bi_assignment")
    dbutils.widgets.text("target_base_path", "/Volumes/workspace/bi_assignment/etl_assignment/tmp")
    raw_base_path = dbutils.widgets.get("raw_base_path")
    target_catalog = dbutils.widgets.get("target_catalog")
    target_schema = dbutils.widgets.get("target_schema")
    target_base_path = dbutils.widgets.get("target_base_path")
except Exception:
    raw_base_path = "/Volumes/workspace/bi_assignment/etl_assignment/data/raw"
    target_catalog = "workspace"
    target_schema = "bi_assignment"
    target_base_path = "/Volumes/workspace/bi_assignment/etl_assignment/tmp"

full_schema = f"{target_catalog}.{target_schema}" if target_catalog else target_schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")

# COMMAND ----------
# Schemas
customer_schema = T.StructType([
    T.StructField("cust_id", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("zip", T.StringType()),
    T.StructField("active", T.StringType()),
])

orders_schema = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("cust_id", T.StringType()),
    T.StructField("order date", T.StringType()),
    T.StructField("prod_id", T.StringType()),
    T.StructField("quantity", T.StringType()),
    T.StructField("status", T.StringType()),
])

product_schema = T.StructType([
    T.StructField("prod_id", T.StringType()),
    T.StructField("cat_id", T.StringType()),
    T.StructField("prod_name", T.StringType()),
    T.StructField("price", T.StringType()),
    T.StructField("currency", T.StringType()),
])

cat_schema = T.StructType([
    T.StructField("cat_id", T.StringType()),
    T.StructField("child", T.StringType()),
    T.StructField("parent", T.StringType()),
])

currency_schema = T.StructType([
    T.StructField("date", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("conversion", T.DoubleType()),
])

# COMMAND ----------
# Helpers

def normalize_currency(col):
    return (
        F.when(F.upper(F.trim(col)).isin("YEN", "JPY"), F.lit("YEN"))
         .when(F.upper(F.trim(col)).isin("POUND", "GBP"), F.lit("POUND"))
         .when(F.upper(F.trim(col)) == "USD", F.lit("USD"))
         .otherwise(F.upper(F.trim(col)))
    )


def save_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    path = f"{target_base_path}/{table_name}"
    (df.write
       .format("delta")
       .mode(mode)
       .option("overwriteSchema", "true")
       .save(path))
    spark.sql(f"DROP TABLE IF EXISTS {full_schema}.{table_name}")
    spark.sql(f"CREATE TABLE {full_schema}.{table_name} USING DELTA LOCATION '{path}'")

# COMMAND ----------
# 1. Ingest Bronze
bronze_customer = (spark.read.option("header", True).schema(customer_schema)
                   .csv(f"{raw_base_path}/customer.csv")
                   .withColumn("ingest_ts", F.current_timestamp())
                   .withColumn("source_file", F.input_file_name()))
bronze_orders = (spark.read.option("header", True).schema(orders_schema)
                 .csv(f"{raw_base_path}/orders.csv")
                 .withColumn("ingest_ts", F.current_timestamp())
                 .withColumn("source_file", F.input_file_name()))
bronze_product = (spark.read.option("header", True).schema(product_schema)
                  .csv(f"{raw_base_path}/product.csv")
                  .withColumn("ingest_ts", F.current_timestamp())
                  .withColumn("source_file", F.input_file_name()))
bronze_category = (spark.read.option("header", True).schema(cat_schema)
                   .csv(f"{raw_base_path}/prod_cat_tree.csv")
                   .withColumn("ingest_ts", F.current_timestamp())
                   .withColumn("source_file", F.input_file_name()))
bronze_currency = (spark.read.schema(currency_schema)
                   .json(f"{raw_base_path}/currency_conversion.json")
                   .withColumn("ingest_ts", F.current_timestamp())
                   .withColumn("source_file", F.input_file_name()))

for name, df in {
    "bronze_customer": bronze_customer,
    "bronze_orders": bronze_orders,
    "bronze_product": bronze_product,
    "bronze_category": bronze_category,
    "bronze_currency_rate": bronze_currency,
}.items():
    save_delta_table(df, name)

# COMMAND ----------
# 2. Clean / Silver
customer_clean = (
    bronze_customer
    .select(
        F.col("cust_id").cast("long").alias("customer_id"),
        F.to_date("created_at", "M/d/yyyy").alias("created_date"),
        F.upper(F.trim("country")).alias("country"),
        F.initcap(F.trim("region")).alias("region"),
        F.initcap(F.trim("type")).alias("customer_type"),
        F.trim("zip").alias("zip_code"),
        F.when(F.lower(F.trim("active")).isin("y", "yes", "true", "1"), F.lit(True)).otherwise(F.lit(False)).alias("is_active")
    )
)

# Multiple rows per customer exist. Keep latest created_at as current customer dimension.
w_customer = Window.partitionBy("customer_id").orderBy(F.col("created_date").desc())
dim_customer = (
    customer_clean
    .withColumn("rn", F.row_number().over(w_customer))
    .filter("rn = 1")
    .drop("rn")
    .withColumn("customer_key", F.col("customer_id"))
)

category_clean = (
    bronze_category
    .select(
        F.col("cat_id").cast("int").alias("category_id"),
        F.initcap(F.trim("child")).alias("category_name"),
        F.initcap(F.trim("parent")).alias("parent_category")
    )
)

# Map category tree to analytics level. If parent=All then child is top category; otherwise parent is top category.
category_hierarchy = (
    category_clean
    .withColumn("category", F.col("category_name"))
    .withColumn("category_group", F.when(F.lower("parent_category") == "all", F.col("category_name")).otherwise(F.col("parent_category")))
)

product_clean = (
    bronze_product
    .select(
        F.col("prod_id").cast("int").alias("product_id"),
        F.col("cat_id").cast("int").alias("category_id"),
        F.initcap(F.trim("prod_name")).alias("product_name"),
        F.col("price").cast("decimal(18,4)").alias("unit_price_local"),
        normalize_currency(F.col("currency")).alias("currency")
    )
)

dim_product = (
    product_clean.alias("p")
    .join(category_hierarchy.alias("c"), "category_id", "left")
    .select(
        "p.product_id", "p.product_name", "p.category_id", "c.category", "c.category_group",
        "p.unit_price_local", "p.currency"
    )
)

currency_clean = (
    bronze_currency
    .select(
        F.to_date("date", "yyyy-MM-dd").alias("rate_date"),
        normalize_currency(F.col("currency")).alias("currency"),
        F.col("conversion").cast("decimal(18,6)").alias("usd_conversion_rate")
    )
)
# Duplicate date/currency rates exist; keep latest loaded/highest conversion deterministically.
w_rate = Window.partitionBy("rate_date", "currency").orderBy(F.col("usd_conversion_rate").desc())
dim_currency_rate = currency_clean.withColumn("rn", F.row_number().over(w_rate)).filter("rn = 1").drop("rn")

orders_clean = (
    bronze_orders
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("cust_id").cast("long").alias("customer_id"),
        F.to_timestamp(F.col("order date"), "M/d/yyyy H:mm").alias("order_ts"),
        F.to_date(F.to_timestamp(F.col("order date"), "M/d/yyyy H:mm")).alias("order_date"),
        F.col("prod_id").cast("int").alias("product_id"),
        F.col("quantity").cast("decimal(18,4)").alias("quantity"),
        F.lower(F.trim("status")).alias("order_status")
    )
)

valid_status = ["paid", "shipped"]
order_enriched = (
    orders_clean.alias("o")
    .join(dim_customer.alias("c"), "customer_id", "left")
    .join(dim_product.alias("p"), "product_id", "left")
    .join(dim_currency_rate.alias("r"), (F.col("o.order_date") == F.col("r.rate_date")) & (F.col("p.currency") == F.col("r.currency")), "left")
)

faulty_transactions = (
    order_enriched
    .withColumn(
        "fault_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("order_id").isNull() | (F.length("order_id") == 0), F.lit("missing_order_id")),
            F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
            F.when(F.col("customer_key").isNull() & F.col("customer_id").isNotNull(), F.lit("unknown_customer")),
            F.when(F.col("product_id").isNull(), F.lit("missing_product_id")),
            F.when(F.col("product_name").isNull() & F.col("product_id").isNotNull(), F.lit("unknown_product")),
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
            F.when(~F.col("order_status").isin(valid_status), F.lit("non_revenue_status")),
            F.when(F.col("usd_conversion_rate").isNull(), F.lit("missing_currency_rate"))
        )
    )
    .filter(F.length("fault_reason") > 0)
    .select("order_id", "customer_id", "order_ts", "product_id", "quantity", "order_status", "fault_reason")
)

# Duplicate order lines = same order/customer/timestamp/product/quantity/status appears more than once.
dup_key = ["order_id", "customer_id", "order_ts", "product_id", "quantity", "order_status"]
duplicate_order_lines = (
    orders_clean
    .groupBy(*dup_key)
    .agg(F.count("*").alias("duplicate_count"))
    .filter("duplicate_count > 1")
)

fact_order_line = (
    order_enriched
    .filter(
        F.col("order_status").isin(valid_status)
        & F.col("order_id").isNotNull()
        & F.col("customer_key").isNotNull()
        & F.col("product_name").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("usd_conversion_rate").isNotNull()
    )
    .withColumn("order_line_id", F.sha2(F.concat_ws("|", "order_id", "customer_id", "order_ts", "product_id", "quantity", "order_status"), 256))
    .withColumn("revenue_usd", (F.col("quantity") * F.col("unit_price_local") * F.col("usd_conversion_rate")).cast("decimal(18,4)"))
    .select(
        "order_line_id", "order_id", "customer_id", F.col("product_id"), "order_ts", "order_date", "quantity", "order_status",
        "unit_price_local", F.col("currency"), "usd_conversion_rate", "revenue_usd"
    )
)

# Date dimension driven from facts.
dim_date = (
    fact_order_line.select("order_date").distinct()
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("day", F.dayofmonth("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
)

for name, df in {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_currency_rate": dim_currency_rate,
    "dim_date": dim_date,
    "fact_order_line": fact_order_line,
    "audit_duplicate_order_lines": duplicate_order_lines,
    "audit_faulty_transactions": faulty_transactions,
}.items():
    save_delta_table(df, name)

# COMMAND ----------
# 3. Analytics views / outputs
analytics_sql = {
    "daily_active_users_by_region": f"""
        SELECT f.order_date, c.region, COUNT(DISTINCT f.customer_id) AS daily_active_users
        FROM {full_schema}.fact_order_line f
        JOIN {full_schema}.dim_customer c ON f.customer_id = c.customer_id
        GROUP BY f.order_date, c.region
        ORDER BY f.order_date, c.region
    """,
    "sweet_revenue_conversion_orders_quantity": f"""
        SELECT
            p.category_group,
            ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
            COUNT(DISTINCT f.order_id) AS order_count,
            SUM(f.quantity) AS total_quantity,
            ROUND(COUNT(DISTINCT f.order_id) / COUNT(DISTINCT f.customer_id), 4) AS orders_per_customer_conversion_proxy
        FROM {full_schema}.fact_order_line f
        JOIN {full_schema}.dim_product p ON f.product_id = p.product_id
        WHERE LOWER(p.category_group) = 'sweet'
        GROUP BY p.category_group
    """,
    "top3_products_by_revenue_region": f"""
        WITH product_region AS (
            SELECT c.region, p.product_id, p.product_name, ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
                   DENSE_RANK() OVER (PARTITION BY c.region ORDER BY SUM(f.revenue_usd) DESC) AS revenue_rank
            FROM {full_schema}.fact_order_line f
            JOIN {full_schema}.dim_customer c ON f.customer_id = c.customer_id
            JOIN {full_schema}.dim_product p ON f.product_id = p.product_id
            GROUP BY c.region, p.product_id, p.product_name
        )
        SELECT region, revenue_rank, product_id, product_name, revenue_usd
        FROM product_region
        WHERE revenue_rank <= 3
        ORDER BY region, revenue_rank, product_name
    """,
    "customer_lifetime_value_proxy": f"""
        SELECT c.customer_id, c.region, c.customer_type, c.is_active,
               ROUND(SUM(f.revenue_usd), 2) AS lifetime_revenue_usd,
               COUNT(DISTINCT f.order_id) AS lifetime_order_count,
               SUM(f.quantity) AS lifetime_quantity
        FROM {full_schema}.fact_order_line f
        JOIN {full_schema}.dim_customer c ON f.customer_id = c.customer_id
        GROUP BY c.customer_id, c.region, c.customer_type, c.is_active
        ORDER BY lifetime_revenue_usd DESC
    """,
    "duplicate_orders_faulty_transactions": f"""
        SELECT 'duplicate_order_line' AS issue_type, order_id, customer_id, CAST(order_ts AS STRING) AS order_ts,
               product_id, CAST(quantity AS STRING) AS quantity, order_status,
               CONCAT('duplicate_count=', duplicate_count) AS issue_detail
        FROM {full_schema}.audit_duplicate_order_lines
        UNION ALL
        SELECT 'faulty_transaction' AS issue_type, order_id, customer_id, CAST(order_ts AS STRING) AS order_ts,
               product_id, CAST(quantity AS STRING) AS quantity, order_status, fault_reason AS issue_detail
        FROM {full_schema}.audit_faulty_transactions
        ORDER BY issue_type, order_id, product_id
    """,
}

for view_name, query in analytics_sql.items():
    df = spark.sql(query)
    df.createOrReplaceTempView(view_name)
    print(f"\n===== {view_name} =====")
    df.show(50, truncate=False)
