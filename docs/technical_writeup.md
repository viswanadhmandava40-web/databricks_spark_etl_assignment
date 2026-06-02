# Technical Write-up: Databricks Spark ETL Pipeline for BI

## 1. Overall design

The solution follows a medallion-style Databricks architecture:

| Layer | Purpose | Tables |
|---|---|---|
| Bronze | Raw ingestion with minimal changes and lineage columns | `bronze_customer`, `bronze_orders`, `bronze_product`, `bronze_category`, `bronze_currency_rate` |
| Silver | Standardized, typed, deduplicated, validated data | intermediate Spark DataFrames in the notebook |
| Gold | Dimensional model and BI-ready facts/audit tables | `dim_customer`, `dim_product`, `dim_currency_rate`, `dim_date`, `fact_order_line`, `audit_duplicate_order_lines`, `audit_faulty_transactions` |

All target tables are written as Delta tables and registered in `workspace.bi_assignment` by default. The implementation uses Spark DataFrames and Spark SQL only.

## 2. Dimensional model

### `dim_customer`

| Column | Description |
|---|---|
| `customer_key` | Surrogate-style key, currently same as `customer_id` for this dataset |
| `customer_id` | Business customer id |
| `created_date` | Customer record date parsed from source |
| `country` | Uppercase country code |
| `region` | Standardized region name |
| `customer_type` | Standardized customer segment/type |
| `zip_code` | Customer postal code |
| `is_active` | Boolean active flag derived from `y/n` |

The customer source contains multiple rows per customer. For this assignment, the latest `created_date` record is selected as the current customer dimension row. At larger scale, this can be extended into SCD Type 2.

### `dim_product`

| Column | Description |
|---|---|
| `product_id` | Product business key |
| `product_name` | Standardized product name |
| `category_id` | Category id from product source |
| `category` | Child category, for example Chocolate, Candy, Crisp, Chip |
| `category_group` | Parent analytics category, for example Sweet or Salt |
| `unit_price_local` | Product price in source currency |
| `currency` | Normalized currency code: USD, YEN, POUND |

### `dim_currency_rate`

| Column | Description |
|---|---|
| `rate_date` | Currency conversion date |
| `currency` | Normalized currency code |
| `usd_conversion_rate` | Conversion rate used to convert local price into USD-equivalent revenue |

The conversion file contains duplicate date/currency rows. The ETL keeps one deterministic record per date/currency using a window function.

### `fact_order_line`

| Column | Description |
|---|---|
| `order_line_id` | SHA-256 hash generated from order line natural keys |
| `order_id` | Order id |
| `customer_id` | Customer id |
| `product_id` | Product id |
| `order_ts` | Parsed order timestamp |
| `order_date` | Parsed order date |
| `quantity` | Decimal quantity |
| `order_status` | Standardized order status |
| `unit_price_local` | Local product price |
| `currency` | Product currency |
| `usd_conversion_rate` | Daily conversion rate |
| `revenue_usd` | `quantity * unit_price_local * usd_conversion_rate` |

Only `paid` and `shipped` orders are included as revenue-valid facts. Other statuses remain available in the audit table.

## 3. Mapping and transformation summary

| Source | Target | Transformation |
|---|---|---|
| `customer.csv.cust_id` | `dim_customer.customer_id` | Cast to long |
| `customer.csv.created_at` | `dim_customer.created_date` | Parse `M/d/yyyy` |
| `customer.csv.region` | `dim_customer.region` | Trim + initcap |
| `customer.csv.active` | `dim_customer.is_active` | `y/yes/true/1` to true, else false |
| `orders.csv.order date` | `fact_order_line.order_ts/order_date` | Parse `M/d/yyyy H:mm` |
| `orders.csv.quantity` | `fact_order_line.quantity` | Cast to decimal |
| `orders.csv.status` | `fact_order_line.order_status` | Lowercase + trim; filter revenue-valid statuses |
| `product.csv.currency` | `dim_product.currency` | Normalize `Yen` to `YEN`, `USD` to `USD` |
| `prod_cat_tree.csv` | `dim_product.category/category_group` | Join category id; parent category becomes reporting group |
| `currency_conversion.json` | `dim_currency_rate` | Parse date, normalize currency, deduplicate date/currency |

## 4. Data quality issues discovered and handling

| Issue | Example / impact | Handling |
|---|---|---|
| Duplicate customer rows | Same customer appears multiple times with changing active flag | Latest customer row retained in `dim_customer` |
| Duplicate order lines | `A-005` has repeated identical product 2 order lines | Captured in `audit_duplicate_order_lines`; fact preserves revenue-valid lines because duplicates may represent either source errors or repeated line items, depending business confirmation |
| Missing customer id | Orders `A-21`, `A-22` | Captured in `audit_faulty_transactions`; excluded from revenue fact |
| Non-revenue status | `created`, `cancelled` | Captured in audit; excluded from revenue fact |
| Fractional quantity | `A-013` has quantity `0.1` | Kept because it is positive and may be valid for weighted/variable products; could be restricted if business requires integer quantities |
| Duplicate currency rates | Duplicate `2020-01-28` rates | Deduplicated by date/currency window rule |
| Currency naming mismatch | Product has `Yen`; conversion has `YEN` | Standardized to uppercase canonical currency |

## 5. Core metrics exposed

The SQL file provides:

1. Daily active users by region.
2. Revenue, order count, conversion proxy, and quantity for Sweet category.
3. Top 3 products by revenue by region.
4. Customer lifetime value proxy with active/inactive flag.
5. Duplicate orders and faulty transactions.

## 6. Trade-offs and 100x scale improvements

### Simplified for assignment

- Customer dimension is current-state only, not SCD Type 2.
- Revenue-valid status is assumed as `paid` and `shipped`.
- Conversion is based on product currency and order date.
- The product/category hierarchy is shallow and modeled directly in `dim_product`.
- Duplicate order lines are audited but not automatically removed from the fact table.

### Improvements for 100x scale

- Use Auto Loader with cloudFiles for incremental ingestion from S3/ADLS.
- Use Delta Live Tables or Databricks Workflows with expectations for quality rules.
- Partition large fact tables by `order_date` and apply Z-ORDER on `customer_id`, `product_id`, and `order_id`.
- Implement SCD Type 2 for customers and products using Delta `MERGE INTO`.
- Add a quarantine layer for invalid records with error codes and replay support.
- Use Unity Catalog with managed tables, lineage, access controls, and table ownership.
- Add automated data quality metrics to a monitoring table and alert through Databricks jobs/Slack/ServiceNow.
- Use incremental processing with watermarks/change tracking instead of full overwrite.
- Add business-approved deduplication rules for orders and order lines.
