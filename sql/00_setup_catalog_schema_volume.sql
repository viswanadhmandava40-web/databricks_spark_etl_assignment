-- Run once if catalog/schema/volume do not already exist.
-- Some personal workspaces may restrict CREATE CATALOG/CREATE VOLUME.

CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.bi_assignment;

-- Create the assignment volume if you have Unity Catalog volume permissions.
CREATE VOLUME IF NOT EXISTS workspace.bi_assignment.etl_assignment;

-- Expected raw file location after upload:
-- /Volumes/workspace/bi_assignment/etl_assignment/data/raw/customer.csv
-- /Volumes/workspace/bi_assignment/etl_assignment/data/raw/orders.csv
-- /Volumes/workspace/bi_assignment/etl_assignment/data/raw/product.csv
-- /Volumes/workspace/bi_assignment/etl_assignment/data/raw/prod_cat_tree.csv
-- /Volumes/workspace/bi_assignment/etl_assignment/data/raw/currency_conversion.json
