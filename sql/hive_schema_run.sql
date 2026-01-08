CREATE SCHEMA IF NOT EXISTS hive.procurement;

DROP TABLE IF EXISTS hive.procurement.orders;
CREATE TABLE hive.procurement.orders (
    transaction_id VARCHAR,
    timestamp VARCHAR,
    store_id VARCHAR,
    product_sku VARCHAR,
    quantity INT
)
WITH (
    format = 'JSON',
    external_location = 'hdfs://namenode:9000/raw/orders/2026-01-08'
);

DROP TABLE IF EXISTS hive.procurement.inventory;
CREATE TABLE hive.procurement.inventory (
    warehouse_id VARCHAR,
    product_sku VARCHAR,
    available_quantity VARCHAR,
    reserved_quantity VARCHAR,
    safety_stock VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 'hdfs://namenode:9000/raw/stock/2026-01-08'
);
