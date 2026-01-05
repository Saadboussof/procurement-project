-- Aggregated Orders: Total demand per product across all stores
SELECT 
    product_sku, 
    SUM(quantity) as total_ordered
FROM hive.procurement.orders
GROUP BY product_sku;
