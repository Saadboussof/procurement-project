SELECT 
    'Unknown Product' as issue_type,
    o.product_sku,
    o.transaction_id
FROM hive.procurement.orders o
LEFT JOIN postgres.public.products p ON o.product_sku = p.sku
WHERE p.sku IS NULL

UNION ALL

SELECT 
    'Missing Supplier' as issue_type,
    p.sku as product_sku,
    NULL as transaction_id
FROM postgres.public.products p
WHERE p.supplier_id IS NULL;
