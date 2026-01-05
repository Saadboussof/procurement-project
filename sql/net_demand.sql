-- Net Demand Calculation with MOQ and Case Size Rounding
-- Formula: net_demand = max(0, aggregated_orders + safety_stock - (available_stock - reserved_stock))
-- Then: round up to nearest pack_size and ensure >= min_order_quantity

WITH aggregated_orders AS (
    SELECT 
        product_sku, 
        SUM(quantity) as total_ordered
    FROM hive.procurement.orders
    GROUP BY product_sku
),
stock_status AS (
    SELECT 
        product_sku, 
        CAST(available_quantity AS INTEGER) as available_quantity, 
        CAST(reserved_quantity AS INTEGER) as reserved_quantity, 
        CAST(safety_stock AS INTEGER) as safety_stock
    FROM hive.procurement.inventory
),
raw_demand AS (
    SELECT
        p.sku as product_sku,
        p.name as product_name,
        p.supplier_id,
        p.min_order_quantity,
        p.pack_size,
        s.name as supplier_name,
        s.lead_time_days,
        COALESCE(ao.total_ordered, 0) as total_ordered,
        COALESCE(ss.available_quantity, 0) as available_quantity,
        COALESCE(ss.reserved_quantity, 0) as reserved_quantity,
        COALESCE(ss.safety_stock, 0) as safety_stock,
        -- Step 1: Calculate raw net demand
        GREATEST(0, COALESCE(ao.total_ordered, 0) + COALESCE(ss.safety_stock, 0) - (COALESCE(ss.available_quantity, 0) - COALESCE(ss.reserved_quantity, 0))) as raw_net_demand
    FROM postgres.public.products p
    LEFT JOIN aggregated_orders ao ON p.sku = ao.product_sku
    LEFT JOIN stock_status ss ON p.sku = ss.product_sku
    LEFT JOIN postgres.public.suppliers s ON p.supplier_id = s.supplier_id
),
final_demand AS (
    SELECT
        product_sku,
        product_name,
        supplier_id,
        supplier_name,
        lead_time_days,
        total_ordered,
        available_quantity,
        reserved_quantity,
        safety_stock,
        raw_net_demand,
        min_order_quantity,
        pack_size,
        -- Step 2: Round up to nearest pack_size (case size)
        CASE 
            WHEN raw_net_demand = 0 THEN 0
            ELSE CEILING(CAST(raw_net_demand AS DOUBLE) / pack_size) * pack_size
        END as rounded_demand,
        -- Step 3: Apply MOQ constraint
        GREATEST(
            CASE 
                WHEN raw_net_demand = 0 THEN 0
                ELSE CEILING(CAST(raw_net_demand AS DOUBLE) / pack_size) * pack_size
            END,
            CASE WHEN raw_net_demand > 0 THEN min_order_quantity ELSE 0 END
        ) as final_order_quantity
    FROM raw_demand
)
SELECT 
    supplier_id,
    supplier_name,
    product_sku,
    product_name,
    lead_time_days,
    raw_net_demand,
    pack_size,
    min_order_quantity,
    final_order_quantity as order_quantity
FROM final_demand
WHERE final_order_quantity > 0;
