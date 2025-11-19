INSERT INTO fact_orders (
                transaction_id,
                item_id,
                item_quantity,
                item_total_price,
                payment_type_id,
                location_type_id,
                transaction_date
                )
SELECT 
    CASE 
        WHEN t.transaction_id IN (SELECT transaction_id FROM fact_orders)
        THEN gen_random_uuid()::text
        ELSE t.transaction_id
    END AS transaction_id,
    t.item_id,
    t.item_quantity,
    t.item_total_price,
    t.payment_type_id,
    t.location_type_id,
    t.transaction_date
FROM fact_orders_temp t;