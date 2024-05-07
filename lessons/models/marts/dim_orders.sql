WITH

--Aggregate measures
order_item_measures AS (
    SELECT
    order_id,
    sum(item_sale_price) as total_sale_price,
    sum(product_cost) as total_product_cost,
    sum(item_profit) as total_profit,
    sum(item_discount) as total_discount
    FROM {{ref('int_ecommerce_order_items_products')}}
    GROUP BY 1
)

SELECT
    -- Dimension data from our staging orders table
    od.order_id,
    od.created_at as order_created_at,
    od.shipped_at AS order_shipped_at,
	od.delivered_at AS order_delivered_at,
	od.returned_at AS order_returned_at,
	od.status AS order_status,
	od.num_items_ordered,

    -- Metrics on an order level
	om.total_sale_price,
	om.total_product_cost,
	om.total_profit,
	om.total_discount
FROM {{ref('stg_ecommerce__orders')}} as od
LEFT JOIN order_item_measures om
    ON od.order_id = om.order_id