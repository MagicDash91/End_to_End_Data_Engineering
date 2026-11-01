select
    i.order_item_id,
    i.order_id,
    o.customer_id,
    i.product_id,
    o.order_date,
    o.order_date::date                                  as order_day,
    o.order_status,
    o.order_status in ('paid', 'shipped', 'delivered')  as is_revenue,
    i.quantity,
    i.unit_price,
    i.gross_amount,
    i.discount_amount,
    i.net_amount
from {{ source('silver', 'order_items') }} as i
join {{ source('silver', 'orders') }} as o using (order_id)
