-- One row per order. Revenue is recognised for paid, shipped and delivered orders; pending,
-- cancelled and returned orders keep their amounts but have is_revenue = false.
with items as (
    select
        order_id,
        count(*)               as item_count,
        sum(quantity)          as units,
        sum(gross_amount)      as gross_amount,
        sum(discount_amount)   as discount_amount,
        sum(net_amount)        as net_amount
    from {{ ref('fct_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(amount) filter (where status = 'settled')                   as paid_amount,
        (array_agg(payment_method order by updated_at desc))[1]         as payment_method,
        bool_or(status = 'refunded')                                     as is_refunded
    from {{ source('silver', 'payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_date::date                                      as order_day,
    o.order_status,
    o.order_status in ('paid', 'shipped', 'delivered')      as is_revenue,
    o.shipping_city,
    coalesce(i.item_count, 0)                               as item_count,
    coalesce(i.units, 0)                                    as units,
    coalesce(i.gross_amount, 0)::numeric(16, 2)             as gross_amount,
    coalesce(i.discount_amount, 0)::numeric(16, 2)          as discount_amount,
    coalesce(i.net_amount, 0)::numeric(16, 2)               as net_amount,
    coalesce(p.paid_amount, 0)::numeric(16, 2)              as paid_amount,
    p.payment_method,
    coalesce(p.is_refunded, false)                          as is_refunded,
    o.updated_at
from {{ source('silver', 'orders') }} as o
left join items as i using (order_id)
left join payments as p using (order_id)
