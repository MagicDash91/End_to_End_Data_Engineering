with orders as (
    select * from {{ ref('fct_orders') }}
),

first_orders as (
    select customer_id, min(order_day) as first_order_day
    from orders
    where is_revenue
    group by customer_id
),

daily as (
    select
        order_day,
        count(*)                                                as orders_placed,
        count(*) filter (where is_revenue)                      as revenue_orders,
        count(*) filter (where order_status = 'cancelled')      as cancelled_orders,
        count(*) filter (where order_status = 'returned')       as returned_orders,
        coalesce(sum(net_amount) filter (where is_revenue), 0)  as revenue,
        coalesce(sum(units) filter (where is_revenue), 0)       as units_sold
    from orders
    group by order_day
),

new_customers as (
    select first_order_day as order_day, count(*) as new_customers
    from first_orders
    group by first_order_day
)

select
    d.date_day,
    d.year_month,
    d.is_weekend,
    coalesce(daily.orders_placed, 0)                    as orders_placed,
    coalesce(daily.revenue_orders, 0)                   as revenue_orders,
    coalesce(daily.cancelled_orders, 0)                 as cancelled_orders,
    coalesce(daily.returned_orders, 0)                  as returned_orders,
    coalesce(daily.revenue, 0)::numeric(16, 2)          as revenue,
    coalesce(daily.units_sold, 0)                       as units_sold,
    case when daily.revenue_orders > 0
         then round(daily.revenue / daily.revenue_orders, 2) end                     as avg_order_value,
    case when daily.orders_placed > 0
         then round(daily.cancelled_orders::numeric / daily.orders_placed, 4) end    as cancellation_rate,
    coalesce(nc.new_customers, 0)                       as new_customers
from {{ ref('dim_date') }} as d
left join daily on daily.order_day = d.date_day
left join new_customers as nc on nc.order_day = d.date_day
where d.date_day <= current_date
