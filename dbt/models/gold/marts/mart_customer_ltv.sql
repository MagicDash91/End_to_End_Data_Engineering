with revenue_orders as (
    select customer_id, order_date, net_amount
    from {{ ref('fct_orders') }}
    where is_revenue
),

per_customer as (
    select
        customer_id,
        count(*)                as orders,
        sum(net_amount)         as lifetime_value,
        min(order_date)         as first_order_at,
        max(order_date)         as last_order_at
    from revenue_orders
    group by customer_id
)

select
    c.customer_id,
    c.full_name,
    c.city,
    c.signup_cohort,
    coalesce(p.orders, 0)                                       as orders,
    coalesce(p.lifetime_value, 0)::numeric(16, 2)               as lifetime_value,
    case when p.orders > 0 then round(p.lifetime_value / p.orders, 2) end as avg_order_value,
    p.first_order_at,
    p.last_order_at,
    (current_date - p.last_order_at::date)                      as days_since_last_order,
    case
        when p.orders is null then 'prospect'
        when current_date - p.last_order_at::date <= 30 and p.orders >= 3 then 'champion'
        when current_date - p.last_order_at::date <= 60 then 'active'
        when current_date - p.last_order_at::date <= 120 then 'at_risk'
        else 'churned'
    end                                                         as segment
from {{ ref('dim_customers') }} as c
left join per_customer as p using (customer_id)
