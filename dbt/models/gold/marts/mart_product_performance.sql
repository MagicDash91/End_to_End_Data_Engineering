with lines as (
    select * from {{ ref('fct_order_items') }}
),

per_product as (
    select
        product_id,
        count(distinct order_id) filter (where is_revenue)              as orders,
        coalesce(sum(quantity) filter (where is_revenue), 0)            as units_sold,
        coalesce(sum(net_amount) filter (where is_revenue), 0)          as revenue,
        coalesce(sum(quantity) filter (where order_status = 'returned'), 0) as units_returned
    from lines
    group by product_id
)

select
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.price_band,
    coalesce(pp.orders, 0)                          as orders,
    coalesce(pp.units_sold, 0)                      as units_sold,
    coalesce(pp.revenue, 0)::numeric(16, 2)         as revenue,
    coalesce(pp.units_returned, 0)                  as units_returned,
    case when coalesce(pp.units_sold, 0) + coalesce(pp.units_returned, 0) > 0
         then round(pp.units_returned::numeric / (pp.units_sold + pp.units_returned), 4)
    end                                             as return_rate,
    rank() over (partition by p.category order by coalesce(pp.revenue, 0) desc) as revenue_rank_in_category
from {{ ref('dim_products') }} as p
left join per_product as pp using (product_id)
