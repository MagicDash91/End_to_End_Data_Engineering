-- fct_orders amounts must equal the sum of their lines in fct_order_items.
with lines as (
    select order_id, sum(net_amount) as net_amount, sum(quantity) as units
    from {{ ref('fct_order_items') }}
    group by order_id
)

select o.order_id, o.net_amount, l.net_amount as lines_net_amount, o.units, l.units as lines_units
from {{ ref('fct_orders') }} as o
left join lines as l using (order_id)
where o.net_amount <> coalesce(l.net_amount, 0)
   or o.units <> coalesce(l.units, 0)
