-- How much each silver rule rejected in the latest run, next to the entity's accepted row count.
with rejected as (
    select entity, reason, count(*) as rejected_rows, max(quarantined_at) as quarantined_at
    from {{ source('silver', 'quarantine') }}
    group by entity, reason
),

accepted as (
    select 'customers' as entity, count(*) as accepted_rows from {{ source('silver', 'customers') }}
    union all select 'products', count(*) from {{ source('silver', 'products') }}
    union all select 'orders', count(*) from {{ source('silver', 'orders') }}
    union all select 'order_items', count(*) from {{ source('silver', 'order_items') }}
    union all select 'payments', count(*) from {{ source('silver', 'payments') }}
)

select
    r.entity,
    r.reason,
    r.rejected_rows,
    a.accepted_rows,
    round(r.rejected_rows::numeric / nullif(r.rejected_rows + a.accepted_rows, 0), 4) as rejected_share,
    r.quarantined_at
from rejected as r
left join accepted as a using (entity)
