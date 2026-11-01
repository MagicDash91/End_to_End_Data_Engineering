{{ config(severity = 'warn') }}
-- Settled payments should cover the order's net amount. A mismatch usually means one of the order's
-- lines was quarantined in silver (zero quantity, orphan, ...), so this warns instead of failing.
select order_id, order_status, net_amount, paid_amount
from {{ ref('fct_orders') }}
where order_status in ('paid', 'shipped', 'delivered')
  and abs(paid_amount - net_amount) > 1
