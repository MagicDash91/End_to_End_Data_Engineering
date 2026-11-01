-- Total revenue in the daily mart must equal revenue in fct_orders (no days lost or double counted).
with mart as (select coalesce(sum(revenue), 0) as revenue from {{ ref('mart_daily_revenue') }}),
     fact as (
         select coalesce(sum(net_amount), 0) as revenue
         from {{ ref('fct_orders') }}
         where is_revenue and order_day <= current_date
     )

select mart.revenue as mart_revenue, fact.revenue as fact_revenue
from mart, fact
where mart.revenue <> fact.revenue
