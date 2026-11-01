select
    product_id,
    sku,
    name                as product_name,
    category,
    unit_price          as current_unit_price,
    case
        when unit_price < 100000 then 'under 100k'
        when unit_price < 500000 then '100k-500k'
        when unit_price < 1000000 then '500k-1m'
        else '1m+'
    end                 as price_band,
    is_active,
    updated_at
from {{ source('silver', 'products') }}
