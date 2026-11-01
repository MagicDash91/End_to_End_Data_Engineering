select
    customer_id,
    first_name,
    last_name,
    trim(concat_ws(' ', first_name, last_name))     as full_name,
    email,
    email_is_valid,
    phone,
    city,
    province,
    created_at                                      as customer_since,
    date_trunc('month', created_at)::date           as signup_cohort,
    updated_at
from {{ source('silver', 'customers') }}
