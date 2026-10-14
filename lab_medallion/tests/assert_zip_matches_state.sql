{{ config(severity='warn') }}

-- A ZIP code must fall in the range of the user's state: the test fails if this query returns rows
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
join {{ ref('states') }} s on u.state = s.code
where cast(u.zip_code as integer) not between s.zip_min and s.zip_max
