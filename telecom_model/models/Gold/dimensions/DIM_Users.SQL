{{
    config(
        materialized='incremental',
        unique_key='user_sk'
    )
}}
with user_selected_columns as (
    select
        user_sk,
        phone_number,
        city ,
        case 
            when lower(trim(status)) = 'active' then 'Active'
            when lower(trim(status)) = 'inactive' then 'inactive'
            when lower(trim(status)) = 'suspended' then 'suspended'
            when lower(trim(status)) = 'terminated' then 'terminated'
            when lower(trim(status)) in ('unknown', '') or status is null then 'unknown'
            else trim(status) 
        end as status,
        customer_type,
        gender,
        age_group,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('Users') }}
)
select *
from user_selected_columns
-- ✅ Unknown / Default User for handling missing or unmatched user references in fact tables
union all
select
    '-1'      as user_sk,
    '-1'          as phone_number,
    'Unknown'     as city,
    'unknown'     as status,
    'unknown'     as customer_type,
    'unknown'     as gender,
    'unknown'     as age_group,
    null          as dbt_valid_from,
    null          as dbt_valid_to   