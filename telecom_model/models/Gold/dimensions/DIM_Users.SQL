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
        status,
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
    Null          as phone_number,
    'Unknown'     as city,
    'unknown'     as status,
    'unknown'     as customer_type,
    'unknown'     as gender,
    'unknown'     as age_group,
    null          as dbt_valid_from,
    null          as dbt_valid_to   