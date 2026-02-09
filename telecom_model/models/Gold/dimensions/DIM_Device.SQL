{{
    config(
        materialized='incremental',
        unique_key='device_sk'
    )
}}
with device_selected_columns as (
    select
        device_sk,
        tac,
        brand,
        model,
        imei,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('Devices') }}
)
select *
from device_selected_columns
-- ✅ Unknown / Default Device for handling missing or unmatched device references in fact tables
union all
select
    '-1'      as device_sk,
    Null          as tac,
    'Unknown'     as brand,
    'Unknown'     as model,
    Null          as imei,
    null          as dbt_valid_from,
    null          as dbt_valid_to