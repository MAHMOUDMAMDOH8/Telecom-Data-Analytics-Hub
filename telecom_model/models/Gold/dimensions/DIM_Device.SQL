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
    '-1'          as tac,
    'unknown'     as brand,
    'Unknown'     as model,
    '-1'          as imei,
    null          as dbt_valid_from,
    null          as dbt_valid_to