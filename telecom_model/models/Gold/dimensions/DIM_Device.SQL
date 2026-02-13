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
        case 
            when lower(trim(brand)) = 'apple' then 'Apple'
            when lower(trim(brand)) = 'samsung' then 'Samsung'
            when lower(trim(brand)) = 'huawei' then 'Huawei'
            when lower(trim(brand)) = 'xiaomi' then 'Xiaomi'
            when lower(trim(brand)) = 'oppo' then 'Oppo'
            when lower(trim(brand)) = 'vivo' then 'Vivo'
            when lower(trim(brand)) = 'oneplus' then 'OnePlus'
            when lower(trim(brand)) = 'motorola' then 'Motorola'
            when lower(trim(brand)) = 'nokia' then 'Nokia'
            when lower(trim(brand)) = 'sony' then 'Sony'
            when lower(trim(brand)) in ('unknown', '') or brand is null then 'unknown'
            else trim(brand)  
        end as brand,
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