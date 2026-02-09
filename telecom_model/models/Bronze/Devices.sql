{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='device_sk',
        indexes=[
            {"columns":['device_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with Device_cte as (
    select
        distinct tac,
        brand,
        model,
        imei,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ref('CDC_Devices')}}
    where tac is not null
),
Device_with_sk as (
    select
        md5(tac || brand || model ) as device_sk,
        *
    from Device_cte
)

{% if is_incremental() %}
select * from Device_with_sk
where device_sk not in (select distinct device_sk from {{ this }})
{% else %}
select * from Device_with_sk
{% endif %}