{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='SMS_sk',
        indexes=[
            {"columns":['SMS_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with SMS_cte as (
    select 
        event_type,
        sid,
        "from"::jsonb ->> 'imei' as from_imei,
        substr("from"::jsonb ->> 'imei',1,8) as from_tac,
        "from"::jsonb ->> 'phone_number' as from_phone_number,
        "from"::jsonb ->> 'cell_site' as from_cell_site,
        "to"::jsonb ->> 'imei' as to_imei,
        substr("to"::jsonb ->> 'imei',1,8) as to_tac,
        "to"::jsonb ->> 'phone_number' as to_phone_number,
        "to"::jsonb ->> 'cell_site' as to_cell_site,
        status,
        body,
        registration_date,
        seasonal_multiplier,
        billing_info::jsonb ->> 'amount'  as amount,
        billing_info::jsonb ->> 'currency' as currency,
        event_timestamp :: timestamp
    from {{source('telecom_row','sms_events_raw')}} as s
    where 
        event_timestamp is not null 
    and
        "from"::jsonb ->> 'phone_number' is not null
    and
        "to"::jsonb ->> 'phone_number' is not null
),
SMS_with_sk as (
    select
        md5(event_timestamp || from_phone_number || to_phone_number) as SMS_sk,
        *
    from SMS_cte
    where amount :: float > 0
)

{% if is_incremental() %}
select * from SMS_with_sk
where SMS_sk not in (select distinct SMS_sk from {{ this }})
{% else %}
select * from SMS_with_sk
{% endif %}