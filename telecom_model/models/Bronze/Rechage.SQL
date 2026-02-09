{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='Recharge_sk',
        indexes=[
            {"columns":['Recharge_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with recharge_cte as (
    select 
        event_type,
        sid,
        "recharge_amount",
        balance_before,
        balance_after,
        payment_method,
        phone_number as recharge_phone_number_from,
        customer as recharge_phone_number_to,
        status,
        billing_info::jsonb ->> 'amount'  as amount,
        billing_info::jsonb ->> 'currency' as currency,
        seasonal_multiplier,
        event_timestamp :: timestamp
    from {{source('telecom_row','recharge_events_raw')}} as p
    where 
        event_timestamp is not null 
    and
        phone_number is not null
    and
        customer is not null

),
recharge_with_sk as (
    select
        md5(event_timestamp || recharge_phone_number_from || recharge_phone_number_to || amount || currency ) as Recharge_sk,
        *
    from recharge_cte
    where amount :: float > 0
)

{% if is_incremental() %}
select * from recharge_with_sk
where Recharge_sk not in (select distinct Recharge_sk from {{ this }})
{% else %}
select * from recharge_with_sk
{% endif %}