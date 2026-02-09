{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='Payment_sk',
        indexes=[
            {"columns":['Payment_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with Payment_cte as (
    select 
        event_type,
        sid,
        transaction_id,
        invoice_number,
        payment_type,
        payment_amount,
        phone_number as from_phone_number,
        customer as to_phone_number,
        status,
        billing_info::jsonb ->> 'amount'  as amount,
        billing_info::jsonb ->> 'currency' as currency,
        seasonal_multiplier,
        event_timestamp :: timestamp
    from {{source('telecom_row','payment_events_raw')}} as p
    where 
        event_timestamp is not null 
    and
        phone_number is not null
    and
        transaction_id is not null
),
Payment_with_sk as (
    select
        md5(event_timestamp || from_phone_number || transaction_id) as Payment_sk,
        *
    from Payment_cte
    where amount :: float > 0
)

{% if is_incremental() %}
select * from Payment_with_sk
where Payment_sk not in (select distinct Payment_sk from {{ this }})
{% else %}
select * from Payment_with_sk
{% endif %}