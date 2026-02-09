{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='Call_sk',
        indexes=[
            {"columns":['Call_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with Call_cte as (
    select 
        event_type,
        sid,
        from_json::jsonb ->> 'imei' as from_imei,
        substr(from_json::jsonb ->> 'imei',1,8) as from_tac,
        from_json::jsonb ->> 'phone_number' as from_phone_number,
        from_json::jsonb ->> 'cell_site' as from_cell_site,
        to_json::jsonb ->> 'imei' as to_imei,
        substr(to_json::jsonb ->> 'imei',1,8) as to_tac,
        to_json::jsonb ->> 'phone_number' as to_phone_number,
        to_json::jsonb ->> 'cell_site' as to_cell_site,
        status,
        call_type,
        behavior_profile,
        seasonal_multiplier,
        billing_info::jsonb ->> 'amount'  as amount,
        billing_info::jsonb ->> 'currency' as currency,
        qos_metrics::jsonb ->> 'codec' as codec,
        qos_metrics::jsonb ->> 'jitter_ms' as jitter_ms,
        qos_metrics::jsonb ->> 'mos_score' as mos_score,
        qos_metrics::jsonb ->> 'packet_loss_percent' as packet_loss_percent,
        event_timestamp :: timestamp  as event_timestamp
    from {{source('telecom_row','call_events_raw')}} as c
    where 
        event_timestamp is not null 
    and
        from_json::jsonb ->> 'phone_number' is not null
    and
        to_json::jsonb ->> 'phone_number' is not null
),
Call_with_sk as (
    select
        md5(event_timestamp || from_phone_number || to_phone_number) as Call_sk,
        *
    from Call_cte
    where amount :: float > 0
)

{% if is_incremental() %}
select * from Call_with_sk
where Call_sk not in (select distinct Call_sk from {{ this }})
{% else %}
select * from Call_with_sk
{% endif %}
