{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='Support_sk',
        indexes=[
            {"columns":['Support_sk'],'unique':true}
        ],
        target_schema='bronze'
    )
}}
with Support_cte as (
    select 
        event_type,
        sid,
        phone_number,
        customer,
        channel,
        reason,
        wait_time_seconds,
        resolution_time_seconds,
        agent_id,
        satisfaction_score,
        first_call_resolution,
        event_timestamp :: timestamp,
        escalated,
        call_back_requested
    from {{source('telecom_row','support_events_raw')}} as s
    where 
        event_timestamp is not null 
    and
        phone_number is not null
    and
        escalated is not NULL
    and 
        call_back_requested is not NULL

),
Support_with_sk as (
    select
        DISTINCT md5(event_timestamp || phone_number || channel || escalated || call_back_requested || agent_id) as Support_sk,
        *
    from Support_cte
)

{% if is_incremental() %}
select * from Support_with_sk
where Support_sk not in (select distinct Support_sk from {{ this }})
{% else %}
select * from Support_with_sk
{% endif %}

