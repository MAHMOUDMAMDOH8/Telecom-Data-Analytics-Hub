{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='support_sk',
        indexes=[
            {'columns': ['support_sk'], 'unique': true},
            {'columns': ['date_sk']},
            {'columns': ['time_sk']},
            {'columns': ['user_sk']},
            {'columns': ['agent_sk']}
        ]
    )
}}
with Fact_support as (
    select
        support_sk,
        event_type,
        sid,
        -- Replace NULL user SKs with '-1'
        COALESCE(user_dim.user_sk, '-1') as user_sk,
        customer,
        -- Replace NULL agent SKs with '-1'
        COALESCE(agent_dim.agent_sk, '-1') as agent_sk,
        channel,
        reason,
        wait_time_seconds,
        resolution_time_seconds,
        satisfaction_score,
        first_call_resolution,
        escalated,
        call_back_requested,
        COALESCE(md5(TO_CHAR(event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) AS date_sk,
        COALESCE(EXTRACT(MINUTE FROM event_timestamp), 0) AS time_sk
    from {{ ref('Support') }} as s
    LEFT join {{ref('DIM_Date')}} as date_dim
        on COALESCE(md5(TO_CHAR(event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) = date_dim.date_sk
    LEFT join {{ref('DIM_Time')}} as time_dim
        on COALESCE(EXTRACT(MINUTE FROM event_timestamp), 0) = time_dim.time_sk
    LEFT join {{ref('DIM_Users')}} as user_dim
        on s.phone_number::text = user_dim.phone_number::text
    LEFT join {{ref('DIM_agent')}} as agent_dim
        on s.agent_id = agent_dim.agent_id
)
SELECT * from Fact_support