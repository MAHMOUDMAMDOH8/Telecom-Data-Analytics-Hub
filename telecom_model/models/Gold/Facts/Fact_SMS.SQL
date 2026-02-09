{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='sms_sk',
        indexes=[
            {'columns': ['sms_sk'], 'unique': true},
            {'columns': ['date_sk']},
            {'columns': ['time_sk']},
            {'columns': ['from_user_sk']},
            {'columns': ['to_user_sk']},
            {'columns': ['from_cell_site_sk']},
            {'columns': ['to_cell_site_sk']}
        ]
    )
}}
with Fact_sms as (
    select
        sms_sk,
        event_type,
        sid,
        -- Keep '-1' for missing relationships (use COALESCE, not NULLIF)
        COALESCE(from_device.device_sk, '-1') as from_device_sk,
        COALESCE(to_device.device_sk, '-1') as to_device_sk,
        from_imei,
        to_imei,
        COALESCE(from_user.user_sk, '-1') as from_user_sk,
        COALESCE(to_user.user_sk, '-1') as to_user_sk,
        COALESCE(from_cell.cell_site_sk, '-1') as from_cell_site_sk,
        COALESCE(to_cell.cell_site_sk, '-1') as to_cell_site_sk,
        s.status,
        body,
        registration_date,
        seasonal_multiplier,
        amount,
        currency,
        COALESCE(md5(TO_CHAR(event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) AS date_sk,
        COALESCE(EXTRACT(MINUTE FROM event_timestamp), 0) AS time_sk
    from {{ ref('SMS') }} s
    LEFT join {{ ref('DIM_cell_site')}} as from_cell
        on from_cell_site = from_cell.cell_id
    LEFT join {{ ref('DIM_cell_site')}} as to_cell
        on to_cell_site = to_cell.cell_id
    LEFT join {{ref('DIM_Device')}} as from_device
        on from_tac = from_device.tac
    LEFT join {{ref('DIM_Device')}} as to_device
        on to_tac = to_device.tac
    LEFT join {{ref('DIM_Date')}} as date_dim
        on COALESCE(md5(TO_CHAR(event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) = date_dim.date_sk
    LEFT join {{ref('DIM_Time')}} as time_dim
        on COALESCE(EXTRACT(MINUTE FROM event_timestamp), 0) = time_dim.time_sk
    LEFT join {{ref('DIM_Users')}} as from_user
        on from_phone_number::text = from_user.phone_number::text
    LEFT join {{ref('DIM_Users')}} as to_user
        on to_phone_number::text = to_user.phone_number::text
)
SELECT * from Fact_sms