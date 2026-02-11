{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='call_sk',
        indexes=[
            {'columns': ['call_sk'], 'unique': true},
            {'columns': ['date_sk']},
            {'columns': ['time_sk']},
            {'columns': ['from_user_sk']},
            {'columns': ['to_user_sk']},
            {'columns': ['from_cell_site_sk']},
            {'columns': ['to_cell_site_sk']}
        ]

    )
}}
with Fact_call as (

    select
        c.call_sk,
        c.event_type,
        c.sid,

        -- Replace NULL device SKs with '-1'
        COALESCE(from_device.device_sk, '-1') as from_device_sk,
        COALESCE(to_device.device_sk, '-1')   as to_device_sk,

        c.from_imei,
        c.to_imei,

        -- Replace NULL user SKs with '-1'
        COALESCE(from_user.user_sk, '-1') as from_user_sk,
        COALESCE(to_user.user_sk, '-1')   as to_user_sk,

        -- Replace NULL cell site SKs with '-1'
        COALESCE(from_cell.cell_site_sk, '-1') as from_cell_site_sk,
        COALESCE(to_cell.cell_site_sk, '-1')   as to_cell_site_sk,

        c.status,
        c.call_type,
        c.behavior_profile,
        c.seasonal_multiplier,
        c.amount,
        c.currency,
        c.codec,
        c.jitter_ms,
        c.mos_score,
        c.packet_loss_percent,

        -- Already handling NULL dates
        COALESCE(md5(TO_CHAR(c.event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) AS date_sk,
        COALESCE(EXTRACT(MINUTE FROM c.event_timestamp), 0) AS time_sk

    from {{ ref('Calls') }} c

    LEFT join {{ ref('DIM_cell_site')}} as from_cell
        on c.from_cell_site = from_cell.cell_id

    LEFT join {{ ref('DIM_cell_site')}} as to_cell
        on c.to_cell_site = to_cell.cell_id

    LEFT join {{ ref('DIM_Users')}} as from_user
        on c.from_phone_number = from_user.phone_number

    LEFT join {{ ref('DIM_Users')}} as to_user
        on c.to_phone_number = to_user.phone_number

    LEFT join {{ ref('DIM_Device')}} as from_device
        on c.from_tac = from_device.tac

    LEFT join {{ ref('DIM_Device')}} as to_device
        on c.to_tac = to_device.tac

    LEFT join {{ ref('DIM_Date')}} as date_dim
        on COALESCE(md5(TO_CHAR(c.event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) = date_dim.date_sk

    LEFT join {{ ref('DIM_Time')}} as time_dim
        on COALESCE(EXTRACT(MINUTE FROM c.event_timestamp), 0) = time_dim.time_sk
)

SELECT * FROM Fact_call
