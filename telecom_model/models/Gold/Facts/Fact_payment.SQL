{{
    config(
        materialized='incremental',
        strategy='merge',
        unique_key='payment_sk',
        indexes=[
            {'columns': ['payment_sk'], 'unique': true},
            {'columns': ['date_sk']},
            {'columns': ['time_sk']},
            {'columns': ['from_user_sk']},
            {'columns': ['to_user_sk']},
        ]

    )
}}
with Fact_payment as (
    select
        payment_sk,
        event_type,
        sid,
        transaction_id,
        invoice_number,
        payment_type,
        payment_amount,
        -- Replace NULL user SKs with '-1'
        COALESCE(from_user.user_sk, '-1') as from_user_sk,
        COALESCE(to_user.user_sk, '-1') as to_user_sk,
        p.status,
        amount,
        currency,
        seasonal_multiplier,
        COALESCE(md5(TO_CHAR(event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) AS date_sk,
        COALESCE(EXTRACT(MINUTE FROM event_timestamp), 0) AS time_sk
    from {{ ref('Payment') }} p
    LEFT join {{ref('DIM_Date')}} as date_dim
        on COALESCE(md5(TO_CHAR(event_timestamp, 'YYYY-MM-DD')), md5('1970-01-01')) = date_dim.date_sk
    LEFT join {{ref('DIM_Time')}} as time_dim
        on COALESCE(EXTRACT(MINUTE FROM event_timestamp), 0) = time_dim.time_sk
    LEFT join {{ref('DIM_Users')}} as from_user
        on from_phone_number = from_user.phone_number
    LEFT join {{ref('DIM_Users')}} as to_user
        on to_phone_number = to_user.phone_number
)
SELECT * from Fact_payment