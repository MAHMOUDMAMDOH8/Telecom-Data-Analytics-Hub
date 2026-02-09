{% snapshot CDC_Users %}
{{
    config(
        unique_key='phone_number',
        strategy='check',
        check_cols=['city', 'status'],
        target_schema='snapshots'
    )
}}

select 
    concat('0', cast(msisdn as text)) :: VARCHAR(11) as phone_number ,
    city :: varchar(50) as city,
    status :: varchar(20) as status,
    customer_type :: varchar(20) as customer_type,
    gender :: varchar(10) as gender,
    age_group :: varchar(20) as age_group,
    activation_date ::timestamp as activation_date
from {{ source('telecom_row', 'users') }}

{% endsnapshot %}