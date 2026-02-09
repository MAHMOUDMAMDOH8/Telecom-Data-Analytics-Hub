{% snapshot CDC_Devices %}
{{
    config(
        unique_key='device_sk',
        strategy='check',
        check_cols=['imei', 'brand', 'model', 'tac'],
        target_schema='snapshots'
    )
}}

select 
    device_sk :: VARCHAR(20) as device_sk,
    imei :: VARCHAR(20) as imei,
    brand :: VARCHAR(50) as brand,
    model :: VARCHAR(100) as model,
    tac :: VARCHAR(20) as tac
from {{ source('telecom_row', 'devices') }}

{% endsnapshot %}