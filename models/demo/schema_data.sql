{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail'
) }}

SELECT
    id,
    JSONExtractString(message, 'field1') as name,
    JSONExtractString(message, 'field2') as gender,
    CAST(JSONExtractString(message, 'field3') AS Date) as birthdate,
    JSONExtractUInt(message, 'field4') as age,
    created_at
FROM {{ source('helloworld', 'json_messages') }}
WHERE JSONExtractString(message, 'key') = 'model_f'

{% if is_incremental() %}
AND created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
