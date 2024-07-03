{{ config(
    materialized='incremental',
    unique_key='id',
    alias='predf_schema'
) }}

SELECT
    id,
    JSONExtractString(message, 'field1') as name,
    JSONExtractString(message, 'field2') as gender,
    -- JSONExtractString(message, 'field3') as birthdate,
    CAST(JSONExtractString(message, 'field3') AS Date) as birthdate,
    CAST(JSONExtractString(message, 'field4') AS UInt32) as age,
    -- JSONExtractUInt(message, 'field4') as age,
    created_at
FROM {{ source('helloworld', 'json_messages') }}
WHERE JSONExtractString(message, 'key') = 'model_g'

{% if is_incremental() %}
AND created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
