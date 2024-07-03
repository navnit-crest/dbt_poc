-- models/raw_json_messages.sql
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    JSONExtractString(message, 'key') as model_key,
    JSONExtractString(message, 'field1') as field1,
    JSONExtractString(message, 'field2') as field2,
    JSONExtractInt(message, 'field3') as field3,
    created_at
FROM {{ source('helloworld', 'json_messages') }}
WHERE JSONExtractString(message, 'key') = 'model_e'

{% if is_incremental() %}
AND created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
