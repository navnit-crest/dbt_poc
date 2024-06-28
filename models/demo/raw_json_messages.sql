-- models/raw_json_messages.sql
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='append'
) }}

SELECT
    id,
    JSONExtractString(message, 'key') as model_key,
    JSONExtractString(message, 'field1') as field1,
    JSONExtractString(message, 'field2') as field2,
    JSONExtractInt(message, 'field3') as field3,
    created_at
FROM {{ source('helloworld', 'json_messages') }}

{% if is_incremental() %}
WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
