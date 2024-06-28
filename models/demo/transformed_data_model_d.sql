-- models/transformed_data_model_a.sql

{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    unique_key='id',
) }}

SELECT
    id,  
    JSONExtractString(message, 'field1') as field1,
    JSONExtractString(message, 'field2') as field2,
    JSONExtractInt(message, 'field3') as field3,
    now() as created_at
FROM {{ source('helloworld', 'json_messages') }}
WHERE JSONExtractString(message, 'key') = 'model_d'

{% if is_incremental() %}
-- AND id > (SELECT max(id) FROM {{ this }})
AND created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
