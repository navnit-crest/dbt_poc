-- models/transformed_data_model_a.sql
{{ config(
    materialized='incremental',
    unique_key='id',
    alias='transformed_data'
) }}

WITH raw AS (
    SELECT * FROM {{ ref('raw_json_messages') }}
    WHERE model_key = 'model_c'
)

SELECT
    id,
    field1,
    field2,
    field3,
    now() as created_at
FROM raw

{% if is_incremental() %}
-- WHERE id > (SELECT max(id) FROM {{ this }})
WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
