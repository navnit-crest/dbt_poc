-- models/final_transformed_data.sql
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT * FROM {{ ref('transformed_data_model_a') }} a
LEFT JOIN {{ ref('transformed_data_model_b') }} b
ON a.id = b.id

{% if is_incremental() %}
WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
