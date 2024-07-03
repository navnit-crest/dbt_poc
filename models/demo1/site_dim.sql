{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    JSONExtractString(message, 'site_id') as id,
    JSONExtractString(message, 'site') as site,
    JSONExtractString(message, 'site_name') as name,
    CAST(JSONExtractString(message, 'site_class') AS String) as site_class,
    message_event_time as updated_at,
    now() as created_at
FROM {{ source('helloworld', 'json_messages_raw') }}
WHERE message_type = 'ADT'

{% if is_incremental() %}
AND message_event_time > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
