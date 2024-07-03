{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    JSONExtractString(message, 'patient_mrn') as id,
    JSONExtractString(message, 'patient_name') as name,
    JSONExtractString(message, 'patient_gender') as gender,
    CAST(JSONExtractString(message, 'patient_dob') AS Date) as birthdate,
    message_event_time as updated_at,
    now() as created_at
FROM {{ source('helloworld', 'json_messages_raw') }}
WHERE message_type = 'ADT'

{% if is_incremental() %}
AND message_event_time > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
