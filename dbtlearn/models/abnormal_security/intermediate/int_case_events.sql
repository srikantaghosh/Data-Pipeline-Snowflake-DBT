{{
  config(
    materialized = 'incremental',
    unique_key = 'case_id',
    incremental_strategy = 'merge'
  )
}}

SELECT 
    ce.customer_id,
    ce.user_id,
    ce.case_id,
    ce.triggering_event_id,
    cce.reported_case_id,
    cce.report_timestamp,
    cce.reported_event_id,
    cce.reported_event_timestamp
FROM {{ ref('stg_case_events') }} ce
LEFT JOIN {{ ref('stg_customer_case_events') }} cce
ON ce.case_id = cce.reported_case_id

-- Incremental logic: only process rows with new or updated timestamps
{% if is_incremental() %}
WHERE ce.triggering_event_id IS NOT NULL
AND ce.reported_event_timestamp > (
    SELECT MAX(reported_event_timestamp) 
    FROM {{ this }}
)
{% endif %}
