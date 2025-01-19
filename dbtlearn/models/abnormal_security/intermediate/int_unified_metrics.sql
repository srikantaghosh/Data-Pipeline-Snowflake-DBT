{{
  config(
    materialized = 'table'
    )
}}

SELECT 
    c.customer_id,
    c.customer_name,
    c.cnt_users,
    c.remediation_action,
    ue.user_id AS user_event_user_id,
    ue.event_id AS user_event_id,
    ue.event_type AS user_event_type,
    ue.event_timestamp AS user_event_timestamp,
    de.detector_id,
    de.event_id AS detector_event_id,
    de.event_type AS detector_event_type,
    de.confidence_level AS detector_confidence_level,
    de.event_timestamp AS detector_event_timestamp,
    ic.case_id AS case_event_id,
    ic.triggering_event_id,
    ic.reported_case_id,
    ic.report_timestamp,
    ic.reported_event_id,
    ic.reported_event_timestamp

FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_user_events') }} ue ON c.customer_id = ue.customer_id
LEFT JOIN {{ ref('intermediate_detector_events') }} de ON ue.event_id = de.event_id
LEFT JOIN {{ ref('intermediate_cases') }} ic ON de.event_id = ic.triggering_event_id