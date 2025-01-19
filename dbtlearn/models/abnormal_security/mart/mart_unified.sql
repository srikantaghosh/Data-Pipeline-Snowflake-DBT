{{
  config(
    materialized = 'table'
    )
}}

SELECT 
    customer_id,
    customer_name,
    cnt_users,
    remediation_action,
    user_event_user_id,
    user_event_id,
    user_event_type,
    user_event_timestamp,
    detector_id,
    detector_event_id,
    detector_event_type,
    detector_confidence_level,
    detector_event_timestamp,
    case_event_id,
    triggering_event_id,
    reported_case_id,
    report_timestamp,
    reported_event_id,
    reported_event_timestamp

FROM {{ ref('int_unified_metrics') }} 
