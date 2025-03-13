{{
    config(
        materialized='table',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['attendee_id', 'event_id']
    )
}}

SELECT
    r.registration_id,
    r.attendee_id,
    r.event_id,
    r.registration_date,
    r.payment_status,
    r.created_at,
    e.fee as event_fee,
    e.event_date,
    TIMESTAMP_TRUNC(r.created_at, DAY) as created_date
FROM {{ ref('prep_registrations') }} r
LEFT JOIN {{ ref('prep_events') }} e ON r.event_id = e.event_id