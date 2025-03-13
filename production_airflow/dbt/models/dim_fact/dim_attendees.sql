{{
    config(
        materialized='table',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['attendee_id']
    )
}}

SELECT
    attendee_id,
    attendee_name,
    email,
    phone,
    date_of_birth,
    address,
    created_at,
    TIMESTAMP_TRUNC(created_at, DAY) as created_date
FROM {{ ref('prep_attendees') }}