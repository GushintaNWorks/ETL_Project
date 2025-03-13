{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['event_id']
    )
}}

SELECT
    event_id,
    event_name,
    location,
    duration,
    fee,
    DATE(event_date) AS event_date, -- Konversi eksplisit ke DATE
    EXTRACT(YEAR FROM DATE(event_date)) AS event_year,
    created_at,
    TIMESTAMP_TRUNC(created_at, DAY) AS created_date
FROM {{ ref('prep_events') }}