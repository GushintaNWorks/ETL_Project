SELECT
    event_id,
    event_name,
    location,
    duration,
    fee,
    event_date,
    created_at
FROM {{ source('raw', 'events') }}