SELECT
    registration_id,
    attendee_id,
    event_id,
    registration_date,
    payment_status,
    created_at
FROM {{ source('raw', 'registrations') }}