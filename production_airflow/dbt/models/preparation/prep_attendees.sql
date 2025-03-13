SELECT
    attendee_id,
    attendee_name,
    email,
    phone,
    date_of_birth,
    address,
    created_at
FROM {{ source('raw', 'attendees') }}