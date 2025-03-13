{{
    config(
        materialized='table',
        partition_by={
            "field": "first_registration_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['attendee_id']
    )
}}

WITH registration_data AS (
    SELECT
        r.attendee_id,
        e.event_name,
        e.fee,
        ROW_NUMBER() OVER (PARTITION BY r.attendee_id ORDER BY e.fee DESC) AS rn_high,
        ROW_NUMBER() OVER (PARTITION BY r.attendee_id ORDER BY e.fee ASC) AS rn_low
    FROM `lateral-yew-452203-p8.de_project_dim_fact.fact_registrations` r
    JOIN `lateral-yew-452203-p8.de_project_dim_fact.dim_events` e 
        ON r.event_id = e.event_id
    WHERE r.payment_status = 'Paid'
),

aggregated_data AS (
    SELECT 
        a.attendee_id,
        a.attendee_name,
        COUNT(DISTINCT r.event_id) AS total_events_registered,
        COUNT(DISTINCT CASE 
            WHEN r.payment_status = 'Paid' THEN r.event_id 
        END) AS total_events_paid,
        
        COALESCE(SUM(CASE 
            WHEN r.payment_status = 'Paid' THEN e.fee 
            ELSE 0 
        END), 0) AS total_spent,
        
        COALESCE(AVG(CASE 
            WHEN r.payment_status = 'Paid' THEN e.fee 
            ELSE NULL 
        END), 0) AS avg_spent_per_event,

        MIN(CASE 
            WHEN r.payment_status = 'Paid' THEN DATE(r.registration_date) 
        END) AS first_registration_date,
        
        MAX(CASE 
            WHEN r.payment_status = 'Paid' THEN DATE(r.registration_date) 
        END) AS last_registration_date,

        MAX(CASE WHEN rd.rn_high = 1 THEN rd.event_name END) AS most_expensive_event,
        MAX(CASE WHEN rd.rn_high = 1 THEN rd.fee END) AS most_expensive_event_fee,

        MAX(CASE WHEN rd.rn_low = 1 THEN rd.event_name END) AS least_expensive_event,
        MAX(CASE WHEN rd.rn_low = 1 THEN rd.fee END) AS least_expensive_event_fee,

        CASE 
            WHEN COUNT(DISTINCT CASE 
                WHEN r.payment_status = 'Paid' THEN r.event_id 
            END) > 2 THEN 'Loyal'
            
            WHEN COUNT(DISTINCT CASE 
                WHEN r.payment_status = 'Paid' THEN r.event_id 
            END) BETWEEN 1 AND 2 THEN 'Regular'
            
            WHEN COUNT(DISTINCT r.event_id) > 0 
                AND COUNT(DISTINCT CASE 
                    WHEN r.payment_status != 'Paid' THEN r.event_id 
                END) = COUNT(DISTINCT r.event_id) THEN 'Interested'
            
            ELSE 'Casual'
        END AS loyalty_status

    FROM `lateral-yew-452203-p8.de_project_dim_fact.dim_attendees` a
    LEFT JOIN `lateral-yew-452203-p8.de_project_dim_fact.fact_registrations` r 
        ON a.attendee_id = r.attendee_id
    LEFT JOIN `lateral-yew-452203-p8.de_project_dim_fact.dim_events` e 
        ON r.event_id = e.event_id
    LEFT JOIN registration_data rd 
        ON a.attendee_id = rd.attendee_id

    WHERE e.event_year IS NOT NULL 

    GROUP BY a.attendee_id, a.attendee_name
)

SELECT * 
FROM aggregated_data