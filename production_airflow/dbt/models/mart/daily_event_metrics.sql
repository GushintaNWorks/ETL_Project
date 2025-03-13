{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['event_id']
    )
}}

WITH partitioned_data AS (
    SELECT 
        TIMESTAMP_TRUNC(SAFE_CAST(e.event_date AS TIMESTAMP), DAY) AS event_date,
        e.event_id,
        e.event_name,
        COUNT(DISTINCT r.registration_id) AS total_registrations,
        COUNT(DISTINCT CASE 
            WHEN r.payment_status = 'Paid' THEN r.registration_id 
        END) AS paid_registrations,
        COALESCE(SUM(CASE 
            WHEN r.payment_status = 'Paid' THEN e.fee 
            ELSE 0 
        END), 0) AS total_revenue,
        COALESCE(AVG(CASE 
            WHEN r.payment_status = 'Paid' THEN e.fee 
            ELSE NULL 
        END), 0) AS avg_ticket_price,
        
        CASE 
            WHEN COUNT(DISTINCT r.registration_id) = 0 THEN 0
            ELSE COUNT(DISTINCT CASE 
                WHEN r.payment_status = 'Paid' THEN r.registration_id 
            END) * 100.0 / COUNT(DISTINCT r.registration_id)
        END AS conversion_rate

    FROM `lateral-yew-452203-p8.de_project_dim_fact.dim_events` e 
    LEFT JOIN `lateral-yew-452203-p8.de_project_dim_fact.fact_registrations` r
        ON e.event_id = r.event_id

    GROUP BY 
        event_date, 
        e.event_id, 
        e.event_name
)

SELECT *
FROM partitioned_data