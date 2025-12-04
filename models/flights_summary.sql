{{ config(materialized='view') }}

SELECT
    carrier,
    COUNT(*) AS total_flights,
    AVG(arr_delay) AS avg_arrival_delay
FROM {{ source('airlines', 'flights') }}
GROUP BY carrier
