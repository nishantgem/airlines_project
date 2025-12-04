{{ config(materialized='table') }}

WITH daily_weather AS (
    SELECT
        airport_code,
        date,
        AVG(avg_wind_speed_kmh) AS avg_wind_speed_kmh,
        SUM(precipitation_mm)   AS total_precipitation_mm
    FROM {{ ref('prep_weather_daily') }}
    WHERE date BETWEEN '2024-05-01' AND '2024-05-31'
    GROUP BY airport_code, date
),

flights_may AS (
    SELECT
        origin AS airport_code,
        flight_date,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)  AS diverted_flights
    FROM {{ ref('prep_flights') }}
    WHERE flight_date BETWEEN '2024-05-01' AND '2024-05-31'
    GROUP BY origin, flight_date
),

airport_summary AS (
    SELECT
        f.airport_code,
        DATE_TRUNC('month', f.flight_date) AS month,
        SUM(f.cancelled_flights) AS total_cancelled,
        SUM(f.diverted_flights)  AS total_diverted,
        AVG(dw.avg_wind_speed_kmh) AS avg_wind_speed_kmh,
        SUM(dw.total_precipitation_mm) AS total_precipitation_mm
    FROM flights_may f
    LEFT JOIN daily_weather dw
      ON f.airport_code = dw.airport_code
     AND f.flight_date = dw.date
    GROUP BY f.airport_code, DATE_TRUNC('month', f.flight_date)
)

SELECT *
FROM airport_summary
ORDER BY airport_code
