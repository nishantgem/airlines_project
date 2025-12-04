{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('airlines', 'airlines_r_raw') }}

