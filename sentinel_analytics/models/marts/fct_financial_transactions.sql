{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        raw_json:transaction_id::string as transaction_id,
        raw_json:user_id::int as user_id,
        raw_json:amount::decimal(10,2) as amount_gbp,
        raw_json:merchant::string as merchant_name,
        raw_json:category::string as category,
        raw_json:timestamp::timestamp as transaction_timestamp,
        ingested_at as processed_at
    FROM {{ source('raw_data', 'raw_transactions') }}
)

SELECT
    transaction_id,
    user_id,
    amount_gbp,
    merchant_name,
    category,
    transaction_timestamp,
    processed_at,
    -- Risk Scoring Logic
    CASE 
        WHEN amount_gbp > 1000 THEN 'CRITICAL'
        WHEN amount_gbp > 500 THEN 'HIGH'
        ELSE 'LOW'
    END as risk_level,
    -- Time-based feature engineering (The missing piece!)
    HOUR(transaction_timestamp) as hour_of_day,
    DAYNAME(transaction_timestamp) as day_of_week
FROM base_data