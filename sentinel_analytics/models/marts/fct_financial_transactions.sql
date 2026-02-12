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
),

user_stats AS (
    SELECT 
        *,
        -- Window Function: Calculate average spending for this user over their last 10 transactions
        AVG(amount_gbp) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_timestamp 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as avg_last_10_txns
    FROM base_data
)

SELECT
    transaction_id,
    user_id,
    amount_gbp,
    merchant_name,
    category,
    transaction_timestamp,
    processed_at,
    avg_last_10_txns,
    -- Advanced Risk Scoring Logic
    CASE 
        -- Flag if transaction is 2x the user's normal average
        WHEN amount_gbp > (2 * avg_last_10_txns) AND amount_gbp > 50 THEN 'SUSPICIOUS_SPIKE'
        WHEN amount_gbp > 1000 THEN 'CRITICAL'
        WHEN amount_gbp > 500 THEN 'HIGH'
        ELSE 'LOW'
    END as risk_level,
    HOUR(transaction_timestamp) as hour_of_day,
    DAYNAME(transaction_timestamp) as day_of_week
FROM user_stats