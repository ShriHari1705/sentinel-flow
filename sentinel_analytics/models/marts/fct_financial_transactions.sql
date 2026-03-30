{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        cluster_by=['type', 'is_fraud']
    )
}}

WITH source_data AS (
    SELECT
        raw_json:transaction_id::string      AS transaction_id,
        raw_json:step::int                   AS step,
        raw_json:type::string                AS type,
        raw_json:amount::decimal(18,2)       AS amount,
        raw_json:name_orig::string           AS name_orig,
        raw_json:old_balance_orig::decimal(18,2) AS old_balance_orig,
        raw_json:new_balance_orig::decimal(18,2) AS new_balance_orig,
        raw_json:name_dest::string           AS name_dest,
        raw_json:old_balance_dest::decimal(18,2) AS old_balance_dest,
        raw_json:new_balance_dest::decimal(18,2) AS new_balance_dest,
        raw_json:is_fraud::int               AS is_fraud,
        raw_json:is_flagged_fraud::int       AS is_flagged_fraud,
        raw_json:timestamp::timestamp        AS transaction_timestamp,
        raw_json:ingested_at::timestamp      AS ingested_at
    FROM {{ source('raw_data', 'raw_transactions') }}

    {% if is_incremental() %}
        -- Only process records newer than the latest ingested record
        -- This is the incremental filter that saves compute cost
        WHERE raw_json:ingested_at::timestamp > (
            SELECT MAX(ingested_at) FROM {{ this }}
        )
    {% endif %}
),

user_behaviour AS (
    SELECT
        *,

        -- Rolling average amount over last 10 transactions per user
        -- Captures the user's normal spending baseline
        AVG(amount) OVER (
            PARTITION BY name_orig
            ORDER BY transaction_timestamp
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS avg_last_10_txns,

        -- Transaction count in last hour per user
        -- Velocity signal: high frequency = suspicious
        COUNT(transaction_id) OVER (
            PARTITION BY name_orig
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '1 HOUR' PRECEDING AND CURRENT ROW
        ) AS txn_count_last_hour,

        -- Balance drain ratio: how much of the original balance was sent
        -- A ratio of 1.0 means the account was completely emptied
        CASE
            WHEN old_balance_orig = 0 THEN 0
            ELSE ROUND(
                (old_balance_orig - new_balance_orig) / old_balance_orig,
                4
            )
        END AS balance_drain_ratio

    FROM source_data
),

final AS (
    SELECT
        transaction_id,
        step,
        type,
        amount,
        name_orig,
        old_balance_orig,
        new_balance_orig,
        name_dest,
        old_balance_dest,
        new_balance_dest,
        is_fraud,
        is_flagged_fraud,
        transaction_timestamp,
        ingested_at,
        avg_last_10_txns,
        txn_count_last_hour,
        balance_drain_ratio,

        -- Multi-signal risk scoring
        -- Ordered by severity: confirmed fraud first, LOW last
        CASE
            WHEN is_fraud = 1
                THEN 'CONFIRMED_FRAUD'

            WHEN type IN ('TRANSFER', 'CASH_OUT')
                AND balance_drain_ratio >= 1.0
                AND amount > 0
                THEN 'SUSPICIOUS_DRAIN'

            WHEN amount > (2 * avg_last_10_txns)
                AND amount > 50
                AND avg_last_10_txns > 0
                THEN 'SUSPICIOUS_SPIKE'

            WHEN is_flagged_fraud = 1
                THEN 'FLAGGED'

            WHEN txn_count_last_hour > 5
                AND type IN ('TRANSFER', 'CASH_OUT')
                THEN 'HIGH_VELOCITY'

            ELSE 'LOW'
        END AS risk_level,

        -- Temporal features for dashboard tile 2
        HOUR(transaction_timestamp)    AS hour_of_day,
        DAYNAME(transaction_timestamp) AS day_of_week,
        DATE(transaction_timestamp)    AS transaction_date

    FROM user_behaviour
)

SELECT * FROM final