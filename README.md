# SentinelFlow — Real-Time Financial Transaction Risk Pipeline

**Live Dashboard:** [sentinel-flow-n6gmfksqy6qokfl92bdjyf.streamlit.app](https://sentinel-flow-n6gmfksqy6qokfl92bdjyf.streamlit.app/)

---

## Problem Statement

Small fintechs and remittance platforms typically have transactional data sitting in production databases (Laravel, Firebase, MongoDB). They have no data warehouse, no transformation layer, and no risk scoring. Analysts query raw production databases directly at end-of-day to reconcile transactions.

A read replica does not solve this — it gives a safer copy of raw data, but analysts still have no pre-modelled gold layer, no risk scoring, and no dashboard. They write SQL from scratch every EOD.

Enterprise AML tools assume you already have a warehouse underneath. **SentinelFlow builds that missing data infrastructure foundation** — from raw transaction events to a risk-scored gold layer and analyst dashboard.

**Target user:** A fraud analyst or risk employee at a small fintech or remittance platform who needs end-of-day visibility into suspicious transactions without writing SQL.

---

## Architecture

```
PaySim CSV (100k rows)
    │
    ▼
Python Producer (src/producer.py)
    │  Batch of 10 records → single JSON file
    ▼
AWS S3 Landing Zone  (sentinel-flow-raw-322b0d3a / eu-west-2)
    │  SQS event notification on ObjectCreated
    ▼
Snowpipe  (auto-ingest, sub-60s latency)
    │  COPY INTO RAW_TRANSACTIONS (VARIANT)
    ▼
Snowflake  SENTINEL_DB.RAW.RAW_TRANSACTIONS
    │
    ▼
dbt  (sentinel_analytics)
    │  Incremental merge, CLUSTER BY (type, is_fraud)
    │  Multi-signal risk scoring, window functions
    ▼
Snowflake  SENTINEL_DB.ANALYTICS.FCT_FINANCIAL_TRANSACTIONS
    │
    ▼
Streamlit Dashboard  (Streamlit Community Cloud)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Infrastructure (IaC) | Terraform |
| Cloud storage | AWS S3 (eu-west-2) |
| Ingestion | Python, Snowpipe, SQS |
| Data warehouse | Snowflake |
| Transformation | dbt (incremental, clustered) |
| Dashboard | Streamlit |
| Dataset | PaySim (academically validated synthetic mobile money data) |

---

## Dataset

[PaySim on Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1) — synthetic dataset based on real mobile money transaction logs from a multinational provider operating in 14+ countries. Used under academic research terms.

We ingest the first 100,000 rows (full 6.3M would take ~87 hours to stream at current batch rate).

PaySim schema used: `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`

Fields added at ingestion: `transaction_id` (uuid4), `timestamp` (derived from step as hour offset from 2024-01-01), `ingested_at` (real ingestion time).

---

## Prerequisites

- Python 3.10+
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [dbt Core](https://docs.getdbt.com/docs/core/installation) + `dbt-snowflake`
- AWS account with CLI configured (`aws configure`)
- Snowflake account (free trial sufficient)
- PaySim CSV downloaded from Kaggle and placed at `src/paysim1.csv`

---

## Setup — Step by Step

### 1. Clone the repo

```bash
git clone <your-repo-url>
cd sentinel-flow
```

### 2. Provision AWS S3 with Terraform

```bash
cd terraform
terraform init
terraform apply
```

Note the output `bucket_name` — it should match `BUCKET_NAME` in `src/producer.py`.

### 3. Set up Snowflake manually

In a Snowflake worksheet as `ACCOUNTADMIN`:

```sql
CREATE WAREHOUSE SENTINEL_WH WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE SENTINEL_DB;
CREATE SCHEMA SENTINEL_DB.RAW;
CREATE SCHEMA SENTINEL_DB.ANALYTICS;

CREATE ROLE SENTINEL_ROLE;
GRANT USAGE ON WAREHOUSE SENTINEL_WH TO ROLE SENTINEL_ROLE;
GRANT ALL ON DATABASE SENTINEL_DB TO ROLE SENTINEL_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE SENTINEL_DB TO ROLE SENTINEL_ROLE;
GRANT ROLE SENTINEL_ROLE TO USER <your_username>;
```

### 4. Configure key-pair authentication

```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/snowflake_key.p8 -nocrypt

# Generate public key
openssl rsa -in ~/.ssh/snowflake_key.p8 -pubout -out ~/.ssh/snowflake_key.pub
```

In Snowflake worksheet:
```sql
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<contents of snowflake_key.pub>';
```

### 5. Configure dbt profile

Create `~/.dbt/profiles.yml`:

```yaml
sentinel_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account_identifier>
      user: <your_username>
      private_key_path: ~/.ssh/snowflake_key.p8
      authenticator: snowflake_jwt
      role: SENTINEL_ROLE
      database: SENTINEL_DB
      warehouse: SENTINEL_WH
      schema: ANALYTICS
      threads: 1
```

Verify:
```bash
cd sentinel_analytics
dbt debug
```

### 6. Set up Snowpipe

Create a Snowflake storage integration (run as `ACCOUNTADMIN`):

```sql
CREATE OR REPLACE STORAGE INTEGRATION sentinel_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket-name>/raw/');

DESC INTEGRATION sentinel_s3_integration;
```

Copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`. Create an IAM role in AWS with the S3 read policy and update its trust policy with these values.

Then in Snowflake:

```sql
USE ROLE SENTINEL_ROLE;
USE DATABASE SENTINEL_DB;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT json_file_format
    TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE TABLE RAW_TRANSACTIONS (
    raw_json VARIANT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STAGE sentinel_s3_stage
    URL = 's3://<your-bucket-name>/raw/'
    STORAGE_INTEGRATION = sentinel_s3_integration
    FILE_FORMAT = json_file_format;

CREATE OR REPLACE PIPE sentinel_pipe
    AUTO_INGEST = TRUE
AS
    COPY INTO RAW_TRANSACTIONS(raw_json)
    FROM @sentinel_s3_stage
    FILE_FORMAT = (FORMAT_NAME = json_file_format);

SHOW PIPES;
```

Copy the `notification_channel` SQS ARN from `SHOW PIPES`. In AWS Console → S3 → your bucket → Properties → Event notifications, create a notification:
- Prefix: `raw/`
- Event: `s3:ObjectCreated:*`
- Destination: the SQS ARN above

Resume the pipe:
```sql
USE ROLE ACCOUNTADMIN;
ALTER PIPE SENTINEL_DB.RAW.SENTINEL_PIPE RESUME;
```

### 7. Stream data

```bash
cd sentinel-flow
python src/producer.py
```

Verify data is landing:
```sql
SELECT COUNT(*) FROM SENTINEL_DB.RAW.RAW_TRANSACTIONS;
```

### 8. Run dbt

```bash
cd sentinel_analytics
dbt run
dbt test
```

All 9 tests should pass.

### 9. View the dashboard

[sentinel-flow-n6gmfksqy6qokfl92bdjyf.streamlit.app](https://sentinel-flow-n6gmfksqy6qokfl92bdjyf.streamlit.app/)

---

## Data Warehouse Design

### Incremental Materialisation
`fct_financial_transactions` uses `incremental` strategy with `merge`. Only records where `ingested_at > MAX(ingested_at)` in the existing table are processed on each run. This minimises Snowflake compute costs at scale.

### Clustering
```sql
CLUSTER BY (type, is_fraud)
```
Dashboard queries filter on `type` and `is_fraud`. Snowflake uses micro-partition pruning on these columns — analytical queries run faster and cheaper.

---

## Risk Scoring Logic

| Risk Level | Condition |
|------------|-----------|
| `CONFIRMED_FRAUD` | `is_fraud = 1` (PaySim ground truth) |
| `SUSPICIOUS_DRAIN` | `balance_drain_ratio >= 1.0` on TRANSFER or CASH_OUT |
| `SUSPICIOUS_SPIKE` | `amount > 2× rolling avg of last 10 txns` (avg > 0 guard) |
| `FLAGGED` | `is_flagged_fraud = 1` (transfer > 200,000) |
| `HIGH_VELOCITY` | `txn_count_last_hour > 5` on TRANSFER or CASH_OUT |
| `LOW` | All other transactions |

Drain and velocity signals are restricted to TRANSFER and CASH_OUT — PaySim fraud only occurs in these transaction types. The `avg > 0` guard prevents false positives on a user's first transaction.

---

## dbt Tests

9 automated data quality tests on `fct_financial_transactions`:

| Test | Column |
|------|--------|
| `unique` | `transaction_id` |
| `not_null` | `transaction_id`, `type`, `amount`, `is_fraud`, `risk_level` |
| `accepted_values` | `type` (5 values), `is_fraud` (0/1), `risk_level` (6 values) |

---

## Project Structure

```
sentinel-flow/
├── src/
│   └── producer.py               # PaySim → S3 batch uploader
├── sentinel_analytics/
│   ├── dbt_project.yml
│   └── models/marts/
│       ├── fct_financial_transactions.sql
│       └── schema.yml
├── terraform/
│   └── main.tf                   # S3 bucket + IAM policy
├── dashboard/
│   ├── app.py                    # Streamlit dashboard
│   └── requirements.txt
└── README.md
```
