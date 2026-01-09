# SentinelFlow: End-to-End Real-Time Financial Pipeline

## 🚀 Executive Summary
SentinelFlow is a cloud-native data engineering project that simulates, ingests, and transforms high-frequency financial transaction data. The pipeline moves data from a local Python producer to an AWS S3 Landing Zone, through Snowflake Snowpipe for real-time ingestion, and finally into a 'Gold' analytical layer using dbt.

## 🛠️ Tech Stack
* **Infrastructure:** Terraform (IaC)
* **Cloud:** AWS (S3, IAM)
* **Data Warehouse:** Snowflake (Snowpipe, Variant Ingestion)
* **Analytics Engineering:** dbt (Modelling, Testing, Documentation)
* **Orchestration:** Event-driven (S3 SQS Notifications)

---

## 🚧 Technical Challenges & Resolutions (The Engineering Log)

### 1. Cloud Security & Connectivity (The 403 Hurdle)
* **Issue:** Persistent `Access Denied` errors when Snowflake attempted to list S3 objects, despite valid credentials.
* **Resolution:** Identified a "Split-Resource" policy requirement in IAM. I refactored the AWS policy to explicitly grant `ListBucket` permissions on the **Bucket ARN** while restricting `GetObject/PutObject` to the **Object ARN** (`/*`).
* **Logic:** Adhering to the *Principle of Least Privilege* ensures that the ingestion service can only access the specific prefix required for the pipeline.

### 2. Infrastructure-as-Code (Terraform State Management)
* **Issue:** Managing resource parity between local development and cloud state.
* **Resolution:** Used Terraform to manage the lifecycle of the S3 Landing Zone. Implemented strict `.gitignore` protocols to prevent sensitive `tfstate` and credential leakage.
* **Logic:** Ensures that the environment is reproducible and prevents "Configuration Drift."

### 3. Semi-Structured Data Ingestion (Snowpipe)
* **Issue:** Transitioning from manual `COPY INTO` batches to real-time automation.
* **Resolution:** Configured **Snowpipe** with SQS event notifications. I resolved a "Pipe not authorized" error by correctly configuring the Snowflake Role hierarchy (`ACCOUNTADMIN` vs `SENTINEL_ROLE`).
* **Logic:** Serverless ingestion reduces data latency to sub-60 seconds, critical for real-time fraud detection.

### 4. dbt Compilation & Namespace Clashing
* **Issue:** dbt compilation failed due to duplicate source definitions (`raw_data_raw_transactions`).
* **Resolution:** Consolidated multiple YAML configurations into a single, governed `schema.yml` and migrated to the modern `data_tests` syntax.
* **Logic:** Dry (Don't Repeat Yourself) code principles within dbt ensure the manifest remains clean and scalable.

---

## 📈 Data Transformation Logic (Gold Layer)
In the `fct_financial_transactions` model, I implemented:
* **Risk Scoring:** Categorizing transactions as `CRITICAL`, `HIGH`, or `LOW` based on GBP volume.
* **Feature Engineering:** Extracting `hour_of_day` and `day_of_week` from raw ISO-8601 timestamps to enable temporal fraud pattern analysis.
* **Data Quality:** Integrated automated tests for `uniqueness` and `non-null` values on the transaction Primary Key.