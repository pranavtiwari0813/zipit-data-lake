# ZIPIT — Food Delivery Data Lake

> Production-grade AWS Data Lake for food delivery analytics.
> Processes **5,000+ orders** across **7 Indian cities** using
> Python, boto3, AWS Glue, Athena, CloudWatch, and CloudFormation.

![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena%20%7C%20CloudWatch-orange?logo=amazon-aws)
![Python](https://img.shields.io/badge/Python-boto3%20%7C%20pandas%20%7C%20pyarrow-blue?logo=python)
![IaC](https://img.shields.io/badge/IaC-CloudFormation-green?logo=amazon-aws)

---

## How Data Flows

**ZIPIT App** → **Python boto3** → **Amazon S3** → **AWS Glue** → **Amazon Athena** → **Business Insights**

| Step | What Happens |
|------|-------------|
| 1. Ingest | `live_producer.py` writes live orders to S3 every 3 seconds |
| 2. Ingest | `batch_upload.py` uploads all 5 CSV files to S3 raw zone |
| 3. Transform | `local_etl.py` cleans data and converts CSV to Parquet (87% smaller) |
| 4. Curate | `local_curated.py` builds 5 business KPI tables |
| 5. Catalog | Tables registered in AWS Glue Data Catalog via Athena DDL |
| 6. Query | 8 SQL queries run on S3 Parquet files via Amazon Athena |
| 7. Monitor | CloudWatch alarms + dashboard watch everything 24/7 |

---

## Data Lake Zones (Medallion Architecture)

| Zone | S3 Folder | What is Stored |
|------|-----------|---------------|
| Bronze (Raw) | `raw/` | Original CSV files — never modified |
| Silver (Processed) | `processed/` | Clean Parquet files — 87% smaller than CSV |
| Gold (Curated) | `curated/` | Business KPI tables — ready for analysis |

---

## ZIPIT Datasets

| Table | Records | Key Columns |
|-------|---------|-------------|
| orders | 5,000 | order_id, item, total_amount, city, status, delivery_mins |
| customers | 500 | customer_id, name, city, loyalty_points |
| restaurants | 100 | restaurant_id, name, cuisine, rating, city |
| riders | 200 | rider_id, name, vehicle_type, rating, city |
| payments | 4,750+ | payment_id, amount, method, gateway, status |

---

## 8 Business SQL Queries (Athena)

| Query | Business Question Answered |
|-------|---------------------------|
| Q1 — Revenue by City | Which city earns ZIPIT the most money? |
| Q2 — Top Restaurants | Which restaurants to show on home screen? |
| Q3 — Delivery Speed | Which city has the fastest deliveries? |
| Q4 — Popular Items | What goes on the Trending Now section? |
| Q5 — Payment Split | Is UPI overtaking Cash on Delivery? |
| Q6 — Cancellation Rate | Which cities cancel the most orders? |
| Q7 — Top Riders | Who are the star ZIPIT riders? |
| Q8 — Daily Growth | Is ZIPIT growing day by day? |

---

## Tech Stack

| Layer | Service | Purpose |
|-------|---------|---------|
| Ingestion | Python + boto3 | Live and batch upload to S3 |
| Storage | Amazon S3 | 3-zone data lake with encryption |
| Transform | pandas + pyarrow | ETL pipeline — CSV to Parquet |
| Catalog | AWS Glue + Athena DDL | Schema management |
| Query | Amazon Athena | Serverless SQL directly on S3 |
| Monitor | CloudWatch + SNS | Alarms and live dashboard |
| IaC | CloudFormation | One-command full stack deploy |
| Security | AWS IAM | Least-privilege access control |

---

## Key Results

| Metric | Value |
|--------|-------|
| Orders processed | 5,000+ end-to-end |
| File size reduction | 87% (CSV to Parquet) |
| SQL queries | 8 business intelligence queries |
| CloudWatch alarms | 3 automated email alerts |
| Deploy command | 1 CLI command for full stack |
| Cost | Rs.0 — built entirely on AWS Free Tier |

---

## Security

| Feature | Implementation |
|---------|---------------|
| IAM | Least-privilege user — only needed permissions |
| Root Account | MFA enabled with Google Authenticator |
| S3 Public Access | All 4 public access blocks enabled |
| Encryption | SSE-S3 AES-256 on every file |
| Versioning | S3 versioning — restore deleted files |
| Lifecycle | Auto-archive to Glacier — 80% cost saving |

---

## Project Structure

| Folder / File | Purpose |
|---------------|---------|
| `ingestion/live_producer.py` | Simulates live ZIPIT orders to S3 every 3 sec |
| `ingestion/batch_upload.py` | Uploads all CSV files to S3 raw zone |
| `sample_data/generate_all.py` | Generates 5 realistic ZIPIT datasets |
| `glue_jobs/local_etl.py` | ETL — raw CSV to processed Parquet |
| `glue_jobs/local_curated.py` | Builds 5 curated business KPI tables |
| `glue_jobs/check_types.py` | Checks actual Parquet column types |
| `athena_queries/` | All 8 business SQL queries |
| `cloudwatch/` | Monitoring setup scripts |
| `template.yaml` | CloudFormation Infrastructure as Code |
| `requirements.txt` | Python dependencies |

---

## Quick Start

```bash
git clone https://github.com/pranavtiwari0813/zipit-data-lake
cd zipit-data-lake
pip install -r requirements.txt

# Add AWS keys to .env file then run:
python sample_data/generate_all.py
python ingestion/batch_upload.py
python glue_jobs/local_etl.py
python glue_jobs/local_curated.py
python ingestion/live_producer.py
```

---

Built by **Pranav Tiwari** | GLA University, Mathura | 2026
