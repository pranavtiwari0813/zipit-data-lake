# ZIPIT — Food Delivery Data Lake 

> Production-grade AWS Data Lake built for food delivery analytics.
> Processes 5,000+ orders, 500 customers, 200 riders across
> 7 Indian cities using Python, boto3, AWS Glue, Athena,
> CloudWatch, and CloudFormation.

---

## Project Overview

ZIPIT is a food delivery platform concept (similar to Zomato/Swiggy).
This project builds a complete Data Lake on AWS to store, process,
and analyze all ZIPIT business data — orders, payments, riders,
restaurants, and customers.

---

## Architecture
ZIPIT App Events
|
+-- live_producer.py   → Live orders to S3 every 3 sec
+-- batch_upload.py    → Bulk CSV upload to S3
|
v
Amazon S3 Data Lake  [ap-south-1 — Mumbai]
|
+-- raw/           Original CSV + JSON (Bronze Layer)
+-- processed/     Clean Parquet files (Silver Layer)
+-- curated/       Business KPI tables (Gold Layer)
+-- athena-results/
|
v
Glue Data Catalog → Amazon Athena (8 SQL Queries)
+
CloudWatch (3 Alarms + Pipeline Health Dashboard)
+
CloudFormation (Infrastructure as Code)

---
 
## Tech Stack

| Layer      | Service          | Purpose                    |
|------------|------------------|----------------------------|
| Ingestion  | Python + boto3   | Live + batch upload to S3  |
| Storage    | Amazon S3        | 3-zone data lake           |
| Catalog    | AWS Glue Crawler | Auto schema detection      |
| Transform  | AWS Glue ETL     | PySpark cleaning + Parquet |
| Query      | Amazon Athena    | Serverless SQL on S3       |
| Monitor    | CloudWatch       | Alarms + dashboard         |
| IaC        | CloudFormation   | One-command deployment     |
| Security   | AWS IAM          | Least-privilege roles      |

---

## Data Lake Zones (Medallion Architecture)

| Zone | Folder | Description |
|---|---|---|
| Bronze | raw/ | Original data exactly as uploaded |
| Silver | processed/ | Cleaned Parquet — nulls removed, types fixed |
| Gold | curated/ | Business KPIs — revenue, rankings, trends |

---

## ZIPIT Datasets

| Table | Records | Description |
|---|---|---|
| orders | 5,000 | Food orders across 7 cities |
| customers | 500 | Customer profiles + loyalty points |
| restaurants | 100 | Restaurant details + ratings |
| riders | 200 | Delivery rider profiles |
| payments | 4,750+ | Payment transactions |

---

## Business SQL Queries (Athena)

| Query | Business Question |
|---|---|
| Q1 | Which city earns ZIPIT the most revenue? |
| Q2 | Which are the top 10 performing restaurants? |
| Q3 | What is the average delivery speed per city? |
| Q4 | Which food items are ordered the most? |
| Q5 | Is UPI taking over Cash on Delivery? |
| Q6 | Which cities have the highest cancellation rates? |
| Q7 | Who are the top 10 ZIPIT riders? |
| Q8 | Is ZIPIT growing day by day? |

---

## Key Results

- ✅ 5,000 food delivery orders processed end-to-end
- ✅ 87% file size reduction — CSV converted to Parquet
- ✅ 8 business SQL queries running on S3 via Athena
- ✅ 3 CloudWatch alarms monitoring the pipeline
- ✅ Infrastructure deployed with 1 CLI command
- ✅ SSE-S3 encryption + versioning + lifecycle policies

---

## Security Implementation

- IAM user with least-privilege permissions
- MFA enabled on root account
- Block all public access on S3
- SSE-S3 AES-256 encryption on all files
- S3 versioning — restore any deleted file
- Lifecycle policy — 80% cost saving on old data

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/pranavtiwari0813/zipit-data-lake
cd zipit-data-lake

# Install dependencies
pip install -r requirements.txt

# Add AWS keys to .env file
# AWS_ACCESS_KEY_ID=your_key
# AWS_SECRET_ACCESS_KEY=your_secret
# AWS_REGION=ap-south-1
# ZIPIT_BUCKET=zipit-datalake-2026

# Generate ZIPIT sample data and upload to S3
python sample_data/generate_all.py

# Upload CSVs to S3
python ingestion/batch_upload.py

# Run ETL — raw CSV to processed Parquet
python glue_jobs/local_etl.py

# Build curated KPI tables
python glue_jobs/local_curated.py

# Start live order simulation (press Ctrl+C to stop)
python ingestion/live_producer.py
```

---

## Project Structure

zipit-data-lake/
├── ingestion/
│   ├── live_producer.py      Live ZIPIT orders → S3
│   └── batch_upload.py       Bulk CSV → S3
├── sample_data/
│   ├── generate_all.py       Generate 5 ZIPIT datasets
│   └── setup_s3_structure.py Create S3 folder structure
├── glue_jobs/
│   ├── local_etl.py          Raw CSV → Processed Parquet
│   ├── local_curated.py      Processed → Curated KPIs
│   └── check_types.py        Check Parquet column types
├── athena_queries/           8 business SQL queries
├── cloudwatch/               Monitoring setup scripts
├── docs/                     Screenshots and report
├── template.yaml             CloudFormation IaC
├── requirements.txt          Python dependencies
└── .env                      AWS credentials (gitignored)

---

Built by **Pranav Tiwari** | GLA University, Mathura | 2026

