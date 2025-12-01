# End-to-End ETL Pipeline Integration for Reddit Data, Airflow, Celery, Postgres, S3, AWS Glue, Athena, and Redshift

This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into a Redshift data warehouse. The pipeline leverages a combination of tools and services including Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)

## Overview

The pipeline is designed to:

1. Extract data from Reddit using its API.
2. Store the raw data into an S3 bucket from Airflow.
3. Transform the data using AWS Glue and Amazon Athena.
4. Load the transformed data into Amazon Redshift for analytics and querying.

## Architecture
![RedditDataEngineering.png](assets/RedditDataEngineering.png)
1. **Reddit API**: Source of the data.
2. **Apache Airflow & Celery**: Orchestrates the ETL process and manages task distribution.
3. **PostgreSQL**: Temporary storage and metadata management.
4. **Amazon S3**: Raw data storage.
5. **AWS Glue**: Data cataloging and ETL jobs.
6. **Amazon Athena**: SQL-based data transformation.
7. **Amazon Redshift**: Data warehousing and analytics.

## Architectural Design Decisions

### 1. The Lakehouse Pattern (S3 + Redshift)
* **Design:** I implemented a "Bronze/Silver" data lake architecture.
    * **Raw Zone (Bronze):** Stores the immutable JSON extracted directly from the Reddit API.
    * **Transformed Zone (Silver):** Stores the cleaned, Parquet-formatted data after AWS Glue processing.
* **Reasoning:** This separation allows me to re-process the raw data if business logic changes without needing to query the API again (which ensures idempotency).

### 2. Schema Evolution with Glue Crawlers
* **Challenge:** Social media APIs frequently change their response structure (adding/removing fields).
* **Solution:** Instead of hard-coding schemas, I utilized **AWS Glue Crawlers** to scan the S3 buckets. The Crawlers automatically infer the schema and update the **AWS Data Catalog**. This allows the Glue ETL jobs to use a dynamic frame that adapts to schema drifts without breaking the pipeline.

### 3. Dual Consumption Layers (Athena vs. Redshift)
* **Design:** The pipeline exposes data via both Amazon Athena and Redshift.
* **Trade-off Analysis:**
    * **Athena:** Used for low-cost, ad-hoc analysis directly on S3 data (serverless).
    * **Redshift:** Used as the Data Warehouse for high-concurrency BI reporting (PowerBI/Tableau), providing faster query performance for aggregations than scanning S3 files repeatedly.

## Prerequisites
- AWS Account with appropriate permissions for S3, Glue, Athena, and Redshift.
- Reddit API credentials.
- Docker Installation
- Python 3.9 or higher

## System Setup
1. Clone the repository.
   ```bash
    git clone https://github.com/airscholar/RedditDataEngineering.git
   ```
2. Create a virtual environment.
   ```bash
    python3 -m venv rde
   ```
3. Activate the virtual environment.
   ```bash
    source rde/bin/activate
   ```
4. Install the dependencies.
   ```bash
    pip install -r requirements.txt
   ```
5. Rename the configuration file and the credentials to the file.
   ```bash
    mv config/config.conf.example config/config.conf
   ```
6. Starting the containers
   ```bash
    docker-compose up -d
   ```
7. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```
