# Reddit Data Engineering Pipeline

A production-grade ETL pipeline that extracts Reddit data, transforms it using streaming logic, and loads it into an AWS Lakehouse architecture (S3 + Redshift). Orchestrated by Apache Airflow with robust CI/CD and automated unit testing.

![System Architecture](assets/Reddit_Data_Engineering.png)

## 🚀 Key Engineering Features

Unlike standard scripts, this pipeline is built with **scalability** and **reliability** as core tenets:

* **Atomic ETL Design:** Combined Extraction and S3 Upload into a single atomic task to prevent race conditions in distributed environments (Celery Executors).
* **Memory Efficiency (O(1)):** Utilizes Python **Generators** (`yield`) to stream data row-by-row, ensuring constant memory usage regardless of dataset size.
* **Idempotency:** Implements "Upsert" logic in Redshift to ensure re-running the pipeline does not create duplicate records.
* **Object-Oriented Design:** Encapsulates external API logic in a modular `RedditClient` class for better state management and testability.
* **Automated CI/CD:** GitHub Actions workflow automatically runs `pytest` suites on every push to ensure code stability.

## 🛠 Tech Stack

* **Orchestration:** Apache Airflow (running on Docker with CeleryExecutor)
* **Language:** Python 3.9 (Pandas removed for lightweight `csv` streaming)
* **Cloud (AWS):** S3 (Data Lake), Glue (Transform), Redshift (Warehousing), Athena (Ad-hoc Query)
* **Quality Assurance:** `pytest` (Unit Testing), `unittest.mock` (API Mocking), GitHub Actions (CI)

## 🏗 Architecture Decisions

### 1. Ingestion Strategy (Generators vs. Lists)
* **Challenge:** Loading 100,000+ posts into a Python list or Pandas DataFrame causes OOM (Out of Memory) kills on smaller worker nodes.
* **Solution:** Refactored extraction logic to use Python **Generators**. This streams one post at a time through the transformation layer, keeping memory footprint low (~50MB) even for GB-scale data.

### 2. Handling Distributed Race Conditions
* **Challenge:** In a `CeleryExecutor` setup, Task A (Extract) might run on *Worker 1* (saving to local disk), while Task B (Upload) runs on *Worker 2*, causing a "File Not Found" failure.
* **Solution:** Implemented an **Atomic Extract-Load pattern**. The extraction task immediately streams data to S3 and cleans up local temporary files within the same execution context.

### 3. Testing Strategy
* **Unit Tests:** Validates transformation logic (timestamps, type casting) to ensure data quality.
* **Mocking:** Uses `unittest.mock` to simulate Reddit API responses, allowing the test suite to run in CI environments without internet access or API credentials.

## ⚙️ Setup & Installation

### 1. Environment Variables
This project uses **Airflow Variables** for secure credential management. Add the following to your `airflow.env` or Airflow UI:

```properties
AIRFLOW_VAR_REDDIT_CLIENT_ID=your_id
AIRFLOW_VAR_REDDIT_CLIENT_SECRET=your_secret
AIRFLOW_VAR_AWS_ACCESS_KEY=your_aws_key
AIRFLOW_VAR_AWS_SECRET_KEY=your_aws_secret
AIRFLOW_VAR_AWS_BUCKET_NAME=your_s3_bucket
```

### 2. Run Locally
```bash
# Start Airflow services
docker-compose up -d

# Trigger the DAG
docker exec -it airflow-webserver airflow dags trigger etl_reddit_pipeline
```

## 🧪 Running Tests
This project enforces code quality via `pytest`.

```bash
# Install test dependencies
pip install pytest mock

# Run test suite
python -m pytest tests/
```

