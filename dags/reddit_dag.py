import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Add the parent directory to path so we can import 'pipelines'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': '25exy',
    'start_date': datetime(2024, 3, 7),
    'retries': 1, # Always retry external API calls
    'retry_delay': 300
}

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# Extraction Task (Atomic: Extract -> Transform -> Upload S3)
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    provide_context=True,
    op_kwargs={
        # Using Jinja template for dynamic filename (e.g., 'reddit_20251209')
        # This fixes the scheduler date bug
        'file_name': 'reddit_{{ ds_nodash }}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100,
        
        # Pass credentials securely via Airflow Variables
        # (Make sure to set these in Airflow UI -> Admin -> Variables)
        'client_id': Variable.get("reddit_client_id", default_var="YOUR_FALLBACK_ID"),
        'client_secret': Variable.get("reddit_client_secret", default_var="YOUR_FALLBACK_SECRET"),
        'aws_access_key_id': Variable.get("aws_access_key", default_var="YOUR_AWS_KEY"),
        'aws_secret_access_key': Variable.get("aws_secret_key", default_var="YOUR_AWS_SECRET"),
        'bucket_name': Variable.get("aws_bucket_name", default_var="your-s3-bucket-name")
    },
    dag=dag
)

# No downstream dependency needed since the task is atomic