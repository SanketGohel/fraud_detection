from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.apache.spark.operators.databricks import DatabricksRunNowOperator
import os

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fraud_detection_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)


# Task 1: Generate & Load User Data into Delta Lake
def generate_users():
    os.system("python3 dags/scripts/generate_users.py")

load_users_task = PythonOperator(
    task_id="load_users",
    python_callable=generate_users,
    dag=dag,
)

# Task 2: Stream Transactional Data to Kafka
# def stream_data():
#     os.system("python3 dags/scripts/streaming_data.py")

# streaming_task = PythonOperator(
#     task_id="stream_transactions",
#     python_callable=stream_data,
#     dag=dag,
# )

# Task 3: Process Data in Databricks
# databricks_process_task = DatabricksRunNowOperator(
#     task_id="process_in_databricks",
#     databricks_conn_id="databricks_default",
#     job_id="1234",  # Replace with your Databricks Job ID
#     dag=dag,
# )

# DAG Execution Order
load_users_task #>> streaming_task #>> databricks_process_task
