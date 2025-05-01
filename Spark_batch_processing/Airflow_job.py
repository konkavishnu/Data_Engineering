from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
        dag_id='spark__air_job',
        start_date=datetime(2023, 1, 1), ----> this is the date after which the job has to start
        schedule_interval=None,  # Trigger manually
        catchup=False, ---> cath up is true means it will start all the jobs / missed jobs from "start_date" to the present date
        ) as dag:

    # Task: Run a Spark job
    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='/opt/spark/bin/spark-submit --master local ~/airflow/dags/News_Platform_Analytics.py',
    )