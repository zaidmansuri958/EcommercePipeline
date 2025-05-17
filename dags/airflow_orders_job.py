from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.providers.aws.hooks.s3 import S3Hook
import os


default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag=DAG(
    'spark_job',
    default_args=default_args,
    description='spark job with airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025,5,14),
    catchup=False,
    tags=['dev']
)


def upload_to_s3():
    files=os.listdir("./data")
    hook=S3Hook(aws_conn_id='')
    for filename in files:
        hook.load_file(
            filename=filename,
            key=f'raw/{filename}',
            bucket_name='zaid-orders-pipeline',
            replace=True 
        )

s3_object_opload=PythonOperator(
    taks_id='upload_to_s3',
    python_callable=upload_to_s3
)


s3_object_opload

