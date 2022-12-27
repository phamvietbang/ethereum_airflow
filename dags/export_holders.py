from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from services.athena import AthenaS3StreamingExporter

athena_s3 = AthenaS3StreamingExporter(
    access_key="AKIAXS2SFBSOTZFERP5O",
    secret_key="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",
    bucket="bangbich123",
    database="onus",
    aws_region="us-east-1"
)


def create_database():
    query_execution_id = athena_s3.create_database()
    # while not athena_s3.has_query_succeeded(query_execution_id):
    #     pass

    query_execution_id = athena_s3.create_event_table()
    # while not athena_s3.has_query_succeeded(query_execution_id):
    #     pass


def export_holders():
    query_execution_id = athena_s3.export_holders()
    # while not athena_s3.has_query_succeeded(query_execution_id):
    #     pass


default_args = {
    'owner': "bangpv",
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        default_args=default_args,
        dag_id='Athena_02',
        start_date=datetime(2022, 12, 26),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='create_database',
        python_callable=create_database
    )
    task2 = PythonOperator(
        task_id='export_holders',
        python_callable=export_holders
    )
    task1 >> task2
