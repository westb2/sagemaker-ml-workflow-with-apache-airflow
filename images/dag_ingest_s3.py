from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.ingest_operator import S3IngestOperator

dag = DAG("handle-request-ingest-s3", description="Simple tutorial DAG",
          start_date=datetime(2019, 1, 1), catchup=False)

ingest_operator = S3IngestOperator(
    task_id="ingest_from_s3", 
    sourceBucket="tony-mlp-demo-files", 
    sourceFilePattern="**/candy-data*.csv", 
    assetName="candy-data-startedByAirflow",
    dag=dag)
