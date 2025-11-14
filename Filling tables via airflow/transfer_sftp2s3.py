from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.sftp.transfers.sftp_to_s3 import SFTPToS3Operator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_transfer_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['transfer', 'etl'],
) as dag:

    # Transfer из SFTP в S3
    sftp_to_s3 = SFTPToS3Operator(
        task_id='sftp_to_s3_transfer',
        sftp_conn_id='sftp_connection',
        sftp_path='/remote/path/file.csv',
        s3_conn_id='aws_default',
        s3_bucket='destination-bucket',
        s3_key='path/to/file.csv',
    )

sftp_to_s3
