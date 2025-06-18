import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
        "email": ["santalovdv@mts.ru"],  # santalovdv
        "email_on_failure": True,
        'start_date': "2025-06-18",
        "ssh_conn_id": "oka-analyze-en-001.krd.bd-cloud.mts.ru", # сервер, где делает потоки/запросы. в терминале написать hostnamectl
}



with DAG (
    dag_id = "dag",
    default_args=default_args,
    schedule_interval = '0 2 * * *',
    tags = ['python','bash','dummy'],
    max_active_runs=1, # один инстанс
    catchup = False) as dag:
    
    
    dummy = DummyOperator(task_id='dummy_pass')
    
    echo = BashOperator(task_id='echo_bash', bash_command = 'echo {{ds}}', dag=dag)
    
    def log_func():
        logging.info('Log')
        
    t1 = PythonOperator(task_id='out_log', python_callable=log_func, dag=dag)
    
    dummy >> [echo, t1]
    
    