from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG('first_dag', description='Our First DAG', 
          schedule_interval='0 12 * * *',
          start_date=datetime(2025, 12, 1), catchup=False)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
