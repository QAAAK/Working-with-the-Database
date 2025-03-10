from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
        "email": ["santalovdv@mts.ru"],  # santalovdv
        "email_on_failure": True,
        'start_date': "2025-01-10",
        "ssh_conn_id": "fob2b-en-001.msk.bd-cloud.mts.ru",
}

# Создаем DAG
dag = DAG(
    'run_extract_script',
    default_args=default_args,
    description='Загрузка с FTP-сервера',
    schedule_interval='0 3 * * *',  # Запуск в 3 утра каждый день 
)

# Оператор для выполнения bash скрипта
run_script = BashOperator(
    task_id='run_extract_last_upd',
    bash_command='bash /path/to/your/extract_last_upd.sh',  # Измените путь к вашему скрипту
    dag=dag,
)

run_script
