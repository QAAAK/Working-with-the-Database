from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
        "email": ["*****"],  # ваша почта
        "email_on_failure": True,
        'start_date': "2025-01-10",
        "ssh_conn_id": "******", # указать ноду, где располагается Aifrlow
}

# Создаем DAG
dag = DAG(
    'run_extract_script_sftp',
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
