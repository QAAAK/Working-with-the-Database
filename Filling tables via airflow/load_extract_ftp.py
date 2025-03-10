from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Определяем параметры DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),  # Укажите дату начала выполнения
}

# Создаем DAG
dag = DAG(
    'run_extract_script',
    default_args=default_args,
    description='DAG to run extract_last_upd.sh every day at 6 AM',
    schedule_interval='0 6 * * *',  # Запуск в 6 AM каждый день
)

# Оператор для выполнения bash скрипта
run_script = BashOperator(
    task_id='run_extract_last_upd',
    bash_command='bash /path/to/your/extract_last_upd.sh',  # Измените путь к вашему скрипту
    dag=dag,
)

run_script
