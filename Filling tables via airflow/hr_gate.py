from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

import requests



secret_hr_gate = Variable.get("secret_hr_gate")


default_args = {
        "email": ["main@mail.com"],  # 
        "email_on_failure": True,
        'start_date': "2025-06-23",
        "ssh_conn_id": "oka-analyze-en-001", # сервер, где делает потоки/запросы. в терминале написать hostnamectl
}



def auth_token(**kwargs):
        
        import base64
        
        #  параметры запроса
        realm = "mts"
        client_id = "oka-analyze-en-001"
        secret = secret_hr_gate
        url = "url"


        credentials = "{}:{}".format(client_id, secret)
        b64_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic {}".format(b64_credentials)
        }


        data = {
            'grant_type': 'client_credentials',
            'scope': 'hr-gate'
        }

        #  запрос POST
        response = requests.post(url, headers=headers, data=data)

        token = ""

        #  результат
        if response.ok:
            token = response.json()['access_token']
            
            kwargs['ti'].xcom_push(key='token', value=token)
        else:
            print("Ошибка:", response.status_code, response.text)
            
            
def load_data_from_postgres(**kwargs):

    hook = PostgresHook(postgres_conn_id='core_gp')
    
    #engine = create_engine(hook.get_uri()) # получить url из Hook
    
    kwargs['ti'].xcom_push(key='engine', value=hook.get_uri())
    
    
            
def pull_function(**kwargs):
    """Пуллим информацию из Xcom"""
    value1 = kwargs['ti'].xcom_pull(key='token')
    
    value2 = kwargs['ti'].xcom_pull(key='engine')
    print(f'{value1},\n {value2}')
    

with DAG (
    dag_id = "hr_gate",
    default_args=default_args,
    schedule_interval = '0 2 * * *',
    tags = ['python','bash','dummy'],
    max_active_runs=1, # один инстанс
    catchup = False) as dag:

    auth_token = PythonOperator(
        task_id='auth_token',
        python_callable=auth_token, 
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    auth_engine = PythonOperator(
        task_id='auth_engine',
        python_callable=load_data_from_postgres,
        provide_context=True,
        dag=dag
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )

[auth_token,auth_engine] >> pull_task

