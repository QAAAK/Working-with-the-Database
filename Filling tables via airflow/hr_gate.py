from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import psycopg2
import requests



secret_hr_gate = Variable.get("secret_hr_gate")
connect_to_db = Variable.get("conn_hr_gate_secret") # позже, разобраться с заменой на хук


default_args = {
        "email": ["main@yandex.ru"],  
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
        url = "https://isso.ru/auth/realms/mts/protocol/openid-connect/token"


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
    
    
            
def employee_hr_gate(table_name, **kwargs):
    
    conn = create_engine(connect_to_db)
    
    headers = {
    "Content-Type": "application/json",
     "Authorization": f"Bearer {kwargs['ti'].xcom_pull(key='token')}" }
    
    
    
    """Пуллим информацию из Xcom"""
    response = requests.get(f'https://hr-gate.ru/api/v1/{table_name}', headers=headers, verify=False).json()

    flattened_data = []
    for item in response:
        flat_item = {}
        for key, value in item.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flat_item[f"{key}_{sub_key}"] = sub_value  # Присоединяем ключи
            else:
                flat_item[key] = value
        flattened_data.append(flat_item)




    import pandas as pd
    # try:
    if response:  # Check if response is not None
        df = pd.DataFrame(flattened_data)
        if not df.empty:  # Check if DataFrame is not empty
            df.to_sql(f"hr_gate_{table_name}", con=conn,  if_exists='replace', index=False, schema="a_data_b2b")
            print(f"passed {table_name}")
        else:
            print(f"{table_name} is empty")

    # except Exception as e:
    #     print(f"Ошибка при вставке в таблицу", e)


def load_table_hr_gate_with_high_dict(table_name, **kwargs):

    conn = create_engine(connect_to_db)
    
    headers = {
    "Content-Type": "application/json",
     "Authorization": f"Bearer {kwargs['ti'].xcom_pull(key='token')}" }
    
    
    
    """Пуллим информацию из Xcom"""
    response = requests.get(f'https://hr-gate.ru/api/v1/{table_name}', headers=headers, verify=False).json()

    flattened_data = []
    for item in response:
        flat_item = {}
        for key, value in item.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, dict):
                        for sub_sub_key, sub_sub_value in sub_value.items():
                            flat_item[f"{key}_{sub_key}_{sub_sub_key}"] = sub_sub_value
                    else:
                        flat_item[f"{key}_{sub_key}"] = sub_value
            else:
                flat_item[key] = value
        flattened_data.append(flat_item)    
        
    import pandas as pd
    # try:
    if response:  # Check if response is not None
        df = pd.DataFrame(flattened_data)
        if not df.empty:  # Check if DataFrame is not empty
            df.to_sql(f"hr_gate_{table_name}", con=conn,  if_exists='replace', index=False, schema="a_data_b2b")
            print(f"passed {table_name}")
        else:
            print(f"{table_name} is empty")
    

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

    load_employees = PythonOperator(
        task_id='load_employees',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'employees'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_employees_roles = PythonOperator(
        task_id='load_employees_roles',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'employee-roles'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_assignments = PythonOperator(
        task_id='load_assignments',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'assignments'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_absence_types = PythonOperator(
        task_id='load_absence_types',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'absence-types'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_absences = PythonOperator(
        task_id='load_absences',
        python_callable=load_table_hr_gate_with_high_dict,
        op_kwargs={'table_name': 'absences'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_timekeepers = PythonOperator(
        task_id='load_timekeepers',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'timekeepers'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_assignmentstatus = PythonOperator(
        task_id='load_assignmentstatus',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'assignmentstatus'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_units = PythonOperator(
        task_id='load_units',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'units'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    load_locations = PythonOperator(
        task_id='load_locations',
        python_callable=load_table_hr_gate,
        op_kwargs={'table_name': 'locations'},
        provide_context=True, # контекст выполнения задачи
        dag=dag,
    )
    
    
    


auth_token >> auth_engine >> [load_employees,load_employees_roles, load_assignments, load_absences, load_absence_types, load_timekeepers, load_assignmentstatus, load_units, load_locations]
