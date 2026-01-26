from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import requests

secret_hr_gate = Variable.get("secret_hr_gate")
connect_to_db = Variable.get("conn_hr_gate_secret")

default_args = {
    "email": ["main@yandex.ru"],
    "email_on_failure": True,
    'start_date': "2025-06-23",
    "ssh_conn_id": "oka-analyze-en-001",
}

def auth_token(**kwargs):
    import base64
    realm = "mts"
    client_id = "oka-analyze-en-001"
    secret = secret_hr_gate
    url = "https://isso.ru/auth/realms/protocol/openid-connect/token"

    credentials = f"{client_id}:{secret}"
    b64_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {b64_credentials}"
    }

    data = {'grant_type': 'client_credentials', 'scope': 'hr-gate'}
    response = requests.post(url, headers=headers, data=data)

    if response.ok:
        token = response.json()['access_token']
        kwargs['ti'].xcom_push(key='token', value=token)
    else:
        print("Ошибка:", response.status_code, response.text)

def load_data_from_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id='core_gp')
    kwargs['ti'].xcom_push(key='engine', value=hook.get_uri())

def _flatten_table_for_simple_dicts(response, table_name):
    flattened_data = []
    for item in response:
        flat_item = {}
        for key, value in item.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flat_item[f"{key}_{sub_key}"] = sub_value
            else:
                flat_item[key] = value
        flattened_data.append(flat_item)
    return flattened_data

def _flatten_table_with_nested_dicts(response, table_name):
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
    return flattened_data

def _save_to_db(table_name, df, conn):
    if df is not None and not df.empty:
        df.to_sql(f"hr_gate_{table_name}", con=conn, if_exists='replace', index=False, schema="a_data_b2b")
        print(f"passed {table_name}")
    else:
        print(f"{table_name} is empty")

def _load_table_hr_gate(table_name, nested=False, **kwargs):
    conn = create_engine(connect_to_db)
    token = kwargs['ti'].xcom_pull(key='token')
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(f'https://hr-gate.ru/api/v1/{table_name}', headers=headers, verify=False).json()

    if not response:
        print(f"{table_name}: пустой ответ")
        return

    if nested:
        flattened_data = _flatten_table_with_nested_dicts(response, table_name)
    else:
        flattened_data = _flatten_table_for_simple_dicts(response, table_name)

    df = pd.DataFrame(flattened_data)
    _save_to_db(table_name, df, conn)

def _load_table_hr_gate_with_high_dict(table_name, **kwargs):

    conn = create_engine(connect_to_db)
    token = kwargs['ti'].xcom_pull(key='token')
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(f'https://hr-gate.ru/api/v1/{table_name}', headers=headers, verify=False).json()
    flattened_data = _flatten_table_with_nested_dicts(response, table_name)
    df = pd.DataFrame(flattened_data)
    _save_to_db(table_name, df, conn)

def build_task(dag, task_id, table_name, fn, **kwargs):
    return PythonOperator(
        task_id=task_id,
        python_callable=fn,
        op_kwargs={'table_name': table_name, **kwargs},
        provide_context=True,
        dag=dag
    )

with DAG(
    dag_id="hr_gate",
    default_args=default_args,
    schedule_interval='0 2 * * *',
    tags=['python', 'bash', 'dummy'],
    max_active_runs=1,
    catchup=False
) as dag:


    auth_token_task = build_task(dag, "auth_token", None, auth_token)

    auth_engine = build_task(dag, "auth_engine", None, load_data_from_postgres)


    tables_simple = [
        ("employees", False, _load_table_hr_gate),
        ("employee-roles", False, _load_table_hr_gate),
        ("assignments", False, _load_table_hr_gate),
        ("absence-types", False, _load_table_hr_gate),
        ("timekeepers", False, _load_table_hr_gate),
        ("assignmentstatus", False, _load_table_hr_gate),
        ("units", False, _load_table_hr_gate),
        ("locations", False, _load_table_hr_gate),

    ]


    loads = []
    for tbl, nested, fn in tables_simple:
        task = PythonOperator(
            task_id=f"load_{tbl}",
            python_callable=fn,
            op_kwargs={'table_name': tbl, 'nested': nested},
            provide_context=True,
            dag=dag
        )
        loads.append(task)

    load_absences = PythonOperator(
        task_id='load_absences',
        python_callable=_load_table_hr_gate_with_high_dict,
        op_kwargs={'table_name': 'absences'},
        provide_context=True,
        dag=dag
    )

    auth_token_task >> auth_engine
    auth_engine_resolved = auth_engine  
    auth_engine_resolved >> loads[0] 
    for i in range(len(loads) - 1):
        loads[i] >> loads[i+1]

    loads[  wait := None ]
    
    loads_final = loads + [load_absences]
    loads[0]  
    auth_engine >> loads[0]
    for idx in range(len(loads) - 1):
        loads[idx] >> loads[idx+1]
    loads[-1] >> load_absences
