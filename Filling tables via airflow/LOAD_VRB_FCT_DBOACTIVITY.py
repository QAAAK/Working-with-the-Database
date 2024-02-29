from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'santalov', # если не отработает, то 'owner': 'admin'
    'start_date': days_ago(0),
    'depends_on_past': False
}


with DAG (
    dag_id = "LOAD_VRB_FCT_DBOACTIVITY",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_fct_dboactivity"
        (id_dboactivity, code_dboactivity, code_dboactivitytype, name_dboactivitytype, dt_activity_start, dt_activity_end, id_subject, is_login, is_auth, ip_address, code_auth_method,     name_auth_device, code_digitalsignature, name_digitalsignature, id_system, id_process, dttm_insert, dttm_update, "w$loadid", "w$ldate" )
        (SELECT id_dboactivity, code_dboactivity, code_dboactivitytype, name_dboactivitytype, 
        dt_activity_start, dt_activity_end, id_subject, is_login, is_auth, ip_address, code_auth_method, name_auth_device, 
        code_digitalsignature, name_digitalsignature, id_system, id_process, dttm_insert, dttm_update,
        'HELLO_UBAD', CURRENT_TIMESTAMP FROM gl.budm_vrb_vrb_fct_dboactivity where id_process>(select max(id_process)
        from gl."cdwh$budm_vrb$vrb_fct_dboactivity"));
            """
    )
t1
