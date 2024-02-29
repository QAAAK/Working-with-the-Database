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
    dag_id = "LOAD_VRB_CL_APPLICATIONSTATUS",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        truncate table gl."cdwh$budm_vrb$vrb_cl_applicationstatus";
        INSERT INTO gl."cdwh$budm_vrb$vrb_cl_applicationstatus"
        (id_applicationstatus, type_application, code_applicationstatus, name_applicationstatus, id_system, id_process, dttm_insert, dttm_update, "w$loadid", "w$ldate" )
        (SELECT id_applicationstatus, type_application, code_applicationstatus, name_applicationstatus, 
        id_system, id_process, dttm_insert, dttm_update, 'HELLO_UBAD', CURRENT_TIMESTAMP FROM gl.budm_vrb_vrb_cl_applicationstatus);
        """
    )





t1 
