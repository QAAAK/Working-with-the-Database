from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'kirsanov-nig', # если не отработает, то 'owner': 'admin'
    'start_date': days_ago(0),
    'depends_on_past': False
}


with DAG (
    dag_id = "LOAD_VRB_DET_DBODEAL",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_det_dbodeal"
        (id_dbodeal, code_dbodealstatus, name_dbodealstatus, dt_application, dt_close, termination_reason, dt_login, id_system, 
        id_process, dttm_insert, dttm_update, num_dbodeal, dt_sign, id_subject_cdi, id_subject, code_department, id_employee, "w$loadid", "w$ldate")
        (SELECT id_dbodeal, code_dbodealstatus, name_dbodealstatus, dt_application, dt_close, 
        termination_reason, dt_login, id_system, id_process, dttm_insert, dttm_update,  
        num_dbodeal, dt_sign, id_subject_cdi, id_subject, code_department, id_employee,
        'HELLO_UBAD', CURRENT_TIMESTAMP FROM gl.budm_vrb_vrb_det_dbodeal where id_process>(select max(id_process)
        from gl."cdwh$budm_vrb$vrb_det_dbodeal"));
        """
    )



t1