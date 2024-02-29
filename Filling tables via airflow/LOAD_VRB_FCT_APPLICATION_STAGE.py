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
    dag_id = "LOAD_VRB_FCT_APPLICATION_STAGE",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:


    t1 = PostgresOperator(
    task_id ="LOAD",
    postgres_conn_id = "drp", # из вебинтерфейса airflow > admin > connections
    sql = """
        INSERT INTO gl."cdwh$budm_vrb$vrb_fct_application_stage"
        (id_application, id_applicationstage, applicationstatus, id_applicationtask, dt_applicationstatus, dt_applicationstage, 
        dsc_ccstatus, dsc_statusreason, is_autorevison, application_revisionreason, code_ccresultdsc, code_ccresult, code_ccanswer, 
        code_ccanswerfull, name_ccanswer, ccmonitorpoint, num_terbobl, is_productoffer, is_authbodydecision, id_system, id_process, 
        dttm_insert, dttm_update, code_applicationstage, name_applicationstage, "w$loadid", "w$ldate")
        (select id_application, id_applicationstage, applicationstatus, id_applicationtask, dt_applicationstatus, dt_applicationstage, 
        dsc_ccstatus, dsc_statusreason, is_autorevison, application_revisionreason, code_ccresultdsc, code_ccresult, code_ccanswer, 
        code_ccanswerfull, name_ccanswer, ccmonitorpoint, num_terbobl, is_productoffer, is_authbodydecision, id_system, id_process, 
        dttm_insert, dttm_update, code_applicationstage, name_applicationstage, 'Hello_ubad',current_timestamp from gl.budm_vrb_vrb_fct_application_stage where id_process>(select max(id_process)
        from gl."cdwh$budm_vrb$vrb_fct_application_stage"));
        """
    )

t1
